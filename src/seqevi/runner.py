"""Private process-group-aware runner for external annotation tools."""

from __future__ import annotations

import os
import signal
import subprocess
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ToolCommand:
    """One explicit external command and its operational environment."""

    arguments: tuple[str, ...]
    working_dir: Path
    stdout_path: Path
    stderr_path: Path
    environment: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.arguments or not self.arguments[0]:
            raise ValueError("tool command requires an executable")
        if self.stdout_path == self.stderr_path:
            raise ValueError("stdout_path and stderr_path must be different")


@dataclass(frozen=True, slots=True)
class ToolRunResult:
    """Structured operational result for one external process."""

    arguments: tuple[str, ...]
    return_code: int
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    stdout_path: Path
    stderr_path: Path
    timed_out: bool = False


class ToolTimeoutError(RuntimeError):
    """Raised after a timed-out process group has been terminated."""

    def __init__(self, result: ToolRunResult, timeout_seconds: float) -> None:
        self.result = result
        self.timeout_seconds = timeout_seconds
        super().__init__(
            f"tool timed out after {timeout_seconds:g} seconds; "
            f"stderr: {result.stderr_path}"
        )


class ToolRunner:
    """Run one command without a shell and clean up its process group."""

    def __init__(self, *, termination_grace_seconds: float = 2.0) -> None:
        if termination_grace_seconds <= 0:
            raise ValueError("termination_grace_seconds must be positive")
        self.termination_grace_seconds = termination_grace_seconds

    def run(
        self,
        command: ToolCommand,
        *,
        timeout_seconds: float | None = None,
    ) -> ToolRunResult:
        """Run a command and return its exit status and diagnostic paths."""

        if timeout_seconds is not None and timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        command.working_dir.mkdir(parents=True, exist_ok=True)
        command.stdout_path.parent.mkdir(parents=True, exist_ok=True)
        command.stderr_path.parent.mkdir(parents=True, exist_ok=True)
        environment = os.environ.copy()
        environment.update(command.environment)

        started_at = datetime.now(UTC)
        started = time.monotonic()
        with (
            command.stdout_path.open("wb") as stdout,
            command.stderr_path.open("wb") as stderr,
        ):
            process = subprocess.Popen(  # noqa: S603
                command.arguments,
                cwd=command.working_dir,
                env=environment,
                stdout=stdout,
                stderr=stderr,
                shell=False,
                start_new_session=True,
            )
            try:
                return_code = process.wait(timeout=timeout_seconds)
            except subprocess.TimeoutExpired:
                self._terminate_process_group(process)
                result = self._result(
                    command,
                    return_code=process.returncode,
                    started_at=started_at,
                    started=started,
                    timed_out=True,
                )
                assert timeout_seconds is not None
                raise ToolTimeoutError(result, timeout_seconds) from None
            except BaseException:
                self._terminate_process_group(process)
                raise

        return self._result(
            command,
            return_code=return_code,
            started_at=started_at,
            started=started,
            timed_out=False,
        )

    def _terminate_process_group(self, process: subprocess.Popen[bytes]) -> None:
        if process.poll() is not None:
            return
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            process.wait()
            return
        try:
            process.wait(timeout=self.termination_grace_seconds)
            return
        except subprocess.TimeoutExpired:
            pass
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            process.wait()
            return
        process.wait()

    @staticmethod
    def _result(
        command: ToolCommand,
        *,
        return_code: int | None,
        started_at: datetime,
        started: float,
        timed_out: bool,
    ) -> ToolRunResult:
        if return_code is None:
            raise RuntimeError("terminated process did not report an exit status")
        return ToolRunResult(
            arguments=command.arguments,
            return_code=return_code,
            started_at=started_at,
            finished_at=datetime.now(UTC),
            duration_seconds=time.monotonic() - started,
            stdout_path=command.stdout_path,
            stderr_path=command.stderr_path,
            timed_out=timed_out,
        )
