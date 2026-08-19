"""Fail-closed process containment for real-tool acceptance harnesses."""

from __future__ import annotations

import math
import signal
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from threading import Event, Thread

from seqevi.runner import ToolCommand, ToolRunResult, ToolRunner


@dataclass(frozen=True, slots=True)
class AcceptanceTimeouts:
    """Two-layer finite timeout contract for one acceptance annotation."""

    internal_seconds: float
    watchdog_seconds: float
    termination_grace_seconds: float = 30.0

    def __post_init__(self) -> None:
        values = (
            self.internal_seconds,
            self.watchdog_seconds,
            self.termination_grace_seconds,
        )
        if not all(math.isfinite(value) and value > 0 for value in values):
            raise ValueError("acceptance timeouts must be finite and positive")
        if self.watchdog_seconds <= self.internal_seconds:
            raise ValueError("external watchdog must exceed internal adapter timeout")
        if self.termination_grace_seconds > 60:
            raise ValueError("acceptance termination grace must not exceed 60 seconds")


@dataclass(slots=True)
class ContainedAcceptanceProcess:
    """Process-like handle whose worker delegates containment to ``ToolRunner``."""

    command: ToolCommand
    timeouts: AcceptanceTimeouts
    _cancel: Event = field(default_factory=Event, init=False)
    _done: Event = field(default_factory=Event, init=False)
    _result: ToolRunResult | None = field(default=None, init=False)
    _error: BaseException | None = field(default=None, init=False)
    _return_code: int | None = field(default=None, init=False)
    _thread: Thread = field(init=False)

    def __post_init__(self) -> None:
        self._thread = Thread(
            target=self._run,
            name="seqevi-acceptance-containment",
            daemon=False,
        )

    def start(self) -> ContainedAcceptanceProcess:
        self._thread.start()
        return self

    def poll(self) -> int | None:
        if not self._done.is_set():
            return None
        return self._terminal_return_code()

    def send_signal(self, sent_signal: signal.Signals) -> None:
        if sent_signal not in {signal.SIGINT, signal.SIGTERM}:
            raise ValueError("acceptance handle supports cancellation signals only")
        self._cancel.set()

    @property
    def cancellation_requested(self) -> bool:
        """Report whether cleanup has durably requested cancellation."""

        return self._cancel.is_set()

    def wait(self, timeout: float | None = None) -> int:
        if not self._done.wait(timeout):
            raise TimeoutError("acceptance containment worker did not finish")
        self._thread.join()
        return self._terminal_return_code()

    @property
    def completed_error(self) -> BaseException | None:
        """Return the stable terminal failure without re-raising it."""

        if not self._done.is_set():
            return None
        return self._error

    def _run(self) -> None:
        try:
            self._result = ToolRunner(
                termination_grace_seconds=self.timeouts.termination_grace_seconds
            ).run(
                self.command,
                timeout_seconds=self.timeouts.watchdog_seconds,
                cancellation_signal=self._cancel,
            )
        except BaseException as error:
            self._error = error
        finally:
            if self._result is not None:
                self._return_code = self._result.return_code
            elif self._error is not None:
                error_result = getattr(self._error, "result", None)
                self._return_code = getattr(error_result, "return_code", -1)
            self._done.set()

    def _terminal_return_code(self) -> int:
        if self._return_code is None:
            raise RuntimeError("acceptance containment finished without a result")
        return self._return_code


def install_watchdog_signal_bridge() -> None:
    """Turn outer watchdog TERM into stack unwinding through inner ToolRunner."""

    def interrupt(_signal: int, _frame: object) -> None:
        raise KeyboardInterrupt("acceptance watchdog requested annotation cleanup")

    signal.signal(signal.SIGTERM, interrupt)


def start_contained_annotation(
    *,
    arguments: tuple[str, ...],
    working_dir: Path,
    stdout_path: Path,
    stderr_path: Path,
    environment: Mapping[str, str],
    timeouts: AcceptanceTimeouts,
) -> ContainedAcceptanceProcess:
    """Validate the annotate timeout and start one ToolRunner-owned boundary."""

    if "annotate" not in arguments:
        raise ValueError("acceptance command must invoke annotate")
    timeout_indexes = [
        index for index, value in enumerate(arguments) if value == "--timeout-seconds"
    ]
    if len(timeout_indexes) != 1 or timeout_indexes[0] + 1 >= len(arguments):
        raise ValueError("acceptance annotate requires one --timeout-seconds value")
    try:
        command_timeout = float(arguments[timeout_indexes[0] + 1])
    except ValueError as error:
        raise ValueError("annotate timeout must be numeric") from error
    if command_timeout != timeouts.internal_seconds:
        raise ValueError("annotate timeout does not match acceptance contract")

    command = ToolCommand(
        arguments=arguments,
        working_dir=working_dir,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        environment=environment,
    )
    return ContainedAcceptanceProcess(command, timeouts).start()
