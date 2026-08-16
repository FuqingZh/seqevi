"""Private process-group-aware runner for external annotation tools."""

from __future__ import annotations

import errno
import logging
import os
import signal
import subprocess
import time
from collections.abc import Callable, Mapping
from ctypes import CDLL, byref, c_int, get_errno
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from threading import Event, Thread

_WAIT_INTERVAL_SECONDS = 0.05
_CLEANUP_STUCK_AFTER_SECONDS = 2.0
_LOGGER = logging.getLogger("seqevi.runner")
_PR_GET_CHILD_SUBREAPER = 37
_DEFERRED_CLEANUP_ERRORS = (KeyboardInterrupt, SystemExit, OSError)


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
    cancelled: bool = False


@dataclass(frozen=True, slots=True)
class ToolCleanupStuck:
    """Out-of-band observation while synchronous process cleanup remains owned."""

    tool: str
    leader_pid: int
    process_group_id: int
    members: tuple[tuple[int, str], ...]


class ToolTimeoutError(RuntimeError):
    """Raised after a timed-out process group has been terminated and reaped."""

    def __init__(self, result: ToolRunResult, timeout_seconds: float) -> None:
        self.result = result
        self.timeout_seconds = timeout_seconds
        super().__init__(
            f"tool timed out after {timeout_seconds:g} seconds; "
            f"stderr: {result.stderr_path}"
        )


class ToolCancelledError(RuntimeError):
    """Raised after a cancelled process group has been terminated and reaped."""

    def __init__(self, result: ToolRunResult) -> None:
        self.result = result
        super().__init__(f"tool was cancelled; stderr: {result.stderr_path}")


@dataclass(frozen=True, slots=True)
class _ProcessMember:
    pid: int
    state: str
    parent_pid: int
    process_group_id: int


class ToolRunner:
    """Run one command without a shell and synchronously clean its Linux group."""

    def __init__(
        self,
        *,
        termination_grace_seconds: float = 5.0,
        cleanup_observer: Callable[[ToolCleanupStuck], None] | None = None,
    ) -> None:
        if termination_grace_seconds <= 0:
            raise ValueError("termination_grace_seconds must be positive")
        self.termination_grace_seconds = termination_grace_seconds
        self.cleanup_observer = cleanup_observer

    def run(
        self,
        command: ToolCommand,
        *,
        timeout_seconds: float | None = None,
        cancellation_signal: Event | None = None,
    ) -> ToolRunResult:
        """Run a command, reacting directly to cancellation and owning cleanup."""

        if timeout_seconds is not None and timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if not hasattr(os, "waitid") or not Path("/proc").is_dir():
            raise RuntimeError("ToolRunner process containment requires Linux /proc")

        command.working_dir.mkdir(parents=True, exist_ok=True)
        command.stdout_path.parent.mkdir(parents=True, exist_ok=True)
        command.stderr_path.parent.mkdir(parents=True, exist_ok=True)
        environment = os.environ.copy()
        environment.update(command.environment)

        started_at = datetime.now(UTC)
        started = time.monotonic()
        cancellation_signal = cancellation_signal or Event()
        process: subprocess.Popen[bytes] | None = None
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
            execution_started = time.monotonic()
            reason: str | None = None
            original_error: BaseException | None = None
            deadline = (
                None if timeout_seconds is None else execution_started + timeout_seconds
            )
            live_members = ()
            try:
                while not self._leader_exited(process.pid):
                    if cancellation_signal.is_set():
                        reason = "cancelled"
                        break
                    wait_for = _WAIT_INTERVAL_SECONDS
                    if deadline is not None:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            reason = "timeout"
                            break
                        wait_for = min(wait_for, remaining)
                    cancellation_signal.wait(wait_for)
                if reason is None and cancellation_signal.is_set():
                    reason = "cancelled"
                if reason is None:
                    live_members, _ = self._group_snapshot(process.pid, process.pid)
            except BaseException as error:
                original_error = error
                reason = "exception"
            cleanup_error = self._terminate_and_reap(
                process, command, send_term=reason is not None or bool(live_members)
            )

            if original_error is not None:
                raise original_error
            if cleanup_error is not None:
                raise cleanup_error
            if reason is None and cancellation_signal.is_set():
                reason = "cancelled"

        result = self._result(
            command,
            return_code=process.returncode,
            started_at=started_at,
            started=started,
            timed_out=reason == "timeout",
            cancelled=reason == "cancelled",
        )
        if reason == "timeout":
            assert timeout_seconds is not None
            raise ToolTimeoutError(result, timeout_seconds)
        if reason == "cancelled":
            raise ToolCancelledError(result)
        return result

    def _terminate_and_reap(
        self,
        process: subprocess.Popen[bytes],
        command: ToolCommand,
        *,
        send_term: bool,
    ) -> BaseException | None:
        leader_pid = process.pid
        cleanup_started: float | None = None
        stuck_reported = False
        deferred_error: BaseException | None = None

        def defer(error: BaseException) -> None:
            nonlocal deferred_error
            if not isinstance(error, OSError) and deferred_error is None:
                deferred_error = error

        if not send_term:
            while True:
                try:
                    late_members, ambiguous = self._group_snapshot(
                        leader_pid, leader_pid
                    )
                    send_term = bool(late_members) or ambiguous
                    break
                except _DEFERRED_CLEANUP_ERRORS as error:
                    defer(error)
        if send_term:
            while True:
                try:
                    self._signal_group(leader_pid, signal.SIGTERM, allow_missing=False)
                    break
                except _DEFERRED_CLEANUP_ERRORS as error:
                    defer(error)
            grace_deadline = time.monotonic() + self.termination_grace_seconds
            while time.monotonic() < grace_deadline:
                try:
                    time.sleep(
                        max(
                            0.0,
                            min(
                                _WAIT_INTERVAL_SECONDS,
                                grace_deadline - time.monotonic(),
                            ),
                        )
                    )
                except _DEFERRED_CLEANUP_ERRORS as error:
                    defer(error)

            # Kill the complete group at the fixed grace boundary. A missing
            # group is a benign exit race; membership is still proven below.
            while True:
                try:
                    self._signal_group(leader_pid, signal.SIGKILL, allow_missing=True)
                    cleanup_started = time.monotonic()
                    break
                except _DEFERRED_CLEANUP_ERRORS as error:
                    defer(error)

        cleanup_started = cleanup_started or time.monotonic()

        while True:
            try:
                stuck_reported = self._wait_for_clean_group(
                    command,
                    leader_pid,
                    cleanup_started,
                    stuck_reported,
                )
                break
            except _DEFERRED_CLEANUP_ERRORS as error:
                defer(error)

        # Fence a fork handoff while the unreaped leader still reserves its PGID.
        while True:
            try:
                if self._signal_group(leader_pid, signal.SIGKILL, allow_missing=True):
                    break
                live, _ = self._group_snapshot(leader_pid, leader_pid)
            except _DEFERRED_CLEANUP_ERRORS as error:
                defer(error)
                continue
            if not stuck_reported:
                self._report_cleanup_stuck(command, leader_pid, live)
                stuck_reported = True
            try:
                time.sleep(_WAIT_INTERVAL_SECONDS)
            except _DEFERRED_CLEANUP_ERRORS as error:
                defer(error)
        while True:
            try:
                self._wait_for_clean_group(
                    command,
                    leader_pid,
                    cleanup_started,
                    stuck_reported,
                )
                break
            except _DEFERRED_CLEANUP_ERRORS as error:
                defer(error)
        while True:
            try:
                owns_adopted_children = self._owns_adopted_children()
                break
            except (KeyboardInterrupt, SystemExit) as error:
                defer(error)
            except OSError as error:
                defer(error)
                if time.monotonic() - cleanup_started >= _CLEANUP_STUCK_AFTER_SECONDS:
                    if not stuck_reported:
                        self._report_cleanup_stuck(command, leader_pid, ())
                        stuck_reported = True
                try:
                    time.sleep(_WAIT_INTERVAL_SECONDS)
                except _DEFERRED_CLEANUP_ERRORS as error:
                    defer(error)
        if owns_adopted_children:
            while True:
                try:
                    adopted_clean = self._reap_adopted_group_zombies(leader_pid)
                except _DEFERRED_CLEANUP_ERRORS as error:
                    defer(error)
                    continue
                if adopted_clean:
                    break
                if time.monotonic() - cleanup_started >= _CLEANUP_STUCK_AFTER_SECONDS:
                    if not stuck_reported:
                        self._report_cleanup_stuck(command, leader_pid, ())
                        stuck_reported = True
                try:
                    time.sleep(_WAIT_INTERVAL_SECONDS)
                except _DEFERRED_CLEANUP_ERRORS as error:
                    defer(error)
        while True:
            try:
                process.wait()
                break
            except _DEFERRED_CLEANUP_ERRORS as error:
                defer(error)
        return deferred_error

    def _wait_for_clean_group(
        self,
        command: ToolCommand,
        leader_pid: int,
        cleanup_started: float,
        stuck_reported: bool,
    ) -> bool:
        clean_snapshots = 0
        while True:
            leader_exited = self._leader_exited(leader_pid)
            live, ambiguous = self._group_snapshot(leader_pid, leader_pid)
            if leader_exited and not live and not ambiguous:
                clean_snapshots += 1
                if clean_snapshots >= 2:
                    return stuck_reported
                time.sleep(0)
                continue
            clean_snapshots = 0
            if time.monotonic() - cleanup_started >= _CLEANUP_STUCK_AFTER_SECONDS:
                if not stuck_reported:
                    self._report_cleanup_stuck(command, leader_pid, live)
                    stuck_reported = True
                self._signal_group(leader_pid, signal.SIGKILL, allow_missing=False)
            time.sleep(_WAIT_INTERVAL_SECONDS)

    def _report_cleanup_stuck(
        self,
        command: ToolCommand,
        leader_pid: int,
        members: tuple[_ProcessMember, ...],
    ) -> None:
        observation = ToolCleanupStuck(
            tool=command.arguments[0],
            leader_pid=leader_pid,
            process_group_id=leader_pid,
            members=tuple((member.pid, member.state) for member in members),
        )
        _LOGGER.error(
            "external-tool cleanup remains stuck",
            extra={
                "tool": observation.tool,
                "leader_pid": observation.leader_pid,
                "process_group_id": observation.process_group_id,
                "members": observation.members,
            },
        )
        if self.cleanup_observer is None:
            return
        try:
            Thread(
                target=self._notify_cleanup_observer,
                args=(observation,),
                name=f"seqevi-cleanup-observer-{leader_pid}",
                daemon=True,
            ).start()
        except RuntimeError:
            _LOGGER.exception("external-tool cleanup observer dispatch failed")

    def _notify_cleanup_observer(self, observation: ToolCleanupStuck) -> None:
        """Notify operational telemetry without transferring cleanup ownership."""

        assert self.cleanup_observer is not None
        try:
            self.cleanup_observer(observation)
        except Exception:
            _LOGGER.exception("external-tool cleanup observer failed")

    @staticmethod
    def _leader_exited(leader_pid: int) -> bool:
        try:
            observed = os.waitid(
                os.P_PID,
                leader_pid,
                os.WEXITED | os.WNOHANG | os.WNOWAIT,
            )
        except ChildProcessError:
            return True
        return observed is not None

    @classmethod
    def _group_snapshot(
        cls, process_group_id: int, leader_pid: int
    ) -> tuple[tuple[_ProcessMember, ...], bool]:
        live: list[_ProcessMember] = []
        ambiguous = False
        try:
            entries = tuple(os.scandir("/proc"))
        except OSError:
            return (), True
        for entry in entries:
            if not entry.name.isdecimal():
                continue
            try:
                member = cls._read_process_member(int(entry.name))
            except FileNotFoundError:
                continue
            except (OSError, ValueError):
                ambiguous = True
                continue
            if member.process_group_id == process_group_id and member.state not in {
                "Z",
                "X",
            }:
                live.append(member)
        return tuple(sorted(live, key=lambda item: item.pid)), ambiguous

    @staticmethod
    def _read_process_member(pid: int) -> _ProcessMember:
        descriptor = os.open(f"/proc/{pid}/stat", os.O_RDONLY)
        try:
            content = os.read(descriptor, 4096)
        finally:
            os.close(descriptor)
        close = content.rfind(b")")
        if close < 0:
            raise ValueError("malformed process stat")
        fields = content[close + 2 :].split()
        if len(fields) < 3:
            raise ValueError("short process stat")
        return _ProcessMember(
            pid=pid,
            state=fields[0].decode("ascii"),
            parent_pid=int(fields[1]),
            process_group_id=int(fields[2]),
        )

    @classmethod
    def _reap_adopted_group_zombies(cls, process_group_id: int) -> bool:
        while True:
            reaped = False
            try:
                entries = tuple(os.scandir("/proc"))
            except OSError:
                return False
            for entry in entries:
                if not entry.name.isdecimal():
                    continue
                pid = int(entry.name)
                if pid == process_group_id:
                    continue
                try:
                    member = cls._read_process_member(pid)
                except (FileNotFoundError, ProcessLookupError):
                    continue
                except (OSError, ValueError):
                    return False
                if (
                    member.process_group_id != process_group_id
                    or member.state not in {"Z", "X"}
                    or member.parent_pid != os.getpid()
                ):
                    continue
                try:
                    waited, _ = os.waitpid(pid, os.WNOHANG)
                except ChildProcessError:
                    continue
                except OSError:
                    return False
                reaped = reaped or waited == pid
            if not reaped:
                return True

    @staticmethod
    def _owns_adopted_children() -> bool:
        if os.getpid() == 1:
            return True
        enabled = c_int()
        try:
            prctl = CDLL(None, use_errno=True).prctl
            result = prctl(_PR_GET_CHILD_SUBREAPER, byref(enabled), 0, 0, 0)
        except AttributeError as error:
            raise OSError(errno.ENOSYS, "prctl is unavailable") from error
        except OSError as error:
            raise OSError(
                error.errno, "failed to query child subreaper state"
            ) from error
        if result != 0:
            error_number = get_errno() or errno.EIO
            raise OSError(error_number, "failed to query child subreaper state")
        return enabled.value == 1

    @staticmethod
    def _signal_group(
        process_group_id: int, sent_signal: signal.Signals, *, allow_missing: bool
    ) -> bool:
        try:
            os.killpg(process_group_id, sent_signal)
        except ProcessLookupError:
            return allow_missing
        except OSError:
            return False
        return True

    @staticmethod
    def _result(
        command: ToolCommand,
        *,
        return_code: int | None,
        started_at: datetime,
        started: float,
        timed_out: bool,
        cancelled: bool,
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
            cancelled=cancelled,
        )
