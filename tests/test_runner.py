from __future__ import annotations

import errno
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest

import seqevi.runner as runner_module
from seqevi.runner import (
    ToolCancelledError,
    ToolCommand,
    ToolRunner,
    ToolTimeoutError,
)


def command(tmp_path: Path, code: str) -> ToolCommand:
    return ToolCommand(
        arguments=(sys.executable, "-c", code),
        working_dir=tmp_path,
        stdout_path=tmp_path / "stdout.log",
        stderr_path=tmp_path / "stderr.log",
    )


def test_runner_captures_structured_result_and_streams(tmp_path: Path) -> None:
    result = ToolRunner().run(
        command(
            tmp_path,
            "import sys; print('out'); print('err', file=sys.stderr); sys.exit(3)",
        )
    )

    assert result.return_code == 3
    assert result.timed_out is False
    assert result.duration_seconds >= 0
    assert result.stdout_path.read_text().strip() == "out"
    assert result.stderr_path.read_text().strip() == "err"


def test_runner_terminates_timed_out_process_group(tmp_path: Path) -> None:
    with pytest.raises(ToolTimeoutError) as raised:
        ToolRunner(termination_grace_seconds=0.1).run(
            command(tmp_path, "import time; time.sleep(10)"), timeout_seconds=0.05
        )

    assert raised.value.result.timed_out is True
    assert raised.value.result.return_code != 0
    assert raised.value.result.stdout_path.is_file()
    assert raised.value.result.stderr_path.is_file()


def test_runner_cleans_up_process_group_on_cancellation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    signals: list[tuple[int, signal.Signals, float]] = []
    cancellation = threading.Event()
    original_killpg = __import__("os").killpg
    started = time.monotonic()
    monkeypatch.setattr(
        "seqevi.runner.os.killpg",
        lambda pid, sent_signal: (
            signals.append((pid, sent_signal, time.monotonic() - started)),
            original_killpg(pid, sent_signal),
        )[-1],
    )

    timer = threading.Timer(0.1, cancellation.set)
    timer.start()
    try:
        with pytest.raises(ToolCancelledError) as raised:
            ToolRunner(termination_grace_seconds=0.1).run(
                command(
                    tmp_path,
                    "import signal,time; signal.signal(signal.SIGTERM, "
                    "signal.SIG_IGN); time.sleep(10)",
                ),
                cancellation_signal=cancellation,
            )
    finally:
        timer.cancel()

    assert raised.value.result.cancelled is True
    assert [sent for _, sent, _ in signals] == [
        signal.SIGTERM,
        signal.SIGKILL,
        signal.SIGKILL,
    ]
    assert signals[0][2] < 1.0


def test_runner_kills_descendant_after_normal_leader_exit(tmp_path: Path) -> None:
    child_pid_path = tmp_path / "child.pid"
    result = ToolRunner(termination_grace_seconds=0.1).run(
        command(
            tmp_path,
            "import pathlib,subprocess,sys; "
            "child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(10)']); "
            f"pathlib.Path({str(child_pid_path)!r}).write_text(str(child.pid))",
        )
    )

    child_pid = int(child_pid_path.read_text())
    assert result.return_code == 0
    child_stat = Path(f"/proc/{child_pid}/stat")
    if child_stat.exists():
        assert child_stat.read_text().split(") ", 1)[1].startswith(("Z ", "X "))


def test_cancellation_kills_term_resistant_descendant(tmp_path: Path) -> None:
    child_pid_path = tmp_path / "child.pid"
    cancellation = threading.Event()
    timer = threading.Timer(0.15, cancellation.set)
    timer.start()
    try:
        with pytest.raises(ToolCancelledError):
            ToolRunner(termination_grace_seconds=0.1).run(
                command(
                    tmp_path,
                    "import pathlib,subprocess,sys,time; "
                    "child=subprocess.Popen([sys.executable,'-c',"
                    "'import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);"
                    "time.sleep(10)']); "
                    f"pathlib.Path({str(child_pid_path)!r}).write_text(str(child.pid)); "
                    "time.sleep(10)",
                ),
                cancellation_signal=cancellation,
            )
    finally:
        timer.cancel()

    child_pid = int(child_pid_path.read_text())
    child_stat = Path(f"/proc/{child_pid}/stat")
    if child_stat.exists():
        assert child_stat.read_text().split(") ", 1)[1].startswith(("Z ", "X "))


def test_cancellation_wakes_runner_without_polling_delay(tmp_path: Path) -> None:
    cancellation = threading.Event()
    timer = threading.Timer(0.1, cancellation.set)
    started = time.monotonic()
    timer.start()
    try:
        with pytest.raises(ToolCancelledError):
            ToolRunner(termination_grace_seconds=0.1).run(
                command(tmp_path, "import time; time.sleep(10)"),
                cancellation_signal=cancellation,
            )
    finally:
        timer.cancel()
    assert time.monotonic() - started < 1.0


def test_cancellation_signals_before_group_discovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cancellation = threading.Event()
    signal_sent = threading.Event()
    original_signal_group = ToolRunner._signal_group
    original_group_snapshot = ToolRunner._group_snapshot
    first_snapshot = True
    errors: list[BaseException] = []

    def signal_group(
        process_group_id: int,
        sent_signal: signal.Signals,
        *,
        allow_missing: bool,
    ) -> bool:
        if sent_signal == signal.SIGTERM:
            signal_sent.set()
        return original_signal_group(
            process_group_id, sent_signal, allow_missing=allow_missing
        )

    def slow_group_snapshot(
        cls: type[ToolRunner], process_group_id: int, leader_pid: int
    ) -> tuple[tuple[object, ...], bool]:
        nonlocal first_snapshot
        if first_snapshot:
            first_snapshot = False
            time.sleep(1.1)
        return original_group_snapshot(process_group_id, leader_pid)

    def run() -> None:
        try:
            ToolRunner(termination_grace_seconds=0.01).run(
                command(tmp_path, "import time; time.sleep(30)"),
                cancellation_signal=cancellation,
            )
        except BaseException as error:
            errors.append(error)

    monkeypatch.setattr(ToolRunner, "_signal_group", staticmethod(signal_group))
    monkeypatch.setattr(ToolRunner, "_group_snapshot", classmethod(slow_group_snapshot))
    thread = threading.Thread(target=run)
    thread.start()
    time.sleep(0.1)
    cancellation.set()
    assert signal_sent.wait(0.9)
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], ToolCancelledError)


def test_cancellation_wins_the_leader_exit_boundary(tmp_path: Path) -> None:
    cancellation = threading.Event()
    cancellation.set()
    with pytest.raises(ToolCancelledError) as raised:
        ToolRunner(termination_grace_seconds=0.01).run(
            command(tmp_path, "pass"), cancellation_signal=cancellation
        )
    assert raised.value.result.cancelled is True


def test_cancellation_after_containment_wins_normal_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cancellation = threading.Event()
    original_snapshot = ToolRunner._group_snapshot
    snapshots = 0

    def snapshot(process_group_id: int, leader_pid: int):
        nonlocal snapshots
        observed = original_snapshot(process_group_id, leader_pid)
        snapshots += 1
        if snapshots == 1:
            cancellation.set()
        return observed

    monkeypatch.setattr(ToolRunner, "_group_snapshot", staticmethod(snapshot))

    with pytest.raises(ToolCancelledError) as raised:
        ToolRunner().run(command(tmp_path, "pass"), cancellation_signal=cancellation)

    assert raised.value.result.cancelled is True


def test_interrupt_during_pre_cleanup_scan_is_deferred_until_group_reaped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    child_pid_path = tmp_path / "child.pid"
    original_snapshot = ToolRunner._group_snapshot
    interrupted = False

    def interrupt_once(process_group_id: int, leader_pid: int):
        nonlocal interrupted
        if not interrupted:
            interrupted = True
            raise KeyboardInterrupt
        return original_snapshot(process_group_id, leader_pid)

    monkeypatch.setattr(ToolRunner, "_group_snapshot", staticmethod(interrupt_once))
    with pytest.raises(KeyboardInterrupt):
        ToolRunner(termination_grace_seconds=0.05).run(
            command(
                tmp_path,
                "import pathlib,subprocess,sys;"
                "child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(10)']);"
                f"pathlib.Path({str(child_pid_path)!r}).write_text(str(child.pid))",
            )
        )

    assert interrupted
    child_pid = int(child_pid_path.read_text())
    child_stat = Path(f"/proc/{child_pid}/stat")
    if child_stat.exists():
        assert child_stat.read_text().split(") ", 1)[1].startswith(("Z ", "X "))


def test_timeout_budget_starts_after_successful_process_launch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_popen = runner_module.subprocess.Popen

    def delayed_popen(*args: Any, **kwargs: Any):
        time.sleep(0.15)
        return original_popen(*args, **kwargs)

    monkeypatch.setattr(runner_module.subprocess, "Popen", delayed_popen)

    result = ToolRunner().run(
        command(tmp_path, "import time;time.sleep(0.1)"), timeout_seconds=0.2
    )

    assert result.return_code == 0
    assert result.duration_seconds >= 0.25


def test_keyboard_interrupt_still_cleans_and_reaps_group(tmp_path: Path) -> None:
    class InterruptingSignal:
        def is_set(self) -> bool:
            return False

        def wait(self, _timeout: float) -> bool:
            raise KeyboardInterrupt

    started = time.monotonic()
    with pytest.raises(KeyboardInterrupt):
        ToolRunner(termination_grace_seconds=0.1).run(
            command(tmp_path, "import time; time.sleep(10)"),
            cancellation_signal=InterruptingSignal(),  # type: ignore[arg-type]
        )
    assert time.monotonic() - started < 1.0


def test_keyboard_interrupt_during_grace_is_deferred_until_reap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pid_path = tmp_path / "leader.pid"
    cancellation = threading.Event()
    cancellation.set()
    original_sleep = runner_module.time.sleep
    interrupted = False

    def interrupt_once(seconds: float) -> None:
        nonlocal interrupted
        if not interrupted:
            interrupted = True
            raise KeyboardInterrupt
        original_sleep(seconds)

    monkeypatch.setattr(runner_module.time, "sleep", interrupt_once)
    with pytest.raises(KeyboardInterrupt):
        ToolRunner(termination_grace_seconds=0.05).run(
            command(
                tmp_path,
                f"import os,pathlib,time;pathlib.Path({str(pid_path)!r}).write_text(str(os.getpid()));time.sleep(10)",
            ),
            cancellation_signal=cancellation,
        )
    assert interrupted
    if pid_path.exists():
        assert not Path(f"/proc/{pid_path.read_text()}").exists()


def test_adopted_zombie_read_ambiguity_is_not_clean(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = ToolRunner._read_process_member
    ambiguous = True

    def read(pid: int):
        nonlocal ambiguous
        if ambiguous:
            ambiguous = False
            raise PermissionError
        return original(pid)

    monkeypatch.setattr(ToolRunner, "_read_process_member", staticmethod(read))
    assert ToolRunner._reap_adopted_group_zombies(999_999) is False
    assert ToolRunner._reap_adopted_group_zombies(999_999) is True


def test_adopted_zombie_waitpid_ambiguity_is_not_clean(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Entry:
        name = "12345"

    attempts = 0
    scans = 0

    def scandir(_path: str) -> tuple[Entry, ...]:
        nonlocal scans
        scans += 1
        return (Entry(),) if scans <= 2 else ()

    def waitpid(_pid: int, _options: int) -> tuple[int, int]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise InterruptedError
        return (12345, 0)

    monkeypatch.setattr(runner_module.os, "scandir", scandir)
    monkeypatch.setattr(
        ToolRunner,
        "_read_process_member",
        staticmethod(
            lambda pid: runner_module._ProcessMember(
                pid, "Z", runner_module.os.getpid(), 7
            )
        ),
    )
    monkeypatch.setattr(runner_module.os, "waitpid", waitpid)
    assert ToolRunner._reap_adopted_group_zombies(7) is False
    assert ToolRunner._reap_adopted_group_zombies(7) is True


def test_subreaper_query_error_is_explicit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Library:
        @staticmethod
        def prctl(*_arguments: object) -> int:
            return -1

    monkeypatch.setattr(runner_module.os, "getpid", lambda: 2)
    monkeypatch.setattr(runner_module, "CDLL", lambda *_args, **_kwargs: Library())
    monkeypatch.setattr(runner_module, "get_errno", lambda: errno.EIO)

    with pytest.raises(OSError, match="failed to query child subreaper state"):
        ToolRunner._owns_adopted_children()


def test_subreaper_query_error_retains_cleanup_until_retry_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[str] = []
    query_attempts = 0

    class Process:
        pid = 12345

        @staticmethod
        def wait() -> None:
            events.append("wait")

    def owns_adopted_children() -> bool:
        nonlocal query_attempts
        query_attempts += 1
        events.append("query")
        if query_attempts == 1:
            raise OSError(errno.EIO, "query failed")
        return True

    monkeypatch.setattr(ToolRunner, "_wait_for_clean_group", lambda *_args: False)
    monkeypatch.setattr(ToolRunner, "_signal_group", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        ToolRunner,
        "_owns_adopted_children",
        staticmethod(owns_adopted_children),
    )
    monkeypatch.setattr(
        ToolRunner,
        "_reap_adopted_group_zombies",
        staticmethod(lambda _process_group_id: events.append("reap") or True),
    )

    cleanup_error = ToolRunner()._terminate_and_reap(  # pyright: ignore[reportPrivateUsage]
        Process(),  # type: ignore[arg-type]
        command(tmp_path, "pass"),
        send_term=False,
    )

    assert cleanup_error is None
    assert events == ["query", "query", "reap", "wait"]


def test_cleanup_stuck_remains_synchronously_owned_until_clean(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cancellation = threading.Event()
    cancellation.set()
    release = threading.Event()
    observed = []
    observed_at: list[float] = []
    forced_at: list[float] = []
    monkeypatch.setattr(runner_module, "_CLEANUP_STUCK_AFTER_SECONDS", 0.05)
    original_snapshot = runner_module.ToolRunner._group_snapshot
    original_signal_group = runner_module.ToolRunner._signal_group

    def signal_group(
        process_group_id: int,
        sent_signal: signal.Signals,
        *,
        allow_missing: bool,
    ) -> bool:
        if sent_signal == signal.SIGKILL and not forced_at:
            forced_at.append(time.monotonic())
        return original_signal_group(
            process_group_id, sent_signal, allow_missing=allow_missing
        )

    def blocked_snapshot(process_group_id: int, leader_pid: int):
        if not release.is_set():
            return (
                (
                    runner_module._ProcessMember(  # pyright: ignore[reportPrivateUsage]
                        pid=leader_pid,
                        state="D",
                        parent_pid=leader_pid,
                        process_group_id=process_group_id,
                    ),
                ),
                False,
            )
        return original_snapshot(process_group_id, leader_pid)

    monkeypatch.setattr(
        runner_module.ToolRunner,
        "_group_snapshot",
        staticmethod(blocked_snapshot),
    )
    monkeypatch.setattr(
        runner_module.ToolRunner,
        "_signal_group",
        staticmethod(signal_group),
    )

    def observe(stuck) -> None:
        observed.append(stuck)
        observed_at.append(time.monotonic())
        threading.Timer(0.05, release.set).start()

    with pytest.raises(ToolCancelledError):
        runner_module.ToolRunner(
            termination_grace_seconds=0.01, cleanup_observer=observe
        ).run(
            command(tmp_path, "import time; time.sleep(10)"),
            cancellation_signal=cancellation,
        )

    assert len(observed) == 1
    assert observed_at[0] - forced_at[0] >= 0.05
    assert observed[0].members == ((observed[0].leader_pid, "D"),)
    assert release.is_set()
