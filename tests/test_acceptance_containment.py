from __future__ import annotations

import os
import signal
import sys
from pathlib import Path

import pytest

import seqevi.runner as runner_module
from benchmarks.acceptance_containment import (
    AcceptanceTimeouts,
    start_contained_annotation,
)
from seqevi.runner import ToolTimeoutError


def _is_live(pid: int) -> bool:
    path = Path(f"/proc/{pid}/stat")
    try:
        state = path.read_text(encoding="ascii").split(") ", 1)[1][0]
    except FileNotFoundError:
        return False
    return state not in {"Z", "X"}


def _live_group_members(process_group_id: int) -> list[int]:
    members: list[int] = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdecimal():
            continue
        try:
            fields = (
                entry.joinpath("stat")
                .read_text(encoding="ascii")
                .rsplit(") ", 1)[1]
                .split()
            )
        except (FileNotFoundError, ProcessLookupError):
            continue
        if int(fields[2]) == process_group_id and fields[0] not in {"Z", "X"}:
            members.append(int(entry.name))
    return members


def _start(
    tmp_path: Path,
    code: str,
    *,
    internal_seconds: float = 0.1,
    watchdog_seconds: float = 0.5,
):
    return start_contained_annotation(
        arguments=(
            sys.executable,
            "-c",
            code,
            "annotate",
            "--timeout-seconds",
            str(internal_seconds),
        ),
        working_dir=tmp_path,
        stdout_path=tmp_path / "stdout.log",
        stderr_path=tmp_path / "stderr.log",
        environment={},
        timeouts=AcceptanceTimeouts(
            internal_seconds=internal_seconds,
            watchdog_seconds=watchdog_seconds,
            termination_grace_seconds=0.1,
        ),
    )


def test_watchdog_terms_kills_reaps_and_empties_hanging_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    parent_pid = tmp_path / "parent.pid"
    child_pid = tmp_path / "child.pid"
    parent_term = tmp_path / "parent.term"
    child_term = tmp_path / "child.term"
    child_code = (
        "import pathlib,signal,time;"
        f"marker=pathlib.Path({str(child_term)!r});"
        "signal.signal(signal.SIGTERM,lambda *_:marker.write_text('term'));"
        "time.sleep(30)"
    )
    code = (
        "import os,pathlib,signal,subprocess,sys,time;"
        f"child=subprocess.Popen([sys.executable,'-c',{child_code!r}]);"
        f"pathlib.Path({str(parent_pid)!r}).write_text(str(os.getpid()));"
        f"pathlib.Path({str(child_pid)!r}).write_text(str(child.pid));"
        f"marker=pathlib.Path({str(parent_term)!r});"
        "signal.signal(signal.SIGTERM,lambda *_:marker.write_text('term'));"
        "time.sleep(30)"
    )
    signals: list[signal.Signals] = []
    original_killpg = runner_module.os.killpg

    def killpg(pid: int, sent_signal: signal.Signals) -> None:
        signals.append(sent_signal)
        original_killpg(pid, sent_signal)

    monkeypatch.setattr(runner_module.os, "killpg", killpg)
    process = _start(tmp_path, code)

    with pytest.raises(ToolTimeoutError) as raised:
        process.wait(timeout=3)

    leader = int(parent_pid.read_text())
    descendant = int(child_pid.read_text())
    assert raised.value.result.timed_out is True
    assert signal.SIGTERM in signals
    assert signal.SIGKILL in signals
    assert parent_term.read_text() == "term"
    assert child_term.read_text() == "term"
    assert not _is_live(leader)
    assert not _is_live(descendant)
    assert _live_group_members(leader) == []
    with pytest.raises(ChildProcessError):
        os.waitpid(leader, os.WNOHANG)


def test_leader_exit_before_descendant_still_empties_owned_boundary(
    tmp_path: Path,
) -> None:
    leader_pid = tmp_path / "leader.pid"
    child_pid = tmp_path / "child.pid"
    child_term = tmp_path / "child.term"
    child_code = (
        "import pathlib,signal,time;"
        f"marker=pathlib.Path({str(child_term)!r});"
        "signal.signal(signal.SIGTERM,lambda *_:marker.write_text('term'));"
        "time.sleep(30)"
    )
    code = (
        "import os,pathlib,subprocess,sys;"
        f"child=subprocess.Popen([sys.executable,'-c',{child_code!r}]);"
        f"pathlib.Path({str(leader_pid)!r}).write_text(str(os.getpid()));"
        f"pathlib.Path({str(child_pid)!r}).write_text(str(child.pid))"
    )

    process = _start(tmp_path, code, watchdog_seconds=2.0)
    assert process.wait(timeout=3) == 0

    leader = int(leader_pid.read_text())
    descendant = int(child_pid.read_text())
    assert child_term.read_text() == "term"
    assert not _is_live(leader)
    assert not _is_live(descendant)
    assert _live_group_members(leader) == []
    with pytest.raises(ChildProcessError):
        os.waitpid(leader, os.WNOHANG)


@pytest.mark.parametrize(
    ("internal", "watchdog", "grace"),
    ((0.0, 1.0, 0.1), (1.0, 1.0, 0.1), (1.0, 2.0, 60.1)),
)
def test_timeout_contract_fails_closed(
    internal: float, watchdog: float, grace: float
) -> None:
    with pytest.raises(ValueError):
        AcceptanceTimeouts(internal, watchdog, grace)


def test_annotation_timeout_must_match_internal_contract(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="does not match"):
        start_contained_annotation(
            arguments=("seqevi", "annotate", "--timeout-seconds", "10"),
            working_dir=tmp_path,
            stdout_path=tmp_path / "stdout.log",
            stderr_path=tmp_path / "stderr.log",
            environment={},
            timeouts=AcceptanceTimeouts(11, 12, 1),
        )
