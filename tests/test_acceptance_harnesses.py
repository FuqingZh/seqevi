from __future__ import annotations

import runpy
import signal
from pathlib import Path
from typing import Any, cast

import pytest


def _c2() -> dict[str, Any]:
    return cast(dict[str, Any], runpy.run_path("benchmarks/c2_acceptance.py"))


class _Process:
    def __init__(self) -> None:
        self.return_code: int | None = None
        self.signals: list[signal.Signals] = []
        self.waited = False

    def poll(self) -> int | None:
        return self.return_code

    def send_signal(self, sent_signal: signal.Signals) -> None:
        self.signals.append(sent_signal)
        self.return_code = -int(sent_signal)

    def wait(self) -> int:
        self.waited = True
        assert self.return_code is not None
        return self.return_code


class _InterruptingWaitProcess(_Process):
    def __init__(self) -> None:
        super().__init__()
        self.wait_attempts = 0

    def wait(self) -> int:
        self.wait_attempts += 1
        if self.wait_attempts == 1:
            raise SystemExit(7)
        return super().wait()


def test_c2_interrupt_terminates_and_reaps_every_initial_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _c2()
    children = {"blf": _Process(), "uniprot": _Process()}

    def interrupt(_seconds: float) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr("time.sleep", interrupt)

    with pytest.raises(KeyboardInterrupt):
        module["_wait_initial"](children)

    assert all(child.signals == [signal.SIGINT] for child in children.values())
    assert all(child.waited for child in children.values())


def test_c2_cleanup_defers_base_exception_until_child_is_reaped() -> None:
    stop_and_reap = _c2()["_stop_and_reap"]
    child = _InterruptingWaitProcess()

    deferred = stop_and_reap({"blf": child})

    assert isinstance(deferred, SystemExit)
    assert child.waited is True
    assert child.wait_attempts == 2


def test_c2_frozen_status_validation_rejects_changed_composition() -> None:
    validate = _c2()["_validate_frozen_result"]
    with pytest.raises(RuntimeError, match="frozen hit/no-hit"):
        validate("blf", {"counts": {"hits": 8776, "no_hits": 340}})


def test_c2_frozen_input_digests_and_threads_are_hard_gates(tmp_path: Path) -> None:
    module = _c2()
    wrong_blf = tmp_path / "blf.fasta"
    wrong_uniprot = tmp_path / "uniprot.fasta"
    wrong_blf.write_text(">wrong\nM\n", encoding="utf-8")
    wrong_uniprot.write_text(">wrong\nM\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="SHA-256"):
        module["_validate_frozen_inputs"]({"blf": wrong_blf, "uniprot": wrong_uniprot})
    module["_validate_threads"](64)
    with pytest.raises(ValueError, match="exactly 64"):
        module["_validate_threads"](1)
    module["_validate_profile"]("eggnog-5.0.2")
    with pytest.raises(ValueError, match="profile eggnog-5.0.2"):
        module["_validate_profile"]("diagnostic-profile")


def test_pressure_listener_includes_final_cleanup_deletes() -> None:
    module = cast(
        dict[str, Any], runpy.run_path("benchmarks/claim_session_pressure.py")
    )
    records = module["_records_sweep_delete_rows"]
    assert records("sweep") is True
    assert records("final_cleanup") is True
    assert records("renew") is False
