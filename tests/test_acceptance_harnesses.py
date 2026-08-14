from __future__ import annotations

import errno
import runpy
import signal
from copy import deepcopy
from pathlib import Path
from typing import Any, cast

import duckdb
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


def test_c2_result_identity_is_frozen_fail_closed(tmp_path: Path) -> None:
    module = _c2()
    path = tmp_path / "result.duckdb"
    expected = module["_FROZEN_RESULT_IDENTITY"]
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE SCHEMA _seqevi")
        connection.execute(
            "CREATE TABLE _seqevi.metadata ("
            '"AdapterContractVersion" VARCHAR, "ToolRuntimeDigest" VARCHAR, '
            '"ResourceID" VARCHAR, "InputDigest" VARCHAR)'
        )
        connection.execute(
            "INSERT INTO _seqevi.metadata VALUES (?, ?, ?, ?)",
            (
                expected["AdapterContractVersion"],
                expected["ToolRuntimeDigest"],
                expected["ResourceID"],
                module["_FROZEN_INPUT_SHA256"]["blf"],
            ),
        )
    assert module["_validate_result_identity"]("blf", path) == {
        **expected,
        "InputDigest": module["_FROZEN_INPUT_SHA256"]["blf"],
    }
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            'UPDATE _seqevi.metadata SET "ResourceID" = ?', ["wrong-resource"]
        )
    with pytest.raises(RuntimeError, match="frozen C2 metadata"):
        module["_validate_result_identity"]("blf", path)


def test_c2_store_metadata_identity_is_frozen_fail_closed() -> None:
    module = _c2()
    digest = "identity-digest"
    after_initial = {
        "evidence": 9116,
        "evidence_identity_sha256": digest,
        "evidence_contract_identities": [module["_FROZEN_RESULT_IDENTITY"]],
    }
    final = {
        **after_initial,
        "claim_sessions": 0,
        "session_claims": 0,
        "missing_artifact_references": 0,
    }
    module["_validate_database_acceptance"](after_initial, final, digest)
    wrong = deepcopy(final)
    wrong["evidence_contract_identities"] = [
        {**module["_FROZEN_RESULT_IDENTITY"], "ResourceID": "wrong-resource"}
    ]
    with pytest.raises(RuntimeError, match="database readback"):
        module["_validate_database_acceptance"](after_initial, wrong, digest)


def test_c2_external_process_inspection_ambiguity_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _c2()

    class Entry:
        name = "123"
        path = "/proc/123"

    monkeypatch.setattr("os.scandir", lambda _path: (Entry(),))

    def denied(_path: Path) -> bytes:
        raise PermissionError(errno.EACCES, "denied")

    monkeypatch.setattr(Path, "read_bytes", denied)
    with pytest.raises(RuntimeError, match="inspection is ambiguous"):
        module["_external_tool_processes"](tmp_path)


def test_c2_external_process_gate_rejects_orphans_immediately(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _c2()
    globals_ = module["_require_no_external_tools"].__globals__
    monkeypatch.setitem(globals_, "_external_tool_processes", lambda _root: [123])
    with pytest.raises(RuntimeError, match="immediately after initial children"):
        module["_require_no_external_tools"](
            tmp_path, "immediately after initial children"
        )


def test_c2_store_binding_requires_fresh_matching_shared_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _c2()
    zero_counts = {
        table: 0
        for table in (
            "sequence",
            "artifact",
            "evidence",
            "evidence_claim_generations",
            "claim_sessions",
            "session_claims",
            "claim_session_open_receipts",
            "claim_session_acquire_receipts",
            "claim_session_acquire_receipt_items",
        )
    }
    with pytest.raises(RuntimeError, match="shared HTTP Store"):
        module["_validate_store_database_binding"](
            "/tmp/local-store", "postgresql://database"
        )

    globals_ = module["_validate_store_database_binding"].__globals__
    monkeypatch.setitem(globals_, "_database_counts", lambda _url, _tables: zero_counts)

    class Response:
        def __init__(self, payload: dict[str, object]) -> None:
            self.payload = payload

        def raise_for_status(self) -> None:
            pass

        def json(self) -> dict[str, object]:
            return self.payload

    class Client:
        def __init__(self, **_kwargs: object) -> None:
            self.closed = False

        def __enter__(self):
            return self

        def __exit__(self, *_error: object) -> None:
            self.closed = True

        def get(self, _path: str) -> Response:
            return Response({"server_time": "2026-08-14T00:00:00+00:00"})

        def post(self, path: str, **_kwargs: object) -> Response:
            if path.endswith("/open"):
                return Response(
                    {"session_id": "probe", "owner_token": "token", "generation": 1}
                )
            return Response({"closed": True})

    monkeypatch.setitem(globals_, "httpx", type("Httpx", (), {"Client": Client}))
    monkeypatch.setitem(globals_, "_database_has_session", lambda _url, _id: False)
    with pytest.raises(RuntimeError, match="not bound to the readback database"):
        module["_validate_store_database_binding"](
            "http://store", "postgresql://database"
        )

    removed: list[tuple[str, str]] = []
    monkeypatch.setitem(globals_, "_database_has_session", lambda _url, _id: True)
    monkeypatch.setitem(
        globals_,
        "_remove_binding_probe",
        lambda _url, session_id, request_id: removed.append((session_id, request_id)),
    )
    request_id = module["_validate_store_database_binding"](
        "http://store", "postgresql://database"
    )
    assert removed == [("probe", request_id)]

    stale = dict(zero_counts)
    stale["evidence"] = 1
    monkeypatch.setitem(globals_, "_database_counts", lambda _url, _tables: stale)
    with pytest.raises(RuntimeError, match="fresh PostgreSQL database"):
        module["_validate_store_database_binding"](
            "http://store", "postgresql://database"
        )


@pytest.mark.parametrize(
    "violation",
    ["sql", "transactions", "latency", "pool_wait", "terminal"],
)
def test_pressure_report_invariants_fail_closed(violation: str) -> None:
    module = cast(
        dict[str, Any], runpy.run_path("benchmarks/claim_session_pressure.py")
    )
    phase_sql = module["_PHASE_SQL_PER_OPERATION"]
    operations = {"acquire": 1, "renew": 2, "finalize": 1, "close": 1, "sweep": 1}
    phases = {
        phase: {
            "operations": count,
            "sql_executions": count * phase_sql[phase],
            "transactions": count,
            "pool_checkouts": count,
            "p50_seconds": 0.1,
            "p95_seconds": 0.2,
            "p99_seconds": 0.3,
            "maximum_seconds": 0.4,
            "pool_wait_p95_seconds": 0.01,
            "pool_wait_maximum_seconds": 0.02,
        }
        for phase, count in operations.items()
    }
    report = {
        "claims": 100,
        "heartbeat_calls": 2,
        "http_status_counts": {"412": 0, "503": 0},
        "phases": phases,
        "sweep_delete_rows": {},
        "residual": {
            "claim_sessions": 1,
            "session_claims": 0,
            "claim_session_acquire_receipts": 1,
            "claim_session_acquire_receipt_items": 100,
        },
        "terminal_evidence_total": 100,
    }
    validate = module["_validate_lane_report"]
    validate(
        report,
        expected_terminal=100,
        expected_sessions=1,
        expected_receipt_headers=1,
    )
    invalid = deepcopy(report)
    if violation == "sql":
        invalid["phases"]["renew"]["sql_executions"] += 1
    elif violation == "transactions":
        invalid["phases"]["acquire"]["transactions"] = 2
    elif violation == "latency":
        invalid["phases"]["finalize"]["maximum_seconds"] = 5.0
    elif violation == "pool_wait":
        invalid["phases"]["close"]["pool_wait_maximum_seconds"] = 5.0
    else:
        invalid["terminal_evidence_total"] = 99
    with pytest.raises(RuntimeError):
        validate(
            invalid,
            expected_terminal=100,
            expected_sessions=1,
            expected_receipt_headers=1,
        )
