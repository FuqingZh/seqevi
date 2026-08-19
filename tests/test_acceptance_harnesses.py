from __future__ import annotations

import argparse
import errno
import math
import runpy
import signal
import subprocess
import sys
from collections import defaultdict
from copy import deepcopy
from pathlib import Path
from typing import Any, cast

import duckdb
import pytest


def _c2() -> dict[str, Any]:
    return cast(dict[str, Any], runpy.run_path("benchmarks/c2_acceptance.py"))


def _pressure_lane_report(
    module: dict[str, Any], *, claims: int, lane_count: int
) -> dict[str, Any]:
    phase_sql = module["_PHASE_SQL_PER_OPERATION"]
    receipt_headers = math.ceil(claims / 1000)
    operations = {
        "acquire": receipt_headers,
        "renew": 1,
        "finalize": receipt_headers,
        "close": 1,
        "sweep": 1,
    }
    return {
        "claims": claims,
        "heartbeat_calls": 1,
        "sweep_calls_returning_work": 0,
        "http_status_observation": "unavailable-direct-persistence",
        "phases": {
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
        },
        "intervals": {
            "renew": [(1.5, 2.5)],
            "acquire": [(1.0, 2.0)],
            "finalize": [(3.0, 4.0)],
        },
        "sweep_delete_rows": {},
        "current_lane_residual": {
            "claim_sessions": 1,
            "session_claims": 0,
            "claim_session_open_receipts": 1,
            "claim_session_acquire_receipts": receipt_headers,
            "claim_session_acquire_receipt_items": claims,
            "terminal_evidence": claims,
        },
        "residual": {
            "claim_sessions": 1,
            "session_claims": 0,
            "claim_session_open_receipts": lane_count,
            "claim_session_acquire_receipts": receipt_headers,
            "claim_session_acquire_receipt_items": claims,
        },
        "terminal_evidence_total": claims,
    }


class _Process:
    def __init__(self) -> None:
        self.return_code: int | None = None
        self.signals: list[signal.Signals] = []
        self.waited = False
        self.cancellation_requested = False

    def poll(self) -> int | None:
        return self.return_code

    def send_signal(self, sent_signal: signal.Signals) -> None:
        self.signals.append(sent_signal)
        self.cancellation_requested = True
        self.return_code = -int(sent_signal)

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        self.waited = True
        assert self.return_code is not None
        return self.return_code


class _InterruptingWaitProcess(_Process):
    def __init__(self) -> None:
        super().__init__()
        self.wait_attempts = 0

    def wait(self, timeout: float | None = None) -> int:
        self.wait_attempts += 1
        if self.wait_attempts == 1:
            raise SystemExit(7)
        return super().wait(timeout)


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


def test_c2_resolves_relative_filesystem_arguments_from_caller_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _c2()
    monkeypatch.chdir(tmp_path)
    args = argparse.Namespace(
        blf=Path("inputs/blf.fasta"),
        uniprot=Path("inputs/uniprot.fasta"),
        output_root=Path("evidence/run"),
    )

    module["_resolve_filesystem_arguments"](args)

    assert args.blf == tmp_path / "inputs/blf.fasta"
    assert args.uniprot == tmp_path / "inputs/uniprot.fasta"
    assert args.output_root == tmp_path / "evidence/run"


def test_c2_candidate_state_rejects_drift_between_child_launches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _c2()
    globals_ = module["_verify_candidate_state"].__globals__

    def check_output(command: list[str], **_kwargs: object) -> str:
        if "rev-parse" in command:
            return "changed-head\n"
        return ""

    monkeypatch.setattr(globals_["subprocess"], "check_output", check_output)
    with pytest.raises(RuntimeError, match="before replay blf child lifecycle"):
        module["_verify_candidate_state"](
            "expected-head", "before replay blf child lifecycle"
        )


def test_c2_candidate_state_rejects_final_report_worktree_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _c2()
    globals_ = module["_verify_candidate_state"].__globals__

    def check_output(command: list[str], **_kwargs: object) -> str:
        if "rev-parse" in command:
            return "expected-head\n"
        return "?? untracked-evidence\n"

    monkeypatch.setattr(globals_["subprocess"], "check_output", check_output)
    with pytest.raises(RuntimeError, match="before accepted report write"):
        module["_verify_candidate_state"](
            "expected-head", "before accepted report write"
        )


def test_c2_child_environment_overrides_stale_pythonpath(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _c2()
    monkeypatch.chdir(tmp_path)
    source_root = module["_candidate_source_root"]()
    monkeypatch.setenv("PYTHONPATH", "/stale/installed/package")

    environment = module["_candidate_child_environment"](source_root)

    assert environment["PYTHONPATH"] == str(source_root.resolve())
    assert "/stale/installed/package" not in environment["PYTHONPATH"]
    assert (source_root / "seqevi" / "__init__.py").is_file()
    assert module["_child_environment_record"](environment) == {
        "PYTHONPATH": str(source_root.resolve())
    }


def test_c2_preflight_ignores_cwd_shadow_package(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _c2()
    shadow = tmp_path / "seqevi"
    shadow.mkdir()
    (shadow / "__init__.py").write_text("SHADOW = True\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    source_root = module["_candidate_source_root"]()
    environment = module["_candidate_child_environment"](source_root)

    imported = Path(module["_preflight_candidate_import"](source_root, environment))

    assert imported.is_relative_to(source_root / "seqevi")
    assert not imported.is_relative_to(shadow)


def test_c2_annotation_child_uses_safe_candidate_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _c2()
    source_root = module["_candidate_source_root"]()
    observed: dict[str, object] = {}

    process = object()

    def start_contained_annotation(**kwargs: object) -> object:
        observed.update(kwargs)
        return process

    monkeypatch.setitem(
        module["_run_command"].__globals__,
        "start_contained_annotation",
        start_contained_annotation,
    )
    returned = module["_run_command"](
        fasta=tmp_path / "input.fasta",
        output=tmp_path / "result.duckdb",
        profile="eggnog-5.0.2",
        store="http://127.0.0.1:8000",
        threads=64,
        stdout_path=tmp_path / "stdout.json",
        stderr_path=tmp_path / "stderr.log",
        environment={"PYTHONPATH": str(source_root)},
        candidate_source_root=source_root,
    )

    arguments = cast(tuple[str, ...], observed["arguments"])
    assert returned is process
    assert arguments[1] == "-P"
    assert (
        Path(arguments[2]).resolve()
        == Path("benchmarks/acceptance_annotation.py").resolve()
    )
    assert arguments[arguments.index("--timeout-seconds") + 1] == "21600.0"
    assert observed["working_dir"] == source_root.parent
    assert cast(dict[str, str], observed["environment"])["PYTHONPATH"] == str(
        source_root
    )
    timeouts = cast(Any, observed["timeouts"])
    assert timeouts.internal_seconds == 21_600
    assert timeouts.watchdog_seconds == 21_660
    assert timeouts.termination_grace_seconds == 30


def test_c2_annotation_child_entry_starts_with_isolated_candidate_path() -> None:
    module = _c2()
    source_root = module["_candidate_source_root"]()
    environment = module["_candidate_child_environment"](source_root)
    entry = Path("benchmarks/acceptance_annotation.py").resolve()

    completed = subprocess.run(
        [sys.executable, "-P", str(entry), "--help"],
        cwd=source_root.parent,
        env=environment,
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )

    assert environment["PYTHONPATH"] == str(source_root.resolve())
    assert completed.returncode == 0, completed.stderr
    assert "annotate" in completed.stdout


def test_pressure_listener_includes_final_cleanup_deletes() -> None:
    module = cast(
        dict[str, Any], runpy.run_path("benchmarks/claim_session_pressure.py")
    )
    records = module["_records_sweep_delete_rows"]
    assert records("sweep") is True
    assert records("final_cleanup") is True
    assert records("renew") is False


def test_pressure_counts_are_frozen_for_acceptance() -> None:
    module = cast(
        dict[str, Any], runpy.run_path("benchmarks/claim_session_pressure.py")
    )
    validate = module["_validate_counts"]
    validate([100, 1000, 3000, 9116])
    for counts in ([100], [100, 1000, 3000], [100, 1000, 3000, 9117]):
        with pytest.raises(ValueError, match="requires counts"):
            validate(counts)


def test_pressure_candidate_root_rejects_stale_import(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = cast(
        dict[str, Any], runpy.run_path("benchmarks/claim_session_pressure.py")
    )
    globals_ = module["_candidate_repository_root"].__globals__
    monkeypatch.setattr(
        globals_["persistence_module"],
        "__file__",
        str(tmp_path / "persistence.py"),
    )

    with pytest.raises(RuntimeError, match="not bound to candidate persistence"):
        module["_candidate_repository_root"]()


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
        "claim_session_open_receipts": 0,
        "missing_artifact_references": 0,
    }
    module["_validate_database_acceptance"](after_initial, final, digest)
    wrong = deepcopy(final)
    wrong["evidence_contract_identities"] = [
        {**module["_FROZEN_RESULT_IDENTITY"], "ResourceID": "wrong-resource"}
    ]
    with pytest.raises(RuntimeError, match="database readback"):
        module["_validate_database_acceptance"](after_initial, wrong, digest)

    residual = deepcopy(final)
    residual["claim_session_open_receipts"] = 1
    with pytest.raises(RuntimeError, match="database readback"):
        module["_validate_database_acceptance"](after_initial, residual, digest)


def test_c2_retention_cleanup_waits_then_uses_candidate_sweeper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _c2()
    events: list[object] = []

    class Persistence:
        outcomes = iter((1, 0))

        def sweep_claim_sessions(self) -> int:
            events.append("sweep")
            return next(self.outcomes)

        def close(self) -> None:
            events.append("close")

    globals_ = module["_retention_cleanup"].__globals__
    monkeypatch.setattr(
        globals_["PostgresEvidencePersistence"],
        "open",
        staticmethod(lambda _url: Persistence()),
    )
    monkeypatch.setattr(globals_["time"], "sleep", lambda value: events.append(value))

    evidence = module["_retention_cleanup"]("postgresql://db", 121.0)

    assert events == [121.0, "sweep", "sweep", "close"]
    assert evidence["sweep_calls_returning_work"] == 1
    with pytest.raises(RuntimeError, match="120-second"):
        module["_retention_cleanup"]("postgresql://db", 120.0)


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


def test_c2_candidate_store_is_loopback_candidate_and_reaped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _c2()
    source_root = module["_candidate_source_root"]()
    observed: dict[str, object] = {}

    class Process:
        pid = 4321
        returncode = None

        def poll(self) -> None:
            return None

        def wait(self, timeout: float | None = None) -> int:
            observed.setdefault("waits", []).append(timeout)  # type: ignore[union-attr]
            return 0

    class Response:
        def raise_for_status(self) -> None:
            pass

    def popen(command: tuple[str, ...], **kwargs: object) -> Process:
        observed.update(command=command, **kwargs)
        return Process()

    globals_ = module["_start_candidate_store"].__globals__
    monkeypatch.setattr(subprocess, "Popen", popen)
    monkeypatch.setattr(globals_["httpx"], "get", lambda *_args, **_kwargs: Response())
    monkeypatch.setitem(globals_, "_require_unoccupied_store_bind", lambda *_args: None)
    monkeypatch.setitem(globals_, "_child_owns_store_listener", lambda *_args: True)
    leader_states = iter((False, True))
    monkeypatch.setitem(
        globals_, "_store_leader_exited_without_reap", lambda _pid: next(leader_states)
    )
    signalled: list[tuple[int, signal.Signals]] = []
    monkeypatch.setattr("os.killpg", lambda pid, sent: signalled.append((pid, sent)))
    output_root = tmp_path / "evidence"
    output_root.mkdir()

    process, record = module["_start_candidate_store"](
        store="http://127.0.0.1:18083",
        database_url="postgresql://user:secret@fresh/database",
        output_root=output_root,
        source_root=source_root,
        environment={"PYTHONPATH": str(source_root)},
    )
    module["_stop_candidate_store"](process)

    command = cast(tuple[str, ...], observed["command"])
    assert command[1:4] == ("-P", "-m", "seqevi")
    assert command[4] == "serve"
    assert observed["cwd"] == source_root.parent
    assert record["pid"] == 4321
    assert record["source"] == str(source_root / "seqevi")
    assert "secret" not in str(record)
    assert cast(list[str], record["command"])[6] == "<redacted>"
    assert signalled == [(4321, signal.SIGTERM), (4321, signal.SIGKILL)]
    for invalid in ("http://store:8000", "https://127.0.0.1:8000", "/tmp/store"):
        with pytest.raises(RuntimeError, match="explicit http"):
            module["_controlled_store_bind"](invalid)


def test_c2_stale_endpoint_cannot_satisfy_candidate_readiness(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _c2()
    source_root = module["_candidate_source_root"]()
    stopped: list[object] = []

    class Process:
        pid = 4321
        returncode = None

        def poll(self) -> None:
            return None

    class Response:
        def raise_for_status(self) -> None:
            pass

    globals_ = module["_start_candidate_store"].__globals__
    monkeypatch.setattr(subprocess, "Popen", lambda *_args, **_kwargs: Process())
    monkeypatch.setattr(globals_["httpx"], "get", lambda *_args, **_kwargs: Response())
    monkeypatch.setitem(globals_, "_require_unoccupied_store_bind", lambda *_args: None)
    monkeypatch.setitem(globals_, "_child_owns_store_listener", lambda *_args: False)
    monkeypatch.setitem(
        globals_, "_store_leader_exited_without_reap", lambda _pid: False
    )
    monkeypatch.setitem(globals_, "_stop_candidate_store", stopped.append)
    output_root = tmp_path / "evidence"
    output_root.mkdir()

    with pytest.raises(RuntimeError, match="not owned by candidate child"):
        module["_start_candidate_store"](
            store="http://127.0.0.1:18083",
            database_url="postgresql://fresh",
            output_root=output_root,
            source_root=source_root,
            environment={"PYTHONPATH": str(source_root)},
        )

    assert len(stopped) == 1


def test_c2_store_startup_exit_fences_surviving_group_before_reap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _c2()
    source_root = module["_candidate_source_root"]()
    events: list[object] = []

    class Process:
        pid = 4321
        returncode = None

        @staticmethod
        def wait() -> int:
            events.append("reap-leader")
            return 1

    globals_ = module["_start_candidate_store"].__globals__
    monkeypatch.setattr(subprocess, "Popen", lambda *_args, **_kwargs: Process())
    monkeypatch.setitem(globals_, "_require_unoccupied_store_bind", lambda *_args: None)
    leader_states = iter((True, True))

    def observe_without_reap(pid: int) -> bool:
        events.append(("observe-without-reap", pid))
        return next(leader_states)

    monkeypatch.setitem(
        globals_, "_store_leader_exited_without_reap", observe_without_reap
    )
    monkeypatch.setattr(
        "os.killpg", lambda pid, sent: events.append(("signal", pid, sent))
    )
    output_root = tmp_path / "evidence"
    output_root.mkdir()

    with pytest.raises(RuntimeError, match="exited during startup"):
        module["_start_candidate_store"](
            store="http://127.0.0.1:18083",
            database_url="postgresql://fresh",
            output_root=output_root,
            source_root=source_root,
            environment={"PYTHONPATH": str(source_root)},
        )

    assert events == [
        ("observe-without-reap", 4321),
        ("signal", 4321, signal.SIGTERM),
        ("observe-without-reap", 4321),
        ("signal", 4321, signal.SIGKILL),
        "reap-leader",
    ]


def test_c2_occupied_store_bind_fails_before_child_launch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _c2()

    class Probe:
        def __enter__(self):
            return self

        def __exit__(self, *_error: object) -> None:
            pass

        def settimeout(self, _timeout: float) -> None:
            pass

        def connect_ex(self, _address: tuple[str, int]) -> int:
            return 0

    globals_ = module["_require_unoccupied_store_bind"].__globals__
    monkeypatch.setattr(globals_["socket"], "socket", lambda *_args: Probe())

    with pytest.raises(RuntimeError, match="already occupied"):
        module["_require_unoccupied_store_bind"]("127.0.0.1", 18083)


def test_c2_main_failure_always_stops_candidate_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _c2()
    globals_ = module["main"].__globals__
    process = object()
    stopped: list[object] = []
    monkeypatch.setitem(globals_, "_ACTIVE_STORE_PROCESS", process)
    monkeypatch.setitem(
        globals_, "_main", lambda: (_ for _ in ()).throw(KeyboardInterrupt())
    )
    monkeypatch.setitem(globals_, "_stop_candidate_store", stopped.append)

    with pytest.raises(KeyboardInterrupt):
        module["main"]()

    assert stopped == [process]
    assert globals_["_ACTIVE_STORE_PROCESS"] is None


def test_c2_store_normal_leader_exit_fences_surviving_group_before_reap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _c2()
    events: list[object] = []

    class Process:
        pid = 4321
        returncode = None

        @staticmethod
        def wait() -> int:
            events.append("reap-leader")
            return 0

    globals_ = module["_stop_candidate_store"].__globals__
    monkeypatch.setitem(
        globals_,
        "_store_leader_exited_without_reap",
        lambda pid: events.append(("observe-exited-without-reap", pid)) or True,
    )
    monkeypatch.setattr(
        "os.killpg", lambda pid, sent: events.append(("signal", pid, sent))
    )

    module["_stop_candidate_store"](Process())

    assert events == [
        ("signal", 4321, signal.SIGTERM),
        ("observe-exited-without-reap", 4321),
        ("signal", 4321, signal.SIGKILL),
        "reap-leader",
    ]


def test_c2_store_group_fence_never_runs_after_leader_reap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _c2()
    events: list[str] = []

    class Process:
        pid = 4321
        returncode = None

        @staticmethod
        def wait() -> int:
            events.append("reap")
            return 0

    globals_ = module["_stop_candidate_store"].__globals__
    monkeypatch.setitem(
        globals_, "_store_leader_exited_without_reap", lambda _pid: True
    )
    monkeypatch.setattr("os.killpg", lambda _pid, sent: events.append(sent.name))

    module["_stop_candidate_store"](Process())

    assert events == ["SIGTERM", "SIGKILL", "reap"]


def test_c2_binding_probe_removes_exact_closed_zero_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _c2()
    queries: list[tuple[str, tuple[str, str]]] = []

    class Cursor:
        rowcount = 0

        def __enter__(self):
            return self

        def __exit__(self, *_error: object) -> None:
            pass

        def execute(self, query: str, parameters: tuple[str, str]) -> None:
            normalized = " ".join(query.split())
            queries.append((normalized, parameters))
            self.rowcount = (
                1
                if "closed = 0" in normalized or "state = 'closing'" in normalized
                else 0
            )

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_error: object) -> None:
            pass

        def cursor(self) -> Cursor:
            return Cursor()

    globals_ = module["_remove_binding_probe"].__globals__
    monkeypatch.setitem(
        globals_,
        "psycopg",
        type("Psycopg", (), {"connect": lambda *_args: Connection()}),
    )

    module["_remove_binding_probe"]("postgresql://db", "session", "request")

    assert queries[0][1] == ("request", "session")
    assert "closed = 0" in queries[0][0]
    assert queries[1][1] == ("session",)


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
        "sweep_calls_returning_work": 0,
        "http_status_observation": "unavailable-direct-persistence",
        "phases": phases,
        "intervals": {
            "renew": [(1.5, 2.5)],
            "acquire": [(1.0, 2.0)],
            "finalize": [(3.0, 4.0)],
        },
        "sweep_delete_rows": {},
        "current_lane_residual": {
            "claim_sessions": 1,
            "session_claims": 0,
            "claim_session_open_receipts": 1,
            "claim_session_acquire_receipts": 1,
            "claim_session_acquire_receipt_items": 100,
            "terminal_evidence": 100,
        },
        "residual": {
            "claim_sessions": 1,
            "session_claims": 0,
            "claim_session_open_receipts": 1,
            "claim_session_acquire_receipts": 1,
            "claim_session_acquire_receipt_items": 100,
        },
        "terminal_evidence_total": 100,
    }
    validate = module["_validate_lane_report"]
    validate(
        report,
        expected_terminal=100,
        lane_count=1,
        cumulative_receipt_headers=1,
    )
    assert "http_status_counts" not in report
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
            lane_count=1,
            cumulative_receipt_headers=1,
        )


def test_pressure_report_counts_each_sweep_call_as_an_operation() -> None:
    module = cast(
        dict[str, Any], runpy.run_path("benchmarks/claim_session_pressure.py")
    )
    phase_sql = module["_PHASE_SQL_PER_OPERATION"]
    operations = {"acquire": 1, "renew": 1, "finalize": 1, "close": 1, "sweep": 3}
    report = {
        "claims": 100,
        "heartbeat_calls": 1,
        "sweep_calls_returning_work": 2,
        "http_status_observation": "unavailable-direct-persistence",
        "phases": {
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
        },
        "intervals": {
            "renew": [(1.5, 2.5)],
            "acquire": [(1.0, 2.0)],
            "finalize": [(3.0, 4.0)],
        },
        "sweep_delete_rows": {},
        "current_lane_residual": {
            "claim_sessions": 1,
            "session_claims": 0,
            "claim_session_open_receipts": 1,
            "claim_session_acquire_receipts": 1,
            "claim_session_acquire_receipt_items": 100,
            "terminal_evidence": 100,
        },
        "residual": {
            "claim_sessions": 1,
            "session_claims": 0,
            "claim_session_open_receipts": 1,
            "claim_session_acquire_receipts": 1,
            "claim_session_acquire_receipt_items": 100,
        },
        "terminal_evidence_total": 100,
    }

    module["_validate_lane_report"](
        report,
        expected_terminal=100,
        lane_count=1,
        cumulative_receipt_headers=1,
    )


def test_pressure_accepts_retention_cleanup_of_prior_lanes() -> None:
    module = cast(
        dict[str, Any], runpy.run_path("benchmarks/claim_session_pressure.py")
    )
    report = _pressure_lane_report(module, claims=9116, lane_count=4)
    report["residual"] = {
        "claim_sessions": 2,
        "session_claims": 0,
        "claim_session_open_receipts": 4,
        "claim_session_acquire_receipts": 13,
        "claim_session_acquire_receipt_items": 12116,
    }
    report["terminal_evidence_total"] = 13216

    module["_validate_lane_report"](
        report,
        expected_terminal=13216,
        lane_count=4,
        cumulative_receipt_headers=13,
    )


@pytest.mark.parametrize("missing", ["claim_sessions", "terminal_evidence"])
def test_pressure_rejects_loss_of_current_lane_rows_or_evidence(missing: str) -> None:
    module = cast(
        dict[str, Any], runpy.run_path("benchmarks/claim_session_pressure.py")
    )
    report = _pressure_lane_report(module, claims=100, lane_count=1)
    report["current_lane_residual"][missing] = 0

    with pytest.raises(RuntimeError, match="current lane residual"):
        module["_validate_lane_report"](
            report,
            expected_terminal=100,
            lane_count=1,
            cumulative_receipt_headers=1,
        )


def test_pressure_measures_each_sweep_call_including_final_empty_call() -> None:
    module = cast(
        dict[str, Any], runpy.run_path("benchmarks/claim_session_pressure.py")
    )
    phases = []

    class Persistence:
        outcomes = iter((1, 1, 0))

        def sweep_claim_sessions(self) -> int:
            phases.append(module["_phase"].get())
            return next(self.outcomes)

    latencies: dict[str, list[float]] = defaultdict(list)
    calls_returning_work = module["_measure_sweeps"](Persistence(), latencies)

    assert calls_returning_work == 2
    assert phases == ["sweep", "sweep", "sweep"]
    assert len(latencies["sweep"]) == 3


def test_c2_rejects_existing_finalizations_in_initial_run() -> None:
    validate = _c2()["_validate_initial_reuse"]
    results = {
        "blf": {
            "counts": {"cache_hits": 9115, "computed": 1},
            "metrics": {"existing_finalizations": 1},
        },
        "uniprot": {
            "counts": {"cache_hits": 0, "computed": 9115},
            "metrics": {"existing_finalizations": 0},
        },
    }
    with pytest.raises(RuntimeError, match="existing finalizations"):
        validate(results)
    results["blf"]["metrics"] = {"existing_finalizations": 0}
    assert validate(results) == 0


def test_pressure_requires_real_heartbeat_overlap() -> None:
    module = cast(
        dict[str, Any], runpy.run_path("benchmarks/claim_session_pressure.py")
    )
    assert module["_has_interval_overlap"]([(1.5, 2.5)], [(1.0, 2.0), (3.0, 4.0)])
    assert not module["_has_interval_overlap"]([(4.0, 5.0)], [(1.0, 2.0), (2.0, 3.0)])


def test_pressure_failed_lane_closes_and_sweeps_with_aggregated_errors() -> None:
    module = cast(
        dict[str, Any], runpy.run_path("benchmarks/claim_session_pressure.py")
    )
    events: list[str] = []

    class Persistence:
        def close_claim_session(self, _authority: object) -> None:
            events.append("close")
            raise RuntimeError("close failed")

        def sweep_claim_sessions(self) -> bool:
            events.append("sweep")
            return False

    errors = module["_cleanup_failed_lane"](Persistence(), object())
    assert events == ["close", "sweep"]
    assert len(errors) == 1
    assert str(errors[0]) == "close failed"
