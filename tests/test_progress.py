from __future__ import annotations

import io
import logging
import time
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from seqevi.annotate import run_annotation
from seqevi.cli import _ProgressRenderer, _render_progress_event
from seqevi.evidence import (
    BusyEvidenceClaim,
    ClaimDisposition,
    CommitOutcome,
    EvidenceCommit,
    EvidenceKey,
    EvidenceQuery,
    EvidenceRecord,
    FetchedEvidence,
    SessionClaimAcquireResult,
)
from seqevi.progress import (
    ProgressEvent,
    ProgressPhase,
    ProgressState,
    ProgressUnit,
    emit_progress,
)
from seqevi.store import LocalStore

from .support import FixtureAdapter, NeverRunAdapter, write_fixture_database
from .support import write_fixture_tool


class _BusyOnceStore:
    def __init__(self, delegate: LocalStore) -> None:
        self.delegate = delegate

    @property
    def supports_claim_sessions(self) -> bool:
        return True

    def lookup_many(
        self, requested_queries: Iterable[EvidenceQuery]
    ) -> dict[EvidenceKey, EvidenceRecord]:
        return self.delegate.lookup_many(requested_queries)

    def commit_many(
        self, proposed_commits: Iterable[EvidenceCommit]
    ) -> tuple[CommitOutcome, ...]:
        return self.delegate.commit_many(proposed_commits)

    def fetch_many(
        self, keys: Iterable[EvidenceKey]
    ) -> dict[EvidenceKey, FetchedEvidence]:
        return self.delegate.fetch_many(keys)

    def fetch(self, key: EvidenceKey) -> FetchedEvidence | None:
        return self.delegate.fetch(key)

    def claim_session(self):
        delegate = self.delegate.claim_session()

        class BusyOnceSession:
            cancellation_signal = delegate.cancellation_signal

            def __init__(self) -> None:
                self.acquire_calls = 0

            def __enter__(self):
                delegate.__enter__()
                return self

            def __exit__(self, *error: object) -> None:
                delegate.__exit__(*error)

            def acquire_many(
                self, requested_queries: Iterable[EvidenceQuery]
            ) -> tuple[SessionClaimAcquireResult, ...]:
                queries = tuple(requested_queries)
                self.acquire_calls += 1
                if self.acquire_calls == 1:
                    expiry = datetime.now(UTC) + timedelta(seconds=1)
                    return tuple(
                        SessionClaimAcquireResult(
                            disposition=ClaimDisposition.BUSY,
                            busy=BusyEvidenceClaim(query.key, expiry, 0.001),
                        )
                        for query in queries
                    )
                return delegate.acquire_many(queries)

            def finalize_many(
                self, proposed: Iterable[EvidenceCommit]
            ) -> tuple[CommitOutcome, ...]:
                return delegate.finalize_many(proposed)

            def raise_if_lost(self) -> None:
                delegate.raise_if_lost()

            def close(self) -> None:
                delegate.close()

        return BusyOnceSession()


@pytest.mark.parametrize(
    "event",
    [
        ProgressEvent(
            ProgressPhase.STORE_LOOKUP,
            ProgressState.RUNNING,
            "lookup",
            completed=1,
            total=2,
            unit=ProgressUnit.SEQUENCES,
        ),
        ProgressEvent(
            ProgressPhase.TOOL,
            ProgressState.RUNNING,
            "tool running",
        ),
    ],
)
def test_progress_event_accepts_exact_or_unknown_work(event: ProgressEvent) -> None:
    assert event.message


@pytest.mark.parametrize(
    "kwargs",
    [
        {"completed": 1},
        {"completed": 1, "total": 2},
        {"unit": ProgressUnit.SEQUENCES},
        {"completed": 3, "total": 2, "unit": ProgressUnit.SEQUENCES},
        {"completed": -1, "total": 2, "unit": ProgressUnit.SEQUENCES},
        {"elapsed_seconds": -0.1},
    ],
)
def test_progress_event_rejects_invented_or_invalid_counts(
    kwargs: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        ProgressEvent(
            ProgressPhase.STORE_LOOKUP,
            ProgressState.RUNNING,
            "lookup",
            **kwargs,  # type: ignore[arg-type]
        )


def test_progress_sink_failure_is_diagnostic_and_non_blocking(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def fail(_event: ProgressEvent) -> None:
        raise RuntimeError("renderer failed")

    with caplog.at_level(logging.ERROR, logger="seqevi.progress"):
        emit_progress(
            fail,
            ProgressEvent(
                ProgressPhase.ANNOTATION,
                ProgressState.STARTED,
                "starting",
            ),
        )

    assert "annotation progress sink failed" in caplog.text


def test_progress_sink_failure_does_not_change_annotation_result(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">hit\nMPEPTIDE\n", encoding="utf-8")
    adapter = FixtureAdapter(
        executable=write_fixture_tool(tmp_path / "fixture-tool"),
        database=write_fixture_database(tmp_path / "database"),
    )

    def fail(_event: ProgressEvent) -> None:
        raise RuntimeError("renderer failed")

    with caplog.at_level(logging.ERROR, logger="seqevi.progress"):
        with LocalStore.open(tmp_path / "store") as store:
            summary = run_annotation(
                fasta_path=fasta,
                output_dir=tmp_path / "result.duckdb",
                adapter=adapter,
                store=store,
                progress_sink=fail,
            )

    assert summary.computed == 1
    assert summary.output_dir.is_file()
    assert "annotation progress sink failed" in caplog.text


def test_progress_renderer_formats_only_exact_percentage() -> None:
    exact = _render_progress_event(
        ProgressEvent(
            ProgressPhase.STORE_LOOKUP,
            ProgressState.RUNNING,
            "store lookup",
            completed=2_000,
            total=2_729,
            unit=ProgressUnit.SEQUENCES,
        ),
        elapsed_seconds=754,
    )
    unknown = _render_progress_event(
        ProgressEvent(
            ProgressPhase.TOOL,
            ProgressState.RUNNING,
            "InterProScan/Pfam · tool batch 1 running",
        ),
        elapsed_seconds=754,
    )

    assert exact == "[seqevi] store lookup · 2,000/2,729 sequences (73%) · 12m34s"
    assert unknown == "[seqevi] InterProScan/Pfam · tool batch 1 running · 12m34s"
    assert "%" not in unknown


def test_progress_renderer_refreshes_elapsed_and_clears_on_close() -> None:
    stream = io.StringIO()
    clock = [0.0]
    renderer = _ProgressRenderer(
        stream=stream,
        refresh_seconds=0.01,
        monotonic=lambda: clock[0],
    )
    renderer(
        ProgressEvent(
            ProgressPhase.TOOL,
            ProgressState.STARTED,
            "tool batch 1 running",
        )
    )
    clock[0] = 65.0
    time.sleep(0.03)
    renderer.close()

    rendered = stream.getvalue()
    assert "[seqevi] tool batch 1 running · 0s" in rendered
    assert "[seqevi] tool batch 1 running · 1m05s" in rendered
    assert rendered.endswith("\r\x1b[K")


def test_run_annotation_emits_cold_and_cache_event_boundaries(tmp_path: Path) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">hit\nMPEPTIDE\n>none\nMNOHITX\n", encoding="utf-8")
    adapter = FixtureAdapter(
        executable=write_fixture_tool(tmp_path / "fixture-tool"),
        database=write_fixture_database(tmp_path / "database"),
    )
    cold_events: list[ProgressEvent] = []
    cache_events: list[ProgressEvent] = []

    with LocalStore.open(tmp_path / "store") as store:
        run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "cold.duckdb",
            adapter=adapter,
            store=store,
            progress_sink=cold_events.append,
        )
        run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "cache.duckdb",
            adapter=NeverRunAdapter(adapter),
            store=store,
            progress_sink=cache_events.append,
        )

    cold_phases = [event.phase for event in cold_events]
    assert cold_phases == [
        ProgressPhase.STAGING,
        ProgressPhase.STAGING,
        ProgressPhase.STORE_LOOKUP,
        ProgressPhase.STORE_LOOKUP,
        ProgressPhase.TOOL,
        ProgressPhase.TOOL,
        ProgressPhase.STORE_COMMIT,
        ProgressPhase.STORE_COMMIT,
        ProgressPhase.STORE_LOOKUP,
        ProgressPhase.STORE_FETCH,
        ProgressPhase.STORE_FETCH,
        ProgressPhase.PACKAGE,
        ProgressPhase.PACKAGE,
    ]
    assert ProgressPhase.TOOL not in {event.phase for event in cache_events}
    assert ProgressPhase.STORE_COMMIT not in {event.phase for event in cache_events}
    ratios = [event for event in cold_events if event.completed is not None]
    assert ratios
    assert all(event.unit is ProgressUnit.SEQUENCES for event in ratios)


def test_run_annotation_reports_claim_wait_without_invented_count(
    tmp_path: Path,
) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">hit\nMPEPTIDE\n", encoding="utf-8")
    adapter = FixtureAdapter(
        executable=write_fixture_tool(tmp_path / "fixture-tool"),
        database=write_fixture_database(tmp_path / "database"),
    )
    events: list[ProgressEvent] = []

    with LocalStore.open(tmp_path / "store") as store:
        run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "result.duckdb",
            adapter=adapter,
            store=_BusyOnceStore(store),
            progress_sink=events.append,
        )

    wait_events = [event for event in events if event.phase is ProgressPhase.CLAIM_WAIT]
    assert [event.state for event in wait_events] == [
        ProgressState.RUNNING,
        ProgressState.COMPLETED,
    ]
    assert all(event.completed is None for event in wait_events)
