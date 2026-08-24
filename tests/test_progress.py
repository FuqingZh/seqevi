from __future__ import annotations

import io
import logging
import time
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from rich.console import Console
from rich.text import Text

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
    BatchProgress,
    ProgressEvent,
    ProgressPhase,
    ProgressState,
    ProgressUnit,
    WorkProgress,
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
            evidence_ready=WorkProgress(1, 2, ProgressUnit.SEQUENCES),
        ),
        ProgressEvent(
            ProgressPhase.TOOL,
            ProgressState.RUNNING,
            "tool running",
            batch=BatchProgress(1, 10),
        ),
    ],
)
def test_progress_event_accepts_exact_or_unknown_work(event: ProgressEvent) -> None:
    assert event.message


@pytest.mark.parametrize(
    ("completed", "total"),
    [
        (3, 2),
        (-1, 2),
        (1, -1),
    ],
)
def test_progress_event_rejects_invented_or_invalid_counts(
    completed: int,
    total: int,
) -> None:
    with pytest.raises(ValueError):
        WorkProgress(completed, total, ProgressUnit.SEQUENCES)


@pytest.mark.parametrize(("number", "size"), [(0, 1), (1, 0), (-1, 2)])
def test_batch_progress_requires_positive_metadata(number: int, size: int) -> None:
    with pytest.raises(ValueError):
        BatchProgress(number, size)


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


def _render_text(
    event: ProgressEvent,
    *,
    elapsed_seconds: float = 754,
    width: int = 120,
) -> str:
    stream = io.StringIO()
    console = Console(
        file=stream,
        width=width,
        color_system=None,
        force_terminal=False,
    )
    console.print(
        _render_progress_event(
            event,
            elapsed_seconds=elapsed_seconds,
            width=width,
        )
    )
    return stream.getvalue()


def test_progress_renderer_shows_exact_cumulative_percentage() -> None:
    exact = _render_text(
        ProgressEvent(
            ProgressPhase.TOOL,
            ProgressState.RUNNING,
            "Running InterProScan/Pfam batch 1",
            evidence_ready=WorkProgress(2_000, 2_729, ProgressUnit.SEQUENCES),
            batch=BatchProgress(1, 729),
        )
    )
    unknown = _render_text(
        ProgressEvent(
            ProgressPhase.TOOL,
            ProgressState.RUNNING,
            "Running managed annotation",
        )
    )

    assert "Running InterProScan/Pfam batch 1 · 729 sequences" in exact
    assert "Unique sequences" in exact
    assert "2,000/2,729" in exact
    assert "73%" in exact
    assert "12m34s elapsed" in exact
    assert "Running managed annotation" in unknown
    assert "12m34s elapsed" in unknown
    assert "%" not in unknown


def test_progress_renderer_drops_bar_but_keeps_ratio_when_compact() -> None:
    rendered = _render_text(
        ProgressEvent(
            ProgressPhase.TOOL,
            ProgressState.RUNNING,
            "Running eggNOG-mapper batch 3",
            evidence_ready=WorkProgress(1_920, 2_729, ProgressUnit.SEQUENCES),
            batch=BatchProgress(3, 809),
        ),
        width=79,
    )

    assert "Unique sequences" not in rendered
    assert "1,920/2,729" in rendered
    assert "70%" in rendered
    assert "12m34s elapsed" in rendered


def test_progress_renderer_replaces_complete_bar_with_active_phase() -> None:
    rendered = _render_text(
        ProgressEvent(
            ProgressPhase.PACKAGE,
            ProgressState.STARTED,
            "Writing result",
            evidence_ready=WorkProgress(2_729, 2_729, ProgressUnit.SEQUENCES),
        )
    )

    assert "Writing result" in rendered
    assert "Unique sequences" not in rendered
    assert "100%" not in rendered


def test_progress_renderer_refreshes_elapsed_and_clears_on_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TERM", "xterm-256color")
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
    plain = Text.from_ansi(rendered).plain
    assert "tool batch 1 running" in plain
    assert "1m05s elapsed" in plain
    assert "\x1b[" in rendered
    assert rendered.endswith("\r\x1b[1A\x1b[2K")


def test_run_annotation_emits_cold_and_cache_event_boundaries(tmp_path: Path) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(
        ">hit\nMPEPTIDE\n>hit-alias\nMPEPTIDE\n>none\nMNOHITX\n",
        encoding="utf-8",
    )
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
    readiness = [
        event.evidence_ready.completed
        for event in cold_events
        if event.evidence_ready is not None
    ]
    assert readiness == sorted(readiness)
    assert readiness[0] == 0
    assert readiness[-1] == 2
    assert all(
        event.evidence_ready.unit is ProgressUnit.SEQUENCES
        for event in cold_events
        if event.evidence_ready is not None
    )
    tool = next(
        event
        for event in cold_events
        if event.phase is ProgressPhase.TOOL and event.state is ProgressState.STARTED
    )
    assert tool.evidence_ready == WorkProgress(0, 2, ProgressUnit.SEQUENCES)
    assert tool.batch == BatchProgress(1, 2)
    cached_lookup = next(
        event
        for event in cache_events
        if event.phase is ProgressPhase.STORE_LOOKUP
        and event.state is ProgressState.RUNNING
    )
    assert cached_lookup.evidence_ready == WorkProgress(2, 2, ProgressUnit.SEQUENCES)


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
    assert all(
        event.evidence_ready == WorkProgress(0, 1, ProgressUnit.SEQUENCES)
        for event in wait_events[:-1]
    )
    assert wait_events[-1].evidence_ready == WorkProgress(1, 1, ProgressUnit.SEQUENCES)


def test_run_annotation_accumulates_mixed_cache_before_computation(
    tmp_path: Path,
) -> None:
    cached_fasta = tmp_path / "cached.fasta"
    cached_fasta.write_text(">hit\nMPEPTIDE\n", encoding="utf-8")
    mixed_fasta = tmp_path / "mixed.fasta"
    mixed_fasta.write_text(">hit\nMPEPTIDE\n>none\nMNOHITX\n", encoding="utf-8")
    adapter = FixtureAdapter(
        executable=write_fixture_tool(tmp_path / "fixture-tool"),
        database=write_fixture_database(tmp_path / "database"),
    )
    events: list[ProgressEvent] = []

    with LocalStore.open(tmp_path / "store") as store:
        run_annotation(
            fasta_path=cached_fasta,
            output_dir=tmp_path / "cached.duckdb",
            adapter=adapter,
            store=store,
        )
        run_annotation(
            fasta_path=mixed_fasta,
            output_dir=tmp_path / "mixed.duckdb",
            adapter=adapter,
            store=store,
            progress_sink=events.append,
        )

    lookup = next(
        event
        for event in events
        if event.phase is ProgressPhase.STORE_LOOKUP
        and event.state is ProgressState.RUNNING
    )
    tool = next(
        event
        for event in events
        if event.phase is ProgressPhase.TOOL and event.state is ProgressState.STARTED
    )
    committed = next(
        event
        for event in events
        if event.phase is ProgressPhase.STORE_COMMIT
        and event.state is ProgressState.COMPLETED
    )
    assert lookup.evidence_ready == WorkProgress(1, 2, ProgressUnit.SEQUENCES)
    assert tool.evidence_ready == WorkProgress(1, 2, ProgressUnit.SEQUENCES)
    assert committed.evidence_ready == WorkProgress(2, 2, ProgressUnit.SEQUENCES)
