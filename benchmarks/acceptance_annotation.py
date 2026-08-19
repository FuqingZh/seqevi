"""Run the SeqEvi CLI with benchmark-watchdog cancellation propagation."""

from __future__ import annotations

from benchmarks.acceptance_containment import install_watchdog_signal_bridge
from seqevi.cli import main as seqevi_main


def main() -> None:
    """Bridge watchdog TERM into annotate cleanup, then run the ordinary CLI."""

    install_watchdog_signal_bridge()
    seqevi_main()


if __name__ == "__main__":
    main()
