"""Run the SeqEvi CLI with benchmark-watchdog cancellation propagation."""

from __future__ import annotations

import runpy
from collections.abc import Callable
from pathlib import Path
from typing import cast

from seqevi.cli import main as seqevi_main


def _install_watchdog_signal_bridge() -> None:
    """Load the sibling helper without adding the repository root to sys.path."""

    helper = runpy.run_path(str(Path(__file__).with_name("acceptance_containment.py")))
    install = cast(Callable[[], None], helper["install_watchdog_signal_bridge"])
    install()


def main() -> None:
    """Bridge watchdog TERM into annotate cleanup, then run the ordinary CLI."""

    _install_watchdog_signal_bridge()
    seqevi_main()


if __name__ == "__main__":
    main()
