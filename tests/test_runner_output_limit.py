from __future__ import annotations

import sys
from pathlib import Path

import pytest

from seqevi.runner import ToolCommand, ToolOutputLimitError, ToolRunner


@pytest.mark.parametrize("stream", ["stdout", "stderr"])
def test_output_limit_reaps_fast_writer_after_exit(tmp_path: Path, stream: str) -> None:
    command = ToolCommand(
        arguments=(
            sys.executable,
            "-c",
            f"import sys; getattr(sys, {stream!r}).write('x' * 200000)",
        ),
        working_dir=tmp_path,
        stdout_path=tmp_path / "stdout",
        stderr_path=tmp_path / "stderr",
    )
    with pytest.raises(ToolOutputLimitError) as raised:
        ToolRunner(termination_grace_seconds=0.01).run(command, output_limit_bytes=64)
    assert raised.value.result.return_code == 0
