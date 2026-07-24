from __future__ import annotations

import signal
import sys
from pathlib import Path

import pytest

from seqevi.runner import ToolCommand, ToolRunner, ToolTimeoutError


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
    signals: list[tuple[int, signal.Signals]] = []

    class FakeProcess:
        pid = 12345
        returncode: int | None = None
        wait_count = 0

        def wait(self, timeout: float | None = None) -> int:
            del timeout
            self.wait_count += 1
            if self.wait_count == 1:
                raise KeyboardInterrupt
            self.returncode = -signal.SIGTERM
            return self.returncode

        def poll(self) -> int | None:
            return self.returncode

    fake_process = FakeProcess()
    monkeypatch.setattr(
        "seqevi.runner.subprocess.Popen", lambda *_args, **_kwargs: fake_process
    )
    monkeypatch.setattr(
        "seqevi.runner.os.killpg",
        lambda pid, sent_signal: signals.append((pid, sent_signal)),
    )

    with pytest.raises(KeyboardInterrupt):
        ToolRunner().run(command(tmp_path, "pass"))

    assert signals == [(fake_process.pid, signal.SIGTERM)]
