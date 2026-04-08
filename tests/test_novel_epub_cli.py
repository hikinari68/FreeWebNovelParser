"""Smoke-тест CLI novel_epub."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_novel_epub_help() -> None:
    root = Path(__file__).resolve().parents[1]
    script = root / "novel_epub.py"
    proc = subprocess.run(
        [sys.executable, str(script), "--help"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0
    assert "--proxy" in proc.stdout or "--proxy" in proc.stderr
    assert "novel_epub" in proc.stdout or "usage" in proc.stdout.lower()
