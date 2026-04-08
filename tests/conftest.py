"""Общие фикстуры."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _no_signal_handlers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Не регистрируем обработчики SIGINT/SIGTERM в тестах."""
    monkeypatch.setattr("novel_downloader.signal.signal", lambda *a, **k: None)
