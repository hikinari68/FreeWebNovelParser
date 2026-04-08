"""Тесты вспомогательных функций модуля novel_downloader."""

from __future__ import annotations

import os

import pytest

from novel_downloader import (
    BOOKS_DIR,
    DEFAULT_OUTPUT,
    _normalize_proxy_url,
    _proxies_dict,
    _resolve_output_path,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("127.0.0.1:8080", "http://127.0.0.1:8080"),
        ("http://proxy:3128", "http://proxy:3128"),
        ("socks5h://192.168.1.1:1080", "socks5h://192.168.1.1:1080"),
        ("HTTPS://x:1", "HTTPS://x:1"),
    ],
)
def test_normalize_proxy_url_ok(raw: str, expected: str) -> None:
    assert _normalize_proxy_url(raw) == expected


def test_normalize_proxy_url_empty() -> None:
    with pytest.raises(ValueError, match="Пустой"):
        _normalize_proxy_url("")
    with pytest.raises(ValueError, match="Пустой"):
        _normalize_proxy_url("   ")


def test_normalize_proxy_url_bad_scheme() -> None:
    with pytest.raises(ValueError, match="Неподдерживаемая схема"):
        _normalize_proxy_url("ftp://127.0.0.1:21")


def test_proxies_dict() -> None:
    assert _proxies_dict("http://127.0.0.1:8080") == {
        "http": "http://127.0.0.1:8080",
        "https": "http://127.0.0.1:8080",
    }


def test_resolve_output_path_default_sentinel() -> None:
    path = _resolve_output_path("my-novel", DEFAULT_OUTPUT)
    assert path == os.path.join(BOOKS_DIR, "my-novel.epub")


def test_resolve_output_path_basename_only() -> None:
    path = _resolve_output_path("ignored", "custom.epub")
    assert path == os.path.join(BOOKS_DIR, "custom.epub")


def test_resolve_output_path_with_parent(tmp_path_factory) -> None:
    base = tmp_path_factory.mktemp("out")
    target = base / "nested" / "book.epub"
    path = _resolve_output_path("n", str(target))
    assert path == str(target)
