"""Тесты NovelDownloader и разбора HTML."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from bs4 import BeautifulSoup, Tag

from novel_downloader import BASE_URL, BookMetadata, NovelDownloader


@pytest.fixture
def downloader(tmp_path) -> NovelDownloader:
    out = tmp_path / "book.epub"
    return NovelDownloader(
        novel_name="test-novel",
        output_file=str(out),
        start_chapter=1,
        max_chapters=1,
        request_delay=0,
    )


METADATA_HTML = """
<html><body>
<div class="m-info">
  <div class="m-desc"><h1 class="tit">Test Title</h1></div>
  <div class="txt">
    <div class="item"><span title="Author"></span><div class="right"><a>Test Author</a></div></div>
    <div class="item"><span title="Genre"></span><div class="right"><a>Fantasy</a></div></div>
    <div class="item"><span title="Status"></span><div class="right">Ongoing</div></div>
  </div>
  <div class="inner"><p>Desc line</p></div>
  <div class="m-book1"><div class="pic"><img src="/covers/x.jpg" /></div></div>
</div>
</body></html>
"""

CHAPTER_HTML = """
<html><body>
<div id="article">ok</div>
<div class="txt">
  <script>x</script>
  <ins>ad</ins>
  <div class="ad">promo</div>
  <p>Hello</p>
  <img src="/rel.png"/>
</div>
<span class="chapter">Chapter 1 — Start</span>
</body></html>
"""

PLACEHOLDER_HTML = """
<html><body>
<div id="article">Chapter content is missing or does not exist! Please try again later!</div>
<div class="txt">ignored</div>
</body></html>
"""


def test_fetch_metadata_parses_page(downloader: NovelDownloader) -> None:
    resp = MagicMock()
    resp.status_code = 200
    resp.text = METADATA_HTML

    with patch.object(downloader, "safe_request", return_value=resp):
        meta = downloader.fetch_metadata()

    assert meta is not None
    assert meta["title"] == "Test Title"
    assert meta["author"] == "Test Author"
    assert meta["genres"] == ["Fantasy"]
    assert meta["status"] == "Ongoing"
    assert "Desc line" in meta["description"]
    assert meta["cover_url"] == f"{BASE_URL}/covers/x.jpg"


def test_fetch_metadata_none_on_failed_request(downloader: NovelDownloader) -> None:
    with patch.object(downloader, "safe_request", return_value=None):
        assert downloader.fetch_metadata() is None


def test_get_attr_src(downloader: NovelDownloader) -> None:
    soup = BeautifulSoup('<img id="i" src="/pic.jpg" />', "html.parser")
    assert downloader._get_attr(soup, "#i", "src") == "/pic.jpg"


def test_process_chapter_content(downloader: NovelDownloader) -> None:
    soup = BeautifulSoup(CHAPTER_HTML, "html.parser")
    div = soup.find("div", class_="txt")
    assert isinstance(div, Tag)
    out = downloader._process_chapter_content(div)
    text = str(out)
    assert "<script" not in text.lower()
    assert "promo" not in text
    assert f"{BASE_URL}/rel.png" in text


def test_download_chapter_success(downloader: NovelDownloader) -> None:
    resp = MagicMock()
    resp.status_code = 200
    resp.text = CHAPTER_HTML

    with patch.object(downloader, "safe_request", return_value=resp):
        data = downloader.download_chapter(1)

    assert data is not None
    assert data["title"] == "Chapter 1 — Start"
    assert "Hello" in data["content"]
    assert data["file_name"] == "chapter_1.xhtml"


def test_download_chapter_placeholder(downloader: NovelDownloader) -> None:
    resp = MagicMock()
    resp.status_code = 200
    resp.text = PLACEHOLDER_HTML

    with patch.object(downloader, "safe_request", return_value=resp):
        assert downloader.download_chapter(1) is None


def test_download_chapter_no_response(downloader: NovelDownloader) -> None:
    with patch.object(downloader, "safe_request", return_value=None):
        assert downloader.download_chapter(1) is None


def test_generate_epub_chapter_escapes_title(downloader: NovelDownloader) -> None:
    ch = downloader.generate_epub_chapter(
        {
            "title": 'A < B',
            "content": "<p>ok</p>",
            "file_name": "c.xhtml",
        }
    )
    assert "A &lt; B" in (ch.content or "")
    assert "<p>ok</p>" in (ch.content or "")


def test_safe_request_returns_200(downloader: NovelDownloader) -> None:
    mock_resp = MagicMock()
    mock_resp.status_code = 200

    with patch.object(
        downloader.session,
        "request",
        return_value=mock_resp,
    ) as req:
        out = downloader.safe_request("https://example.com", max_retries=1)

    assert out is mock_resp
    req.assert_called_once()


def test_safe_request_404_returns_response(downloader: NovelDownloader) -> None:
    mock_resp = MagicMock()
    mock_resp.status_code = 404
    mock_resp.reason = "Not Found"

    with patch.object(downloader.session, "request", return_value=mock_resp):
        out = downloader.safe_request("https://example.com/x", max_retries=1)

    assert out is mock_resp


def test_safe_request_retries_then_success(
    downloader: NovelDownloader,
) -> None:
    bad = MagicMock()
    bad.status_code = 503
    good = MagicMock()
    good.status_code = 200

    with (
        patch.object(downloader.session, "request", side_effect=[bad, good]),
        patch("novel_downloader.time.sleep", return_value=None),
        patch("novel_downloader.random.random", return_value=0.0),
    ):
        out = downloader.safe_request(
            "https://example.com",
            max_retries=3,
            initial_delay=0.01,
        )

    assert out is good


def test_proxy_sets_session_proxies(tmp_path) -> None:
    out = tmp_path / "b.epub"
    d = NovelDownloader(
        novel_name="n",
        output_file=str(out),
        proxy="http://127.0.0.1:9",
        request_delay=0,
    )
    assert d.session.proxies.get("http") == "http://127.0.0.1:9"
    assert d.session.proxies.get("https") == "http://127.0.0.1:9"


def test_run_aborts_without_metadata(downloader: NovelDownloader) -> None:
    with patch.object(downloader, "fetch_metadata", return_value=None):
        downloader.run()
    # не падает


def test_run_one_chapter_writes_epub(downloader: NovelDownloader, tmp_path) -> None:
    meta: BookMetadata = {
        "title": "T",
        "author": "A",
        "genres": [],
        "status": "x",
        "description": "<p>d</p>",
        "cover_url": None,
    }
    ch = {
        "title": "Ch1",
        "content": "<p>body</p>",
        "file_name": "chapter_1.xhtml",
    }

    downloader.metadata = meta

    with (
        patch.object(downloader, "fetch_metadata", return_value=meta),
        patch.object(downloader, "download_chapter", return_value=ch),
        patch.object(downloader, "save_progress", return_value=True),
        patch.object(downloader, "finalize_epub", return_value=True),
        patch("novel_downloader.time.sleep", return_value=None),
    ):
        downloader.run()

    assert downloader.output_file == str(tmp_path / "book.epub")


def test_create_description_page_escapes_meta(downloader: NovelDownloader) -> None:
    downloader.metadata = {
        "title": "<x>",
        "author": "A",
        "genres": ["G"],
        "status": "S",
        "description": "<p>html</p>",
    }
    page = downloader._create_description_page()
    assert "&lt;x&gt;" in (page.content or "")
    assert "<p>html</p>" in (page.content or "")
