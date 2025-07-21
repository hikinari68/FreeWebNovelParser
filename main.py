import argparse
import logging
import mimetypes
import os
import random
import signal
import time
from typing import Dict, List, Optional, Tuple, Union
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag
from ebooklib import epub

# Константы
BASE_URL = "https://freewebnovel.com"
SAVE_INTERVAL = 5
DEFAULT_NOVEL = "shadow-slave"
DEFAULT_START_CHAPTER = 1
DEFAULT_MAX_CHAPTERS = 0
DEFAULT_DELAY_SEC = 1
DEFAULT_OUTPUT = "novel.epub"

HEADERS = {
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    'accept-language': 'en-US,en;q=0.9,ru;q=0.8',
    'cache-control': 'max-age=0',
    'priority': 'u=0, i',
    'sec-cha-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-cha-ua-platform': '"macOS"',
    'sec-cha-ua-mobile': '?0',
    'sec-fetch-user': '?1',
    'upgrade-Insecure-Requests': '1',
    'dnt': '1'
}

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


class NovelDownloader:
    """Скачивает новеллу с webnovel.com и конвертирует в EPUB."""

    def __init__(
            self,
            novel_name: str = DEFAULT_NOVEL,
            start_chapter: int = DEFAULT_START_CHAPTER,
            max_chapters: int = DEFAULT_MAX_CHAPTERS,
            output_file: str = DEFAULT_OUTPUT,
            request_delay: float = DEFAULT_DELAY_SEC
    ):
        if output_file == DEFAULT_OUTPUT:
            self.output_file = f"{novel_name}.epub"
        else:
            self.output_file = output_file
        self.temp_file = f"{self.output_file}.tmp"

        self.novel_name = novel_name
        self.start_chapter = start_chapter
        self.max_chapters = max_chapters
        self.request_delay = request_delay
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.metadata = {}
        self.should_stop = False
        self._register_signal_handlers()

    def _register_signal_handlers(self):
        """Регистрирует обработчики сигналов для корректного завершения."""
        signal.signal(signal.SIGINT, self._handle_exit_signal)
        signal.signal(signal.SIGTERM, self._handle_exit_signal)

    def _handle_exit_signal(self, signum, frame):
        """Обрабатывает сигналы завершения программы."""
        logger.info(f"\nSignal {signum} received, work completed...")
        self.should_stop = True

    def safe_request(
            self,
            url: str,
            method: str = "GET",
            headers: Optional[dict] = None,
            params: Optional[dict] = None,
            max_retries: int = 5,
            initial_delay: float = 3.0,
            backoff_factor: float = 2.0,
            timeout: float = 30.0
    ) -> Optional[requests.Response]:
        """
        Выполняет HTTP-запрос с повторными попытками при сбоях.
        
        Параметры:
        - url: URL для запроса
        - method: HTTP метод (GET/POST)
        - headers: дополнительные заголовки
        - params: параметры запроса
        - max_retries: максимальное количество попыток
        - initial_delay: начальная задержка (сек)
        - backoff_factor: множитель экспоненциальной задержки
        - timeout: таймаут запроса
        
        Возвращает: Response объект или None при ошибке
        """
        attempt = 0
        current_delay = initial_delay

        # Используем заголовки сессии по умолчанию
        request_headers = self.session.headers.copy()
        if headers:
            request_headers.update(headers)

        while attempt <= max_retries:
            attempt += 1
            try:
                response = self.session.request(
                    method,
                    url,
                    headers=request_headers,
                    params=params,
                    timeout=timeout
                )

                # Проверяем статус код
                if response.status_code == 200:
                    return response

                # Обработка специфичных ошибок
                if response.status_code == 404:
                    logger.info(f"Resource({url}) not found: {response.reason}")
                    return response

                if response.status_code == 403:
                    logger.info(f"Access forbidden: {url}")

                if response.status_code in (429, 503):
                    logger.info(f"Too many requests (status code {response.status_code})")

                if 400 <= response.status_code < 500:
                    logger.info(f"Client error ({response.status_code}): {url}")

                if 500 <= response.status_code < 600:
                    logger.info(f"Server error ({response.status_code}): {url}")

            except requests.exceptions.RequestException as e:
                error = f"{type(e).__name__} {e}"

                if isinstance(e, requests.exceptions.Timeout):
                    logger.info(f"Request timeout ({error}): {url}")
                elif isinstance(e, requests.exceptions.ConnectionError):
                    logger.info(f"Network error ({error}): {url}")
                else:
                    logger.info(f"Request error ({error}): {url}")

            if attempt < max_retries:
                jitter = 0.1 * current_delay * random.random()
                sleep_time = current_delay + jitter

                logger.info(f"Retry in {sleep_time:.1f} sec (attempt {attempt}/{max_retries})")
                time.sleep(sleep_time)

                current_delay *= backoff_factor
            else:
                logger.info(f"Maximum number of attempts exceeded ({max_retries}) for {url}")

        return None

    def fetch_metadata(self) -> Optional[Dict[str, Union[str, List[str]]]]:
        """Получает метаданные книги со страницы обзора."""
        url = f"{BASE_URL}/novel/{self.novel_name}"
        try:
            response = self.safe_request(url, timeout=15)
            if response is None:
                return None

            soup = BeautifulSoup(response.text, 'html.parser')

            SELECTORS = {
                'title': '.m-info .m-desc h1.tit',
                'author': '.m-info .txt .item:has(span[title="Author"]) .right a',
                'genres': '.m-info .txt .item:has(span[title="Genre"]) .right a',
                'status': '.m-info .txt .item:has(span[title="Status"]) .right',
                'description': '.m-info .inner p',
                'cover': '.m-info .m-book1 .pic img'
            }

            self.metadata = {
                'title': self._get_text(soup, SELECTORS['title']),
                'author': self._get_text(soup, SELECTORS['author']) or "Unknown Author",
                'genres': [a.text.strip() for a in soup.select(SELECTORS['genres'])],
                'status': self._get_text(soup, SELECTORS['status']),
                'description': ''.join(str(p) for p in soup.select(SELECTORS['description'])),
                'cover_url': self._get_attr(soup, SELECTORS['cover'], 'src')
            }

            if self.metadata['cover_url']:
                self.metadata['cover_url'] = urljoin(BASE_URL, self.metadata['cover_url'])

            return self.metadata

        except (requests.RequestException, ValueError) as e:
            logger.info(f"[ERROR] Metadata fetch failed: {type(e).__name__} - {str(e)}")
            return None

    def _get_text(self, soup: BeautifulSoup, selector: str) -> Optional[str]:
        """Извлекает текст из элемента по CSS-селектору."""
        element = soup.select_one(selector)
        return element.text.strip() if element else None

    def _get_attr(self, soup: BeautifulSoup, selector: str, attr: str) -> Optional[str]:
        """Извлекает атрибут из элемента по CSS-селектору."""
        element = soup.select_one(selector)
        return element.get(attr) if element else None

    def _create_epub(self) -> epub.EpubBook:
        """Создает базовую структуру EPUB книги."""
        if not self.metadata:
            self.fetch_metadata()

        book = epub.EpubBook()
        book.set_identifier(self.novel_name)
        book.set_title(self.metadata.get('title', self.novel_name.replace('-', ' ').title()))
        book.set_language('en')
        book.add_author(self.metadata.get('author', 'Unknown'))

        # Добавляем описание
        if desc := self.metadata.get('description'):
            book.add_metadata('DC', 'description', desc)

        # Добавляем жанры
        for genre in self.metadata.get('genres', []):
            book.add_metadata('DC', 'subject', genre)

        # Загрузка обложки
        if cover_url := self.metadata.get('cover_url'):
            self._add_cover(book, cover_url)

        return book

    def _add_cover(self, book: epub.EpubBook, cover_url: str) -> None:
        """Добавляет обложку в EPUB."""
        try:
            response = self.safe_request(cover_url, timeout=10)
            if response is None:
                return

            content_type = response.headers.get('Content-Type', '')
            if not content_type.startswith('image/'):
                logger.info(f"[WARN] Invalid cover content type: {content_type}")
                return

            ext = mimetypes.guess_extension(content_type) or '.jpg'
            book.set_cover(f"cover{ext}", response.content)
        except Exception as e:
            logger.info(f"[ERROR] Cover download failed: {type(e).__name__} - {str(e)}")

    def _create_description_page(self) -> epub.EpubHtml:
        """Генерирует HTML-страницу с описанием книги."""
        html = epub.EpubHtml(
            title='Description',
            file_name='description.xhtml',
            lang='en'
        )

        title = self.metadata.get('title', '')
        author = self.metadata.get('author', 'Unknown')
        status = self.metadata.get('status', 'Unknown')
        genres = ', '.join(self.metadata.get('genres', []))
        description = self.metadata.get('description', 'No description available')

        html.content = f"""<html xmlns="http://www.w3.org/1999/xhtml">
        <head><title>Description</title></head>
        <body>
            <h1>{title}</h1>
            <div class="meta">
                <div><strong>Author:</strong> {author}</div>
                <div><strong>Status:</strong> {status}</div>
                <div><strong>Genres:</strong> {genres}</div>
            </div>
            <h2>Summary</h2>
            <div class="description">{description}</div>
        </body>
        </html>"""
        return html

    def _process_chapter_content(self, content: Tag) -> Tag:
        """Очищает и преобразует контент главы."""
        # Удаление рекламных элементов
        for element in content.find_all(['script', 'ins', 'div.ad']):
            element.decompose()

        # Фикс относительных URL изображений
        for img in content.find_all('img', src=True):
            img['src'] = urljoin(BASE_URL, img['src'])

        return content

    def download_chapter(self, chapter_num: int) -> Optional[Dict[str, str]]:
        """Загружает и обрабатывает одну главу."""
        url = f"{BASE_URL}/novel/{self.novel_name}/chapter-{chapter_num}"

        try:
            # Устанавливаем Referer для последовательности глав
            headers = {'Referer': f"{BASE_URL}/novel/{self.novel_name}/chapter-{chapter_num-1}"} if chapter_num > 1 else {}

            response = self.safe_request(url, headers=headers, timeout=30, initial_delay=10, max_retries=10)
            if response is None:
                return None

            soup = BeautifulSoup(response.text, 'html.parser')
            content_div = soup.find('div', class_='txt')

            if (not content_div
                or soup.find('div', id='article').text ==
                "Chapter content is missing or does not exist! Please try again later!"):
                logger.info(f"[WARN] No content found in chapter {chapter_num}")
                return None

            content = self._process_chapter_content(content_div)

            title_tag = soup.find('span', class_='chapter')
            if title_tag:
                title = title_tag.text.strip()
                title_tag.decompose()
            else:
                title = f"Chapter {chapter_num}"

            return {
                'title': title,
                'content': str(content),
                'file_name': f'chapter_{chapter_num}.xhtml'
            }

        except requests.RequestException as e:
            logger.info(f"[ERROR] Chapter {chapter_num} download failed: {type(e).__name__} - {str(e)}")
            return None

    def generate_epub_chapter(self, chapter_data: Dict[str, str]) -> epub.EpubHtml:
        """Создает объект главы EPUB из данных."""
        chapter = epub.EpubHtml(
            title=chapter_data['title'],
            file_name=chapter_data['file_name'],
            lang='en'
        )
        chapter.content = f"""
                <html>
                    <head><title>{chapter_data['title']}</title></head>
                    <body>
                        <h1>{chapter_data['title']}</h1>
                        <div class="content">{chapter_data['content']}</div>
                    </body>
                </html>
                """
        return chapter

    def save_progress(self, book: epub.EpubBook) -> bool:
        """Сохраняет текущий прогресс во временный файл."""
        try:
            epub.write_epub(self.temp_file, book, {})
            return True
        except Exception as e:
            logger.info(f"[ERROR] Progress saving error: {type(e).__name__} - {str(e)}")
            return False

    def finalize_epub(self, book: epub.EpubBook) -> bool:
        """Финализирует EPUB и переименовывает временный файл."""
        try:
            # Добавляем навигацию
            book.add_item(epub.EpubNcx())
            book.add_item(epub.EpubNav())

            epub.write_epub(self.temp_file, book, {})

            # Заменяем временный файл постоянным
            if os.path.exists(self.output_file):
                os.remove(self.output_file)
            os.rename(self.temp_file, self.output_file)

            return True
        except Exception as e:
            logger.info(f"[ERROR] EPUB finalizing error: {type(e).__name__} - {str(e)}")
            return False

    def run(self) -> None:
        """Основной рабочий процесс."""
        logger.info(f"Starting download: {self.novel_name}")
        start_time = time.time()

        # Получение метаданных
        if not self.fetch_metadata():
            logger.info("Aborting: Failed to fetch metadata")
            return

        # Создание EPUB
        book = self._create_epub()
        book.toc = []
        book.spine = ['nav']

        # Добавление описания
        desc_page = self._create_description_page()
        book.add_item(desc_page)
        book.toc.append(epub.Link(desc_page.file_name, 'Description', 'desc'))
        book.spine.append(desc_page)

        # Загрузка глав
        chapter_count = 0
        current_chapter = self.start_chapter

        try:
            while not self.should_stop and (self.max_chapters == 0 or chapter_count < self.max_chapters):
                chapter_data = self.download_chapter(current_chapter)
                if not chapter_data:
                    logger.info(f"Stopping at chapter {current_chapter}")
                    break

                epub_chapter = self.generate_epub_chapter(chapter_data)
                book.add_item(epub_chapter)
                book.toc.append(epub_chapter)
                book.spine.append(epub_chapter)

                chapter_count += 1
                current_chapter += 1
                time.sleep(self.request_delay)

                # Промежуточное сохранение
                if chapter_count % SAVE_INTERVAL == 0:
                    self.save_progress(book)
                    logger.info(f"Downloaded {chapter_count} chapters...")
        finally:
            self.save_progress(book)
            # Финализация EPUB
            if self.finalize_epub(book):
                elapsed = time.time() - start_time
                logger.info(f"Successful saved: {os.path.abspath(self.output_file)}")
                logger.info(f"Chapters uploaded: {chapter_count} | Time: {elapsed:.2f}с")

                # Удаляем временный файл при успехе
                if os.path.exists(self.temp_file):
                    os.remove(self.temp_file)
            else:
                logger.info("A temporary file with progress has been saved.:")
                logger.info(f"  {os.path.abspath(self.temp_file)}")


def main():
    """Точка входа с обработкой аргументов командной строки."""
    parser = argparse.ArgumentParser(
        description='Скачивание веб-новеллы в формате EPUB',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('-n', '--novel', default=DEFAULT_NOVEL,
                        help='Название новеллы (часть URL)')
    parser.add_argument('-s', '--start', type=int, default=DEFAULT_START_CHAPTER,
                        help='Стартовая глава')
    parser.add_argument('-m', '--max', type=int, default=DEFAULT_MAX_CHAPTERS,
                        help='Макс. глав (0=все)')
    parser.add_argument('-o', '--output', default=DEFAULT_OUTPUT,
                        help='Выходной EPUB-файл')
    parser.add_argument('-d', '--delay', type=float, default=DEFAULT_DELAY_SEC,
                        help='Задержка между запросами (сек)')

    args = parser.parse_args()

    downloader = NovelDownloader(
        novel_name=args.novel,
        start_chapter=args.start,
        max_chapters=args.max,
        output_file=args.output,
        request_delay=args.delay
    )
    downloader.run()


if __name__ == "__main__":
    main()
