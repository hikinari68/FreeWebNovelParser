import argparse

from novel_downloader import (
    BOOKS_DIR,
    DEFAULT_DELAY_SEC,
    DEFAULT_MAX_CHAPTERS,
    DEFAULT_NOVEL,
    DEFAULT_OUTPUT,
    DEFAULT_PROXY,
    DEFAULT_START_CHAPTER,
    NovelDownloader,
)


def main():
    """Точка входа с обработкой аргументов командной строки."""
    parser = argparse.ArgumentParser(
        description="Скачивание веб-новеллы в формате EPUB",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-n", "--novel", default=DEFAULT_NOVEL, help="Название новеллы (часть URL)"
    )
    parser.add_argument(
        "-s", "--start", type=int, default=DEFAULT_START_CHAPTER, help="Стартовая глава"
    )
    parser.add_argument(
        "-m", "--max", type=int, default=DEFAULT_MAX_CHAPTERS, help="Макс. глав (0=все)"
    )
    parser.add_argument(
        "-o",
        "--output",
        default=DEFAULT_OUTPUT,
        help=(
            f"Имя EPUB-файла; без пути файл создаётся в каталоге {BOOKS_DIR}/ "
            f"(по умолчанию как {BOOKS_DIR}/<имя новеллы>.epub)"
        ),
    )
    parser.add_argument(
        "-d",
        "--delay",
        type=float,
        default=DEFAULT_DELAY_SEC,
        help="Задержка между запросами (сек)",
    )
    parser.add_argument(
        "-p",
        "--proxy",
        default=DEFAULT_PROXY,
        metavar="URL",
        help=(
            "Прокси для всех HTTP(S)-запросов "
            "(http://, https://, socks5://, socks5h://, socks4:// …). "
            "Без схемы подставляется http://"
        ),
    )

    args = parser.parse_args()

    downloader = NovelDownloader(
        novel_name=args.novel,
        start_chapter=args.start,
        max_chapters=args.max,
        output_file=args.output,
        request_delay=args.delay,
        proxy=args.proxy,
    )
    downloader.run()


if __name__ == "__main__":
    main()
