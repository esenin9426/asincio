"""Проект 1. Асинхронный веб-краулер.
Проверка: uv run pytest projects/p01_crawler -v

Подробное ТЗ — в README.md рядом. Реализуйте по порядку: каждая часть
опирается на предыдущие.
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import aiohttp


@dataclass
class CrawlConfig:
    max_concurrency: int = 10   # всего одновременных запросов
    per_host_limit: int = 2     # одновременных запросов к одному хосту (host:port)
    max_pages: int = 100        # сколько URL всего можно запросить
    max_depth: int = 2          # стартовая страница — глубина 0
    timeout: float = 5.0        # на одну попытку запроса, секунды
    retries: int = 2            # дополнительные попытки при 5xx/ошибке/таймауте
    retry_delay: float = 0.05   # пауза перед повтором, удваивается с каждой попыткой
    same_host_only: bool = True # ходить только по хосту стартовой страницы


@dataclass
class Page:
    url: str
    status: int
    depth: int
    title: str | None = None
    links: list[str] = field(default_factory=list)


@dataclass
class CrawlResult:
    pages: dict[str, Page] = field(default_factory=dict)  # url -> Page (любой HTTP-статус)
    errors: dict[str, str] = field(default_factory=dict)  # url -> имя класса исключения


# --- Часть 1. Разбор HTML (синхронный код) ------------------------------------

def extract_links(base_url: str, html: str) -> list[str]:
    """Ссылки из <a href="..."> в порядке появления.

    - относительные ссылки разрешаются относительно base_url (urljoin);
    - фрагмент (#...) отбрасывается;
    - остаются только схемы http и https (mailto:, javascript: и т.п. — нет);
    - без повторов (первое вхождение сохраняет порядок).
    Используйте html.parser.HTMLParser, а не регулярные выражения.
    """
    raise NotImplementedError


def extract_title(html: str) -> str | None:
    """Текст первого <title> без пробелов по краям; None, если тега нет."""
    raise NotImplementedError


# --- Часть 2. Ограничение нагрузки на хост ------------------------------------

class HostLimiter:
    """Не больше `limit` одновременных операций на один хост.

        limiter = HostLimiter(2)
        async with limiter.slot("example.com:80"):
            ...

    Для каждого хоста — свой семафор, создаётся при первом обращении.
    Разные хосты друг другу не мешают. Семафор хоста, у которого больше
    нет ни владельцев, ни ожидающих, удаляется (`active_hosts` — сколько
    семафоров сейчас хранится), чтобы словарь не рос бесконечно.
    """

    def __init__(self, limit: int) -> None:
        raise NotImplementedError

    @property
    def active_hosts(self) -> int:
        raise NotImplementedError

    @asynccontextmanager
    async def slot(self, host: str) -> AsyncIterator[None]:
        raise NotImplementedError
        yield


# --- Часть 3. Загрузка с таймаутом и повторами --------------------------------

async def fetch(session: aiohttp.ClientSession, url: str, config: CrawlConfig) -> tuple[int, str]:
    """GET url → (HTTP-статус, тело как текст).

    - Каждая попытка ограничена config.timeout секунд.
    - Повторять (до config.retries раз) при статусе >= 500, при
      aiohttp.ClientError и при TimeoutError. Перед k-м повтором
      (k = 1, 2, …) пауза config.retry_delay * 2 ** (k - 1).
    - Статусы < 500 (в т.ч. 404) не повторяются — сразу возвращаются.
    - Если попытки кончились: последний ответ был 5xx — вернуть его;
      последней была ошибка — пробросить её.
    """
    raise NotImplementedError


# --- Часть 4. Обход -----------------------------------------------------------

async def crawl(
    start_url: str, config: CrawlConfig, session: aiohttp.ClientSession | None = None
) -> CrawlResult:
    """Обход в ширину начиная со start_url.

    - Обход идёт по уровням: страницы глубины d загружаются конкурентно,
      и только когда уровень закончен, начинается уровень d + 1. Так глубина
      каждой страницы — минимальное число переходов от старта.
    - Каждый URL запрашивается не больше одного раза.
    - Одновременно — не больше config.max_concurrency запросов всего и
      config.per_host_limit на хост (HostLimiter; хост = netloc URL).
    - Всего запрашивается не больше config.max_pages URL (успешных и
      неуспешных вместе); при отборе приоритет у тех, кто раньше обнаружен.
    - Ссылки извлекаются только из ответов со статусом 200 и переходы по ним
      делаются, только если глубина страницы < config.max_depth.
    - same_host_only: ссылки на другие хосты не посещаются (но остаются в
      Page.links).
    - Страница с любым HTTP-статусом попадает в pages, исключение после
      всех повторов — в errors (имя класса: "TimeoutError",
      "ClientConnectorError", ...). Ошибка одной страницы не прерывает обход.
    - Если session не передана — создайте свою и закройте её в конце.
    - Отмена crawl() должна отменить все запущенные загрузки: после неё
      не остаётся висящих задач.
    """
    raise NotImplementedError
