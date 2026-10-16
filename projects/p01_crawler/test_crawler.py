import asyncio
import contextlib
from collections import Counter

import aiohttp
import pytest
from aiohttp import web

from projects.p01_crawler.crawler import (
    CrawlConfig,
    HostLimiter,
    crawl,
    extract_links,
    extract_title,
    fetch,
)


def now():
    return asyncio.get_running_loop().time()


# --- Тестовый сайт ------------------------------------------------------------

class Site:
    """Сайт-дерево: /p/n ссылается на /p/2n+1 и /p/2n+2 (если n меньше size),
    а ещё — на корень (цикл), на себя с фрагментом и наружу.
    Считает обращения и одновременные запросы."""

    def __init__(self, size=31, delay=0.05):
        self.size = size
        self.delay = delay
        self.hits = Counter()
        self.in_flight = 0
        self.max_in_flight = 0
        self.flaky_left = {}
        self.base = ""

    @contextlib.contextmanager
    def track(self, path):
        self.hits[path] += 1
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            yield
        finally:
            self.in_flight -= 1

    async def page(self, request):
        n = int(request.match_info["n"])
        with self.track(request.path):
            await asyncio.sleep(self.delay)
            if n >= self.size:
                raise web.HTTPNotFound()
            kids = [k for k in (2 * n + 1, 2 * n + 2) if k < self.size]
            links = "".join(f'<a href="/p/{k}">{k}</a>' for k in kids)
            body = (
                f"<html><head><title> Page {n} </title></head><body>{links}"
                f'<a href="/p/0">home</a><a href="#top">top</a>'
                f'<a href="/p/{n}#x">self</a><a href="mailto:a@b.c">mail</a>'
                f'<a href="http://127.0.0.1:1/outside">ext</a></body></html>'
            )
            return web.Response(text=body, content_type="text/html")

    async def flaky(self, request):
        key = request.match_info["key"]
        with self.track(request.path):
            left = self.flaky_left.setdefault(key, int(request.query.get("fail", 2)))
            if left > 0:
                self.flaky_left[key] = left - 1
                return web.Response(status=503, text="busy")
            return web.Response(text="<title>ok</title>", content_type="text/html")

    async def slow(self, request):
        with self.track(request.path):
            await asyncio.sleep(10)
            return web.Response(text="late")

    async def mixed(self, request):
        with self.track(request.path):
            body = (
                '<a href="/p/1">ok</a><a href="/missing/1">404</a>'
                '<a href="/slow">slow</a><a href="/flaky/m?fail=5">500</a>'
                '<a href="http://127.0.0.1:1/dead">dead</a>'
            )
            return web.Response(text=body, content_type="text/html")

    async def wide(self, request):
        with self.track(request.path):
            links = "".join(f'<a href="/p/{1000 + i}">x</a>' for i in range(40))
            return web.Response(text=links, content_type="text/html")

    def url(self, path):
        return self.base + path


@contextlib.asynccontextmanager
async def serve(site):
    app = web.Application()
    app.router.add_get("/p/{n}", site.page)
    app.router.add_get("/flaky/{key}", site.flaky)
    app.router.add_get("/slow", site.slow)
    app.router.add_get("/mixed", site.mixed)
    app.router.add_get("/wide", site.wide)
    runner = web.AppRunner(app, shutdown_timeout=0.1)
    await runner.setup()
    tcp = web.TCPSite(runner, "127.0.0.1", 0)
    await tcp.start()
    port = tcp._server.sockets[0].getsockname()[1]
    site.base = f"http://127.0.0.1:{port}"
    try:
        yield site
    finally:
        await runner.cleanup()


# --- Часть 1 ------------------------------------------------------------------

def test_extract_links():
    html = """
    <a href="/a">1</a> <A HREF="b/c?x=1">2</A> <a href="https://other.org/z#frag">3</a>
    <a href="/a#again">dup</a> <a href="mailto:me@x.org">m</a> <a href="javascript:void(0)">j</a>
    <a>no href</a> <a href="../up">up</a> <link href="/style.css">
    """
    assert extract_links("http://site.ru/dir/page", html) == [
        "http://site.ru/a",
        "http://site.ru/dir/b/c?x=1",
        "https://other.org/z",
        "http://site.ru/up",
    ]
    assert extract_links("http://x/", "<p>нет ссылок</p>") == []


def test_extract_title():
    assert extract_title("<html><head><title>\n  Привет, мир \n</title></head></html>") == "Привет, мир"
    assert extract_title("<TITLE>a</TITLE><title>b</title>") == "a"
    assert extract_title("<p>без заголовка</p>") is None


# --- Часть 2 ------------------------------------------------------------------

async def test_host_limiter_per_host():
    limiter = HostLimiter(2)
    active = Counter()
    peak = Counter()

    async def job(host):
        async with limiter.slot(host):
            active[host] += 1
            peak[host] = max(peak[host], active[host])
            await asyncio.sleep(0.05)
            active[host] -= 1

    start = now()
    await asyncio.gather(*(job(h) for h in ["a"] * 6 + ["b"] * 6 + ["c"] * 2))
    elapsed = now() - start
    assert peak == {"a": 2, "b": 2, "c": 2}
    assert elapsed < 0.25, f"разные хосты должны работать параллельно ({elapsed:.2f} c)"
    assert limiter.active_hosts == 0, "семафоры свободных хостов нужно удалять"


async def test_host_limiter_cleanup_on_cancel():
    limiter = HostLimiter(1)
    entered = asyncio.Event()

    async def holder():
        async with limiter.slot("h"):
            entered.set()
            await asyncio.sleep(10)

    async def waiter():
        async with limiter.slot("h"):
            pass

    t1 = asyncio.create_task(holder())
    await entered.wait()
    t2 = asyncio.create_task(waiter())
    await asyncio.sleep(0.01)
    assert limiter.active_hosts == 1
    t2.cancel()
    t1.cancel()
    await asyncio.gather(t1, t2, return_exceptions=True)
    assert limiter.active_hosts == 0


# --- Часть 3 ------------------------------------------------------------------

async def test_fetch_ok_and_404_not_retried():
    async with serve(Site()) as site, aiohttp.ClientSession() as s:
        status, text = await fetch(s, site.url("/p/0"), CrawlConfig())
        assert status == 200 and "Page 0" in text
        status, _ = await fetch(s, site.url("/p/999"), CrawlConfig(retries=3))
        assert status == 404
        assert site.hits["/p/999"] == 1, "4xx не повторяются"


async def test_fetch_retries_5xx_with_backoff():
    async with serve(Site()) as site, aiohttp.ClientSession() as s:
        start = now()
        status, text = await fetch(s, site.url("/flaky/a?fail=2"), CrawlConfig(retries=2, retry_delay=0.1))
        elapsed = now() - start
        assert status == 200 and "ok" in text
        assert site.hits["/flaky/a"] == 3
        assert 0.3 <= elapsed < 0.6, f"паузы 0.1 + 0.2 c, прошло {elapsed:.2f}"

        status, _ = await fetch(s, site.url("/flaky/b?fail=10"), CrawlConfig(retries=1, retry_delay=0.01))
        assert status == 503, "кончились попытки — вернуть последний 5xx"
        assert site.hits["/flaky/b"] == 2


async def test_fetch_timeout_and_connection_error():
    async with serve(Site()) as site, aiohttp.ClientSession() as s:
        start = now()
        with pytest.raises(TimeoutError):
            await fetch(s, site.url("/slow"), CrawlConfig(timeout=0.1, retries=2, retry_delay=0.01))
        assert site.hits["/slow"] == 3
        assert now() - start < 1
        with pytest.raises(aiohttp.ClientConnectionError):
            await fetch(s, "http://127.0.0.1:1/", CrawlConfig(retries=1, retry_delay=0.01))


# --- Часть 4 ------------------------------------------------------------------

async def test_crawl_tree():
    async with serve(Site(size=31, delay=0.05)) as site:
        start = now()
        res = await crawl(site.url("/p/0"), CrawlConfig(max_depth=10, max_concurrency=20, per_host_limit=20))
        elapsed = now() - start
        assert set(res.pages) == {site.url(f"/p/{n}") for n in range(31)}
        assert res.errors == {}
        assert all(v == 1 for v in site.hits.values()), "каждый URL — ровно один раз"
        for n in range(31):
            page = res.pages[site.url(f"/p/{n}")]
            assert page.status == 200
            assert page.title == f"Page {n}"
            assert page.depth == (n + 1).bit_length() - 1
        assert site.url("/p/0") in res.pages[site.url("/p/5")].links
        assert "http://127.0.0.1:1/outside" in res.pages[site.url("/p/0")].links
        # 5 уровней по ~0.05 c: последовательный обход занял бы > 1.5 c
        assert elapsed < 0.8, f"уровни должны загружаться конкурентно ({elapsed:.2f} c)"


async def test_crawl_depth_limit():
    async with serve(Site(size=31, delay=0)) as site:
        res = await crawl(site.url("/p/0"), CrawlConfig(max_depth=2))
        assert set(res.pages) == {site.url(f"/p/{n}") for n in range(7)}
        assert max(p.depth for p in res.pages.values()) == 2


async def test_crawl_limits_concurrency():
    async with serve(Site(size=63, delay=0.05)) as site:
        await crawl(site.url("/p/0"), CrawlConfig(max_depth=10, max_concurrency=8, per_host_limit=3))
        assert site.max_in_flight == 3, f"на хост одновременно {site.max_in_flight}, а нужно не больше 3"
        site.hits.clear()
        site.max_in_flight = 0
        await crawl(site.url("/p/0"), CrawlConfig(max_depth=10, max_concurrency=4, per_host_limit=100))
        assert site.max_in_flight == 4


async def test_crawl_max_pages():
    async with serve(Site(delay=0)) as site:
        res = await crawl(site.url("/wide"), CrawlConfig(max_pages=10, max_depth=3))
        assert len(res.pages) + len(res.errors) == 10
        assert sum(site.hits.values()) == 10
        expected = {site.url("/wide")} | {site.url(f"/p/{1000 + i}") for i in range(9)}
        assert set(res.pages) == expected, "приоритет у ссылок, обнаруженных раньше"


async def test_crawl_errors_do_not_stop_crawl():
    async with serve(Site(delay=0)) as site:
        cfg = CrawlConfig(max_depth=1, timeout=0.2, retries=1, retry_delay=0.01, same_host_only=False)
        res = await crawl(site.url("/mixed"), cfg)
        assert res.pages[site.url("/p/1")].status == 200
        assert res.pages[site.url("/missing/1")].status == 404
        assert res.pages[site.url("/flaky/m?fail=5")].status == 503
        assert res.errors == {
            site.url("/slow"): "TimeoutError",
            "http://127.0.0.1:1/dead": "ClientConnectorError",
        }


async def test_crawl_same_host_only():
    async with serve(Site(size=3, delay=0)) as site:
        res = await crawl(site.url("/p/0"), CrawlConfig(max_depth=5, retries=0))
        assert "http://127.0.0.1:1/outside" not in res.pages
        assert res.errors == {}


async def test_crawl_uses_given_session():
    async with serve(Site(size=3, delay=0)) as site, aiohttp.ClientSession() as s:
        await crawl(site.url("/p/0"), CrawlConfig(), session=s)
        assert not s.closed, "чужую сессию закрывать нельзя"


async def test_crawl_cancel_leaves_no_tasks():
    async with serve(Site(size=1000, delay=0.2)) as site:
        before = asyncio.all_tasks()
        task = asyncio.create_task(crawl(site.url("/p/0"), CrawlConfig(max_depth=20, max_pages=1000)))
        await asyncio.sleep(0.5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0)
        leftover = {t for t in asyncio.all_tasks() - before if not t.done()}
        # задачи тестового сервера (aiohttp.web, приём соединений) не в счёт
        leftover = {
            t for t in leftover
            if not any(s in t.get_coro().cr_code.co_filename for s in ("aiohttp/web_", "/asyncio/"))
        }
        assert not leftover, f"остались задачи: {leftover}"
