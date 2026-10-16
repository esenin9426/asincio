"""Общая инфраструктура тестов курса.

pytest сам по себе не умеет запускать `async def test_...`. Этот хук запускает
такие тесты через `asyncio.run()` и ограничивает их время, чтобы зависшее
решение падало понятной ошибкой, а не вешало весь прогон.

Использование в тестах:

    async def test_something():           # лимит по умолчанию — 10 секунд
        ...

    @pytest.mark.timeout(2)
    async def test_fast():                # свой лимит
        ...
"""

import asyncio
import faulthandler
import inspect
import sys

import pytest

DEFAULT_TIMEOUT = 10.0


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "timeout(seconds): лимит времени для async-теста"
    )


@pytest.hookimpl(tryfirst=True)
def pytest_pyfunc_call(pyfuncitem):
    func = pyfuncitem.obj
    if not inspect.iscoroutinefunction(func):
        return None

    marker = pyfuncitem.get_closest_marker("timeout")
    timeout = marker.args[0] if marker else DEFAULT_TIMEOUT
    kwargs = {
        name: pyfuncitem.funcargs[name]
        for name in pyfuncitem._fixtureinfo.argnames
    }

    async def runner():
        cm = asyncio.timeout(timeout)
        try:
            async with cm:
                await func(**kwargs)
        except TimeoutError:
            if cm.expired():
                pytest.fail(
                    f"Тест не уложился в {timeout} с — решение зависло "
                    f"или работает последовательно, а не конкурентно",
                    pytrace=False,
                )
            raise

    # Сторож: если цикл завис так, что даже отмена не помогает,
    # процесс будет завершён с дампом стеков всех потоков.
    faulthandler.dump_traceback_later(timeout + 10, exit=True, file=sys.stderr)
    try:
        asyncio.run(runner())
    finally:
        faulthandler.cancel_dump_traceback_later()
    return True


# --- Базы данных (уроки 16–18, проекты) -------------------------------------
# Фикстуры отдают параметры подключения; если БД не запущена — тест
# пропускается с подсказкой. Сами подключения тесты открывают через
# песочницы из dbtest.py.

def _require(host: str, port: int, name: str) -> None:
    import socket

    try:
        with socket.create_connection((host, port), timeout=0.5):
            pass
    except OSError:
        pytest.skip(f"{name} недоступен на {host}:{port}. Запустите: docker compose up -d --wait")


@pytest.fixture
def pg_dsn() -> str:
    from urllib.parse import urlparse

    import dbtest

    u = urlparse(dbtest.PG_DSN)
    _require(u.hostname, u.port or 5432, "PostgreSQL")
    return dbtest.PG_DSN


@pytest.fixture
def redis_url() -> str:
    from urllib.parse import urlparse

    import dbtest

    u = urlparse(dbtest.REDIS_URL)
    _require(u.hostname, u.port or 6379, "Redis")
    return dbtest.REDIS_URL


@pytest.fixture
def ch_config() -> dict:
    import dbtest

    _require(dbtest.CH_CONFIG["host"], dbtest.CH_CONFIG["port"], "ClickHouse")
    return dict(dbtest.CH_CONFIG)
