# Урок 04. Отмена (cancellation)

Отмена — самая тонкая часть asyncio. Правильно написанный async-код должен
корректно реагировать на отмену в **любой** точке `await`.

## Как работает отмена

1. `task.cancel(msg=None)` **просит** отменить задачу и возвращает `True`,
   если задача ещё не завершена.
2. На ближайшем `await` внутри задачи будет брошено
   `asyncio.CancelledError` (наследник `BaseException`, а не `Exception`!).
3. Задача может перехватить его, чтобы прибраться (`finally`, `except`),
   и **должна пробросить дальше**. Тогда `task.cancelled() == True`.
4. Если задача «проглотила» `CancelledError` и вернула значение — отмена не
   состоялась, `task.cancelled() == False`. Так делать почти никогда нельзя.

```python
async def worker():
    try:
        while True:
            await do_work()
    except asyncio.CancelledError:
        await save_state()   # можно ждать в обработчике
        raise                # обязательно пробросить!
    finally:
        release_resources()
```

> Из-за того что `CancelledError` — `BaseException`, `except Exception:`
> его не ловит. Голый `except:` или `except BaseException:` — ловит,
> и это баг, если вы не пробрасываете исключение дальше.

## Счётчик отмен: `cancelling()` и `uncancel()` (3.11+)

У задачи есть счётчик запросов отмены:

- `task.cancelling()` — сколько отмен запрошено и ещё не «снято».
- `task.uncancel()` — уменьшить счётчик. Используют `asyncio.timeout` и
  `TaskGroup`: они сами отменяют задачу, ловят `CancelledError` и
  превращают его в `TimeoutError`, снимая *свою* отмену. Если параллельно
  пришла внешняя отмена — счётчик останется > 0, и они её пробросят.

Отсюда правило: если вы ловите `CancelledError` от **чужой** задачи
(например, после `task.cancel(); await task`), проверьте, не отменяют ли
**вас самих**:

```python
task.cancel()
try:
    await task
except asyncio.CancelledError:
    if asyncio.current_task().cancelling():
        raise          # отменяют нас — не глотаем
```

## `asyncio.shield(aw)`

```python
await asyncio.shield(save_to_db())
```

При отмене внешней задачи `await shield(...)` бросит `CancelledError`, но
`save_to_db()` (обёрнутая в задачу) продолжит выполняться. Держите ссылку на
внутреннюю задачу, если хотите потом её дождаться.

## Сообщение отмены

`task.cancel("shutdown")` → `CancelledError("shutdown")`; сообщение видно
в `exc.args` внутри задачи и при `await task`.

## Корректное завершение (graceful shutdown)

Типичный шаблон:
1. Перестать принимать новую работу.
2. Дать текущим задачам время закончить (`asyncio.wait(tasks, timeout=...)`).
3. Отменить оставшиеся и **дождаться** их (`gather(..., return_exceptions=True)`).

`asyncio.run` при выходе делает шаг 3 для всех незавершённых задач.

## Задачи

Проверка: `uv run pytest lessons/l04_cancellation -v`

1. `cancellable_worker` — уборка при отмене.
2. `cancel_and_wait` — отменить чужую задачу, не проглотив свою отмену.
3. `protected_save` — `shield`.
4. `cancel_reason` — сообщение отмены.
5. `graceful_shutdown` — дать время, затем отменить и дождаться.
6. `run_ignoring_cancel_for` — «критическая секция», которая доделывает
   работу и только потом выполняет отложенную отмену.

## Вопросы для самопроверки

1. Почему `CancelledError` наследуется от `BaseException`?
2. Что произойдёт, если задача поймала `CancelledError` и вернула `None`?
3. Зачем `asyncio.timeout` вызывает `uncancel()`?
4. Что видит внешний код при отмене задачи, ждущей `shield(...)`?
5. Почему после `task.cancel()` обязательно нужен `await task`?

Документация: https://docs.python.org/3/library/asyncio-task.html#task-cancellation
