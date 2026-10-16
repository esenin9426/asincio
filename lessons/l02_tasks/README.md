# Урок 02. Задачи (Task)

## Корутина vs задача

`await coro()` выполняет корутину «внутри» текущей — последовательно.
**Задача** (`asyncio.Task`) — корутина, отданная циклу на самостоятельное
выполнение. Она стартует при ближайшей возможности (на следующей итерации
цикла), даже если её никто не `await`-ит.

```python
async def main():
    t1 = asyncio.create_task(fetch("a"))   # запланирована
    t2 = asyncio.create_task(fetch("b"))   # запланирована
    # обе уже «бегут» конкурентно
    a = await t1
    b = await t2                           # итого ≈ max(t_a, t_b)
```

`Task` — подкласс `Future`: у него есть результат/исключение, состояние и
колбэки завершения.

## API задачи

| Метод / функция | Что делает |
|---|---|
| `asyncio.create_task(coro, *, name=None, context=None)` | создать задачу в текущем цикле |
| `loop.create_task(coro)` | то же через объект цикла |
| `task.done()` | завершена ли (успех, ошибка или отмена) |
| `task.result()` | результат; если ошибка — бросит её; если не готово — `InvalidStateError` |
| `task.exception()` | исключение или `None` (на отменённой — `CancelledError`) |
| `task.cancelled()` | была ли отменена |
| `task.cancel(msg=None)` | запросить отмену (урок 04) |
| `task.add_done_callback(fn)` / `remove_done_callback` | колбэк `fn(task)` после завершения |
| `task.get_name()` / `set_name()` | имя задачи (видно в логах и отладке) |
| `task.get_coro()` / `get_context()` | исходная корутина / контекст (урок 14) |
| `asyncio.current_task()` | задача, внутри которой выполняется код |
| `asyncio.all_tasks()` | все незавершённые задачи цикла |
| `task.get_stack()` / `print_stack()` | стек приостановленной задачи |

## Сильные ссылки на задачи — важно!

Цикл хранит на задачи **только слабые ссылки**. Задача, на которую никто
не ссылается, может быть собрана сборщиком мусора посреди выполнения.

```python
# ПЛОХО: задача может исчезнуть
asyncio.create_task(background_job())

# ХОРОШО: держим ссылку, пока задача работает
background = set()
task = asyncio.create_task(background_job())
background.add(task)
task.add_done_callback(background.discard)
```

Ещё лучше — `TaskGroup` (урок 05).

## Необработанные исключения

Если задача упала, а её результат никто не забрал (`await`, `result()`,
`exception()`), при уничтожении объекта в лог уйдёт
`Task exception was never retrieved`. Всегда забирайте результаты задач.

## Задачи создаются «лениво», но запускаются «жадно»

`create_task` только планирует запуск: тело корутины начнёт выполняться,
когда текущая корутина дойдёт до точки переключения. Исключение —
`eager_task_factory` (урок 14).

## Задачи

Проверка: `uv run pytest lessons/l02_tasks -v`

1. `run_concurrently` — конкурентный запуск нескольких ожиданий.
2. `spawn_named` — имена задач и `current_task()`.
3. `describe` — состояния задачи.
4. `BackgroundTasks` — безопасный «fire-and-forget» со строгими ссылками и
   колбэками завершения.
5. `first_done_name` — колбэки `add_done_callback` + `Future` для ожидания.

## Вопросы для самопроверки

1. Когда начнёт выполняться тело корутины, переданной в `create_task`?
2. Что будет, если задача упала, а её никто не `await`-ил?
3. Почему нужна сильная ссылка на задачу?
4. Чем `task.result()` отличается от `await task`?
5. Что вернёт `task.exception()` для отменённой задачи?

Документация: https://docs.python.org/3/library/asyncio-task.html#creating-tasks
