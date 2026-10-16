# Урок 05. TaskGroup и ExceptionGroup — структурированная конкурентность

## Проблема

`create_task` + `gather` легко оставляют «сирот»: задача упала, а соседи
продолжают работать; исключение потерялось; задача пережила функцию,
которая её создала. **Структурированная конкурентность**: время жизни
дочерних задач ограничено блоком кода.

## `asyncio.TaskGroup` (3.11+)

```python
async with asyncio.TaskGroup() as tg:
    t1 = tg.create_task(fetch("a"))
    t2 = tg.create_task(fetch("b"))
# здесь ВСЕ задачи группы гарантированно завершены
print(t1.result(), t2.result())
```

Правила:
1. Выход из `async with` ждёт **все** задачи группы.
2. Если любая задача упала (не `CancelledError`), группа **отменяет все
   остальные** задачи и тело блока (если оно ещё ждёт).
3. Все исключения собираются в `ExceptionGroup` (или `BaseExceptionGroup`),
   который бросается из `async with`.
4. Новые задачи можно добавлять, пока группа активна — в том числе
   **из самих задач** (передайте `tg` внутрь).
5. Если отменили внешнюю задачу — отменяются все дети, наружу выходит
   `CancelledError`.
6. `KeyboardInterrupt`/`SystemExit` в задаче пробрасываются как есть.

## `ExceptionGroup` и `except*` (3.11+)

```python
try:
    async with asyncio.TaskGroup() as tg:
        ...
except* ValueError as eg:          # eg — ExceptionGroup только с ValueError
    for e in eg.exceptions:
        log(e)
except* (KeyError, TypeError) as eg:
    ...
```

- Каждая ветка `except*` «откусывает» подходящие исключения; необработанные
  пробрасываются дальше новой группой.
- Внутри `except*` нельзя `return`/`break`/`continue` — сохраняйте данные в
  переменные и выходите после блока.
- `eg.subgroup(pred)`, `eg.split(type)`, `eg.derive(...)` — ручная работа с группой.
- Обычный `except ExceptionGroup:` тоже работает — ловит группу целиком.

## Досрочная остановка группы

В 3.13 у `TaskGroup` нет метода `cancel()`. Идиома — бросить своё исключение
внутри задачи и поймать его через `except*`:

```python
class Found(Exception):
    def __init__(self, value): self.value = value

try:
    async with asyncio.TaskGroup() as tg:
        for x in items:
            tg.create_task(check(x))        # check бросает Found(x)
except* Found as eg:
    result = eg.exceptions[0].value
```

## TaskGroup и таймауты

```python
async with asyncio.timeout(5):
    async with asyncio.TaskGroup() as tg:
        ...
```

По таймауту отменяется внешняя задача → группа отменяет детей → снаружи
`TimeoutError`.

## Когда TaskGroup не подходит

Если ошибка одной задачи **не должна** отменять соседей — либо ловите
исключения внутри каждой задачи (обёртка), либо используйте
`gather(return_exceptions=True)`.

## Задачи

Проверка: `uv run pytest lessons/l05_taskgroup -v`

1. `process_all` — результаты группы по порядку.
2. `collect_errors` — `except*` по типам, остальные пробрасываются.
3. `group_with_timeout` — группа под таймаутом.
4. `run_isolated` — ошибки не отменяют соседей.
5. `walk_tree` — динамическое добавление задач в группу из задач.
6. `search_first` — досрочная остановка группы своим исключением.

## Вопросы для самопроверки

1. Что произойдёт с задачами группы, если одна из них бросила `ValueError`?
2. Почему внутри `except*` нельзя написать `return`?
3. Что выйдет из `async with TaskGroup`, если две задачи упали одновременно?
4. Чем `TaskGroup` лучше `gather` и в каких случаях хуже?
5. Как добавить задачу в группу из другой задачи этой же группы?

Документация: https://docs.python.org/3/library/asyncio-task.html#task-groups
