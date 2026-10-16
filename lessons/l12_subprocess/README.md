# Урок 12. Подпроцессы

asyncio умеет запускать процессы и общаться с ними через неблокирующие
пайпы — можно управлять десятками процессов из одного потока.

## Запуск

```python
proc = await asyncio.create_subprocess_exec(
    "ls", "-la",                              # программа и аргументы — БЕЗ шелла
    stdout=asyncio.subprocess.PIPE,
    stderr=asyncio.subprocess.PIPE,
)
stdout, stderr = await proc.communicate()     # ждёт завершения, читает всё
print(proc.returncode)
```

`create_subprocess_shell("ls | wc -l", ...)` — через шелл. **Опасно** при
подстановке пользовательских данных (инъекции); используйте `shlex.quote`
или `exec`.

Значения для `stdin/stdout/stderr`: `PIPE`, `DEVNULL`, `STDOUT` (для
stderr — слить в stdout), файловый дескриптор/файл, `None` (наследовать).

## Объект Process

| | |
|---|---|
| `proc.pid`, `proc.returncode` | `returncode is None`, пока процесс жив; отрицательный — убит сигналом |
| `await proc.wait()` | дождаться завершения |
| `await proc.communicate(input=None)` | записать stdin, прочитать stdout/stderr до EOF, дождаться выхода |
| `proc.stdin` | `StreamWriter` |
| `proc.stdout`, `proc.stderr` | `StreamReader` — можно `readline()` в цикле |
| `proc.send_signal(sig)`, `terminate()`, `kill()` | сигналы |

## Ловушки

1. **Дедлок пайпов.** `await proc.wait()` при `stdout=PIPE` и большом
   выводе зависнет: процесс заблокирован на записи в заполненный пайп,
   а вы его не читаете. Используйте `communicate()` или читайте потоки
   конкурентно с ожиданием.
2. **Таймаут не убивает процесс.** `asyncio.wait_for(proc.communicate(), 5)`
   отменит ожидание, но процесс продолжит жить. Нужно `proc.kill()` +
   `await proc.wait()` (чтобы не оставить зомби).
3. **Отмена `communicate()`** может потерять уже прочитанные данные.
4. **Буферизация вывода дочернего процесса**: Python в пайп пишет блоками —
   для «живого» построчного вывода нужен `-u` / `flush=True`.
5. Вывод — байты; декодируйте сами.

## Задачи

Проверка: `uv run pytest lessons/l12_subprocess -v`

Тесты запускают `sys.executable -c "..."` — так они переносимы.

1. `run_command` — запуск и сбор вывода.
2. `run_with_timeout` — таймаут с гарантированным убийством процесса.
3. `stream_lines` — асинхронный генератор строк вывода по мере появления.
4. `run_parallel` — много процессов с ограничением параллельности.
5. `pipe_through` — передача данных в stdin.
6. `interactive_session` — диалог с процессом: запрос → ответ построчно.

## Вопросы для самопроверки

1. Почему `proc.wait()` с `stdout=PIPE` может зависнуть?
2. Чем `create_subprocess_exec` безопаснее `create_subprocess_shell`?
3. Что делать с процессом после таймаута?
4. Почему «живой» вывод дочернего Python-скрипта приходит пачками?
5. Что означает отрицательный `returncode`?

Документация: https://docs.python.org/3/library/asyncio-subprocess.html
