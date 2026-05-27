# Что такое my_nedo_cron?

`my_nedo_cron` - расширение PostgreSQL для запуска SQL-команд по расписанию.

Расширение работает внутри PostgreSQL как background worker, хранит задачи в таблице `nedo_cron.jobs` и записывает историю запусков в `nedo_cron.job_run_details`.

---

# Содержание

- [Как работает my_nedo_cron](#как-работает-my_nedo_cron)
- [Cron-синтаксис](#cron-синтаксис)
- [Управление задачами](#управление-задачами)
- [Сборка и установка](#сборка-и-установка)
- [Настройка PostgreSQL](#настройка-postgresql)
- [Мониторинг запусков](#мониторинг-запусков)
- [Настройки расширения](#настройки-расширения)
- [Проверка](#проверка)

---

# Как работает my_nedo_cron

После загрузки через `shared_preload_libraries` расширение запускает background worker `my_nedo_cron`.

Worker читает задачи из таблицы:

```sql
CREATE TABLE nedo_cron.jobs (
    jobid bigint PRIMARY KEY,
    schedule text NOT NULL,
    command text NOT NULL,
    nodename text NOT NULL DEFAULT 'localhost',
    nodeport int NOT NULL,
    database text NOT NULL DEFAULT current_database(),
    username text NOT NULL DEFAULT current_user
);
```

Для выполнения задачи worker открывает отдельное `libpq`-подключение к PostgreSQL от имени пользователя, который создал задачу, и отправляет SQL-команду на выполнение.

Разные задачи могут выполняться параллельно через разные подключения. Одна и та же задача не запускается параллельно сама с собой: если следующий запуск наступил раньше завершения предыдущего, он ставится в очередь.

Изменения в `nedo_cron.jobs` подхватываются через relcache invalidation, поэтому `schedule`, `unschedule`, прямой `INSERT`, `UPDATE` или `DELETE` таблицы задач быстро становятся видны worker-у.

# Cron-синтаксис

Поддерживается стандартный cron-формат из 5 полей:

```text
 ┌───────────── minute (0 - 59)
 │ ┌───────────── hour (0 - 23)
 │ │ ┌───────────── day of month (1 - 31), $ = последний день месяца
 │ │ │ ┌───────────── month (1 - 12 или JAN-DEC)
 │ │ │ │ ┌───────────── day of week (0 - 7 или SUN-SAT, 0 и 7 = Sunday)
 │ │ │ │ │
 * * * * *
```

Примеры расписаний:

```text
'5 seconds'    # каждые 5 секунд
'* * * * *'    # каждую минуту
'*/5 * * * *'  # каждые 5 минут
'0 * * * *'    # каждый час
'0 0 * * *'    # каждый день в 00:00
'0 0 * * 1-5'  # каждый будний день в 00:00
'0 12 $ * *'   # в 12:00 в последний день месяца
```

Дополнительно поддерживается формат секунд:

```sql
'N seconds'
```

где `N` должен быть от `1` до `59`.

# Управление задачами

## Создать задачу

```sql
SELECT nedo_cron.schedule('5 seconds', 'SELECT 1');
```

Функция возвращает `jobid`:

```text
 schedule
----------
        1
```

Пример cron-задачи:

```sql
SELECT nedo_cron.schedule(
    '0 3 * * *',
    $$DELETE FROM events WHERE created_at < now() - interval '7 days'$$
);
```

Пример задачи на последний день месяца:

```sql
SELECT nedo_cron.schedule(
    '0 12 $ * *',
    'CALL process_month_end()'
);
```

## Проверить расписание

```sql
SELECT nedo_cron.check_schedule('5 seconds');
SELECT nedo_cron.check_schedule('* * * * *');
```

Если расписание некорректное, `schedule()` вернет ошибку с указанием поля и токена:

```sql
SELECT nedo_cron.schedule('* * * * 8', 'SELECT 1');
```

Пример ошибки:

```text
invalid schedule at field 5 near "8": day-of-week must be 0-7
```

## Посмотреть задачи

```sql
SELECT * FROM nedo_cron.jobs;
```

## Удалить задачу

```sql
SELECT nedo_cron.unschedule(1);
```

При удалении задачи связанные строки из `nedo_cron.job_run_details` удаляются через `ON DELETE CASCADE`.

# Сборка и установка

Для PostgreSQL 17 на Debian/Ubuntu установите зависимости:

```bash
sudo apt install postgresql-17 postgresql-server-dev-17 build-essential libpq-dev
```

Соберите расширение из исходников:

```bash
make
sudo make install
```

Если `pg_config` находится не в стандартном месте, укажите его в `Makefile` или передайте через окружение:

```bash
make PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config
sudo make install PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config
```

# Настройка PostgreSQL

Чтобы background worker запускался вместе с PostgreSQL, добавьте расширение в `postgresql.conf`:

```conf
shared_preload_libraries = 'my_nedo_cron'
```

Если уже используются другие preload-расширения, перечислите их через запятую:

```conf
shared_preload_libraries = 'pg_cron,my_nedo_cron'
```

Укажите базу данных, в которой будут храниться metadata-таблицы `nedo_cron.jobs` и `nedo_cron.job_run_details`:

```conf
nedo_cron.database_name = 'postgres'
```

`CREATE EXTENSION` нужно выполнять именно в этой базе:

```sql
CREATE EXTENSION my_nedo_cron;
```

После изменения `shared_preload_libraries` или `nedo_cron.database_name` требуется перезапуск PostgreSQL:

```bash
sudo systemctl restart postgresql
```

## Доступ для выполнения задач

`my_nedo_cron` запускает задачи через `libpq`-подключение к `localhost` от имени пользователя, который создал задачу.

Поэтому `pg_hba.conf` должен разрешать такое подключение. Для локальной разработки обычно используют `trust` для `127.0.0.1/32` или настраивают пароль через `.pgpass`.

Если подключение запрещено, задача завершится со статусом `failed`, а текст ошибки будет записан в `nedo_cron.job_run_details.return_message`.

# Мониторинг запусков

История запусков хранится в таблице `nedo_cron.job_run_details`:

```sql
SELECT jobid,
       job_pid,
       database,
       username,
       status,
       return_message,
       start_time,
       end_time
FROM nedo_cron.job_run_details
ORDER BY run_id DESC
LIMIT 10;
```

Возможные статусы:

```text
starting
running
succeeded
failed
timeout
cancelled
```

Таблица истории не очищается автоматически. Если нужно удалять старые записи, можно запланировать отдельную задачу:

```sql
SELECT nedo_cron.schedule(
    '0 12 * * *',
    $$DELETE FROM nedo_cron.job_run_details
      WHERE end_time < now() - interval '7 days'$$
);
```

# Настройки расширения

| Настройка | Значение по умолчанию | Описание |
| --- | --- | --- |
| `nedo_cron.database_name` | `postgres` | База данных, где находятся metadata-таблицы расширения. Меняется только после restart PostgreSQL. |
| `nedo_cron.statement_timeout_ms` | `10000` | Максимальное время выполнения SQL-команды в миллисекундах. Применяется после `pg_reload_conf()`. |

Посмотреть текущие настройки:

```sql
SELECT name, setting, context
FROM pg_settings
WHERE name LIKE 'nedo_cron.%';
```

Изменить timeout выполнения задач:

```sql
ALTER SYSTEM SET nedo_cron.statement_timeout_ms = 5000;
SELECT pg_reload_conf();
```

Изменить metadata-базу:

```sql
ALTER SYSTEM SET nedo_cron.database_name = 'postgres';
```

После изменения `nedo_cron.database_name` нужен restart PostgreSQL.

# Проверка

Быстрая ручная проверка:

```sql
SELECT nedo_cron.schedule('5 seconds', 'SELECT 1');

SELECT *
FROM nedo_cron.job_run_details
ORDER BY run_id DESC
LIMIT 5;
```

Интеграционный тест расширения:

```bash
sudo -u postgres psql -v ON_ERROR_STOP=1 -d postgres -f tests/ultimate_extension_test.sql
```

Ожидаемый финальный вывод:

```text
my_nedo_cron ultimate test passed
```
