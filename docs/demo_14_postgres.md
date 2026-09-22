# Демо 14. Connections і робота з Postgres

Файл: [`dags/demo_14_postgres.py`](../dags/demo_14_postgres.py)

## Ідея

Connection — іменований набір доступів: хост, порт, база, логін, пароль, додаткові параметри. Код знає лише ім'я (`conn_id`), усе решта лежить в Airflow.

Навіщо: пароль не потрапляє в git; зміна пароля — правка в одному місці, а не в двадцяти DAG-ах; різні середовища (dev/prod) використовують той самий код із різними підключеннями.

## Три способи завести підключення

**1. Інтерфейс:** Admin → Connections → плюс

| Поле | Значення |
|---|---|
| Connection Id | `demo_postgres` |
| Connection Type | Postgres |
| Host | адреса сервера |
| Database | назва бази |
| Login | логін |
| Password | пароль |
| Port | 5432 |
| Extra | JSON з додатковими параметрами, напр. `{"sslmode": "require"}` |

Кнопка **Test** одразу перевіряє з'єднання.

**2. Змінна середовища** — у `docker-compose.yaml`:

```yaml
AIRFLOW_CONN_DEMO_POSTGRES: "postgresql://логін:пароль@хост:5432/база"
```

Префікс `AIRFLOW_CONN_`, ім'я великими літерами → `conn_id` маленькими. Символи `@ : / ? #` у паролі треба закодувати (`@` → `%40`, `#` → `%23`), інакше URI розбереться неправильно. Замість URI можна передати JSON:

```yaml
AIRFLOW_CONN_DEMO_POSTGRES: '{"conn_type":"postgres","host":"...","login":"...","password":"...","schema":"база","port":5432}'
```

**3. CLI:**

```bash
docker exec airflow airflow connections add demo_postgres --conn-uri "postgresql://..."
docker exec airflow airflow connections list
docker exec airflow airflow connections delete demo_postgres
```

Пріоритет той самий, що й у Variables: **змінна середовища → секретний бекенд → база метаданих**.

## Два способи виконати SQL

### `SQLExecuteQueryOperator` — коли результат не потрібен у Python

```python
SQLExecuteQueryOperator(
    task_id="create_table",
    conn_id="demo_postgres",
    sql="CREATE TABLE IF NOT EXISTS demo_orders (...);",
)
```

| Параметр | Значення за замовч. | Що робить |
|---|---|---|
| **`sql`** | обов'язковий | Рядок, список рядків або шлях до `.sql`. Підтримує Jinja |
| **`conn_id`** | `None` | Ім'я підключення |
| `parameters` | `None` | Значення для плейсхолдерів `%s` |
| `autocommit` | `False` | Комітити кожну команду одразу |
| `split_statements` | `None` | Розбити рядок на окремі команди по `;` |
| `return_last` | `True` | Повернути результат лише останньої команди |
| `handler` | `fetch_all_handler` | Як обробити курсор |
| `show_return_value_in_logs` | `False` | Друкувати результат у логах |
| `database` | `None` | Перекрити базу з підключення |

Цей оператор універсальний: із `conn_id` від MySQL він піде в MySQL, від Snowflake — у Snowflake. Окремі `PostgresOperator`, `MySqlOperator` тощо застаріли.

### `PostgresHook` — коли результат треба обробити

```python
hook = PostgresHook(postgres_conn_id="demo_postgres")
records = hook.get_records("SELECT client, amount FROM demo_orders;")
```

| Метод | Повертає | Призначення |
|---|---|---|
| **`run(sql, parameters=, autocommit=)`** | `None` або результат | Виконати команду |
| **`get_records(sql, parameters=)`** | список кортежів | Прочитати всі рядки |
| **`get_first(sql, parameters=)`** | один кортеж | Одне значення, агрегат |
| **`get_df(sql, df_type="pandas")`** | DataFrame | Одразу в pandas або polars |
| `get_df_by_chunks(sql, chunksize=)` | генератор | Великі вибірки частинами |
| **`insert_rows(table, rows, target_fields=, commit_every=1000, replace=)`** | `None` | Пакетна вставка |
| `upsert_rows(table, rows, target_fields=, conflict_fields=, update_fields=)` | `None` | `INSERT ... ON CONFLICT DO UPDATE` |
| `copy_expert(sql, filename)` | `None` | `COPY` — найшвидший шлях для CSV |
| `bulk_load(table, tmp_file)` / `bulk_dump` | `None` | Завантаження/вивантаження файлу |
| `get_conn()` | connection | Сире з'єднання psycopg2 |
| `get_sqlalchemy_engine()` | Engine | Для pandas `to_sql` |
| `get_table_primary_key(table)` | список | Первинний ключ таблиці |
| `test_connection()` | `(bool, str)` | Перевірка доступу |

## Параметри замість форматування рядків

```python
# ТАК НЕ ТРЕБА
hook.run(f"SELECT * FROM orders WHERE client = '{name}'")

# ТАК ТРЕБА
hook.get_records("SELECT * FROM orders WHERE client = %s", parameters=(name,))
```

`%s` — плейсхолдер psycopg2. Значення підставляє драйвер, екрануючи його як треба. Це захист від SQL-ін'єкції: ім'я `'; DROP TABLE orders; --` стане звичайним текстовим значенням, а не командою. Плюс такі запити швидші за рахунок повторного використання плану.

У `SQLExecuteQueryOperator` те саме робиться через `parameters=`.

## Читання Connection із коду

```python
from airflow.sdk import Connection

conn = Connection.get("demo_postgres")
print(conn.host, conn.login, conn.schema, conn.port)
print(conn.extra_dejson.get("sslmode"))
```

У шаблонах: `{{ conn.demo_postgres.host }}`.

Зверніть увагу: назва бази лежить у полі **`schema`** — історична особливість Airflow, яка регулярно збиває з пантелику.

## Ідемпотентність завантаження

Задача з `retries` може виконатися двічі. Щоб повтор не створив дублів:

| Підхід | Запис |
|---|---|
| Перезапис періоду | `DELETE FROM t WHERE day = %s` перед вставкою |
| Upsert | `INSERT ... ON CONFLICT (id) DO UPDATE` або `hook.upsert_rows(...)` |
| Тимчасова таблиця | Завантажити в `_tmp`, потім атомарно підмінити |

## Типові помилки

| Симптом | Причина |
|---|---|
| `The conn_id ... isn't defined` | Підключення не заведене, або помилка в імені |
| `could not translate host name` | Хост недосяжний із контейнера. `localhost` усередині контейнера — це сам контейнер |
| Пароль не підходить, хоч правильний | Спецсимволи в URI не закодовані |
| `relation does not exist` | Не та база або не та схема; перевірте `search_path` |
| Після повтору задачі дані задублились | Завантаження не ідемпотентне |
| `No module named psycopg2` | Не встановлений провайдер `apache-airflow-providers-postgres` |
| Пам'ять закінчилась на великій вибірці | `get_records` тягне все в пам'ять; беріть `get_df_by_chunks` |

## Що спробувати

- Натиснути **Test** у Admin → Connections.
- Замінити `get_records` на `get_df` і подивитися на DataFrame у логах.
- Додати в `Extra` поле `{"sslmode": "require"}` — для хмарних баз це зазвичай обов'язково.
