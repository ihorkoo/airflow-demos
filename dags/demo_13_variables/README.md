# Демо 13. Variables

Файл: [`demo_13_variables.py`](demo_13_variables.py)

## Ідея

Variables — сховище «ключ–значення» всередині Airflow. Туди кладуть налаштування, які змінюються незалежно від коду: назву схеми, ліміт рядків, адресу папки, перелік регіонів для завантаження.

Сенс простий: змінити значення в інтерфейсі може аналітик, не відкриваючи репозиторій і не роблячи деплой.

## Довідка: API

| Виклик | Що робить |
|---|---|
| `Variable.get(key)` | Прочитати. Немає змінної — помилка |
| `Variable.get(key, default=...)` | Прочитати з запасним значенням |
| `Variable.get(key, deserialize_json=True)` | Розібрати значення як JSON у `dict` |
| `Variable.set(key, value)` | Записати |
| `Variable.set(key, obj, serialize_json=True)` | Записати словник як JSON |
| `Variable.set(key, value, description=...)` | Записати з описом |
| `Variable.delete(key)` | Видалити |
| `Variable.keys()` | Перелік усіх ключів |

> В Airflow 2 параметр звався `default_var`. У третій версії — **`default`**. Це одна з частих причин `TypeError` при копіюванні коду з туторіалів.

## Три способи завести змінну

**1. Інтерфейс:** Admin → Variables → плюс. Поля: `Key`, `Val`, `Description`.

**2. Змінна середовища** — у `docker-compose.yaml`:

```yaml
AIRFLOW_VAR_TARGET_SCHEMA: "public"
AIRFLOW_VAR_REPORT_SETTINGS: '{"email": "data@example.com", "retries": 2}'
```

Префікс `AIRFLOW_VAR_`, ім'я великими літерами. У коді — маленькими: `Variable.get("target_schema")`.

**3. CLI:**

```bash
docker exec airflow airflow variables set target_schema public
docker exec airflow airflow variables export vars.json
```

Пріоритет пошуку: **змінна середовища → секретний бекенд → база метаданих**. Змінна із середовища перекриває те, що заведено в інтерфейсі, і в інтерфейсі її не видно взагалі.

## Правило, яке варто запам'ятати

```python
# ТАК НЕ ТРЕБА
SCHEMA = Variable.get("target_schema")      # верхній рівень файлу

@dag(...)
def my_dag(): ...
```

Верхній рівень файлу Airflow перечитує кожні кілька секунд (`min_file_process_interval`, за замовчуванням 30 с) для **кожного** DAG-файлу. Виклик `Variable.get` там перетворюється на постійний потік запитів до бази. На десятку DAG-ів це вже помітне навантаження.

Правильно — усередині задачі:

```python
@task
def load():
    schema = Variable.get("target_schema", default="public")
```

Або через шаблон, який рендериться безпосередньо перед запуском задачі:

```python
BashOperator(task_id="x", bash_command="echo {{ var.value.target_schema }}")
```

| Шаблон | Що дає |
|---|---|
| `{{ var.value.ім'я }}` | Рядкове значення |
| `{{ var.json.ім'я }}` | Розібраний JSON |
| `{{ var.json.ім'я.поле }}` | Поле зі словника |

## JSON замість десяти змінних

Замість `report_email`, `report_retries`, `report_format` заведіть одну `report_settings`:

```json
{"email": "data@example.com", "retries": 2, "format": "xlsx"}
```

```python
settings = Variable.get("report_settings", deserialize_json=True)
```

Менше записів в інтерфейсі, і всі налаштування одного процесу лежать разом.

## Variables і секрети

Якщо ключ змінної містить одне зі слів `password`, `passwd`, `passphrase`, `secret`, `token`, `access_token`,
`api_key`, `apikey`, `access_key`, `private_key`, `authorization`, `auth_header`, `bearer`, `connection_string`,
`dsn`, `proxy`, `webhook_url`, `service_account` — Airflow **маскує значення в логах** зірочками.
Повний перелік задається параметром `sensitive_var_conn_names`. Це страховка, а не система безпеки.

Для паролів правильне місце — **Connections** ([демо 14](../demo_14_postgres/README.md)), а для серйозного продакшена — секретний бекенд (HashiCorp Vault, AWS Secrets Manager, GCP Secret Manager) через `AIRFLOW__SECRETS__BACKEND`.

## Типові помилки

| Симптом | Причина |
|---|---|
| `TypeError: unexpected keyword argument 'default_var'` | Airflow 2 API; треба `default` |
| `KeyError` / `Variable ... does not exist` | Немає змінної і не заданий `default` |
| Змінив в інтерфейсі — нічого не змінилось | Значення перекрите змінною середовища |
| Airflow гальмує без видимої причини | `Variable.get` на верхньому рівні файлу |
| Значення прийшло рядком `"500"` | Variables зберігаються як текст; потрібен `int(...)` |
| Пароль світиться в логах | Ключ не містить слова зі списку маскування |

## Що спробувати

- Завести `target_schema` в Admin → Variables зі значенням `staging` і переконатися, що значення зі `docker-compose.yaml` усе одно перемагає.
- Прибрати рядок із `docker-compose.yaml`, зробити `docker compose up -d` — тепер працює значення з інтерфейсу.
- Подивитися в Admin → Variables на `last_run_marker`, який DAG записав сам.
