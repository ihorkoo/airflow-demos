# Демо 5. Контекст запуску, дати й шаблони

Файл: [`demo_05_context.py`](demo_05_context.py)

## Ідея

Кожен запуск DAG-а знає, *за яку дату* він рахує дані. Це не те саме, що поточний час на сервері. Саме завдяки цьому перезапуск учорашнього запуску дає учорашній результат, а не сьогоднішній.

Правило, яке відрізняє робочий пайплайн від зламаного: **ніколи не беріть `datetime.now()` для вибірки даних**. Беріть дату з контексту.

## Логічна дата

`logical_date` — мітка періоду, за який рахується запуск. Для `@daily` запуск за 15 березня стартує **на початку 16 березня** й обробляє дані 15-го.

| Поле контексту | Що це |
|---|---|
| `logical_date` | Мітка періоду запуску |
| `data_interval_start` | Початок інтервалу даних |
| `data_interval_end` | Кінець інтервалу даних |
| `run_id` | Ідентифікатор запуску (`manual__...`, `scheduled__...`) |

Для щоденного DAG-а `data_interval_start` = `logical_date`, а `data_interval_end` — доба потому. У SQL фільтрують саме по інтервалу:

```sql
WHERE created_at >= '{{ data_interval_start }}' AND created_at < '{{ data_interval_end }}'
```

## Доступ із Python

```python
from airflow.sdk import get_current_context

context = get_current_context()
context["ti"].task_id
```

Другий спосіб — оголосити потрібні ключі параметрами функції, Airflow підставить їх сам:

```python
@task
def show(ds: str, run_id: str, ti=None):
    print(ds, run_id, ti.try_number)
```

## Довідка: ключі контексту (Airflow 3.3.1)

| Ключ | Тип | Що це |
|---|---|---|
| `dag` | `DAG` | Об'єкт DAG-а |
| `dag_run` | `DagRun` | Запуск: `run_type`, `run_id`, `conf`, `start_date` |
| `ti` / `task_instance` | `TaskInstance` | Поточна задача: `task_id`, `try_number`, `xcom_push/pull`, `map_index` |
| `task` | `BaseOperator` | Об'єкт задачі |
| `logical_date` | `DateTime` | Мітка періоду |
| `data_interval_start` / `data_interval_end` | `DateTime` | Межі інтервалу |
| `ds` / `ds_nodash` | `str` | `2025-03-15` / `20250315` |
| `ts` / `ts_nodash` / `ts_nodash_with_tz` | `str` | Час у ISO та без розділювачів |
| `run_id` | `str` | Ідентифікатор запуску |
| `try_number` | `int` | Номер спроби, з 1 |
| `task_reschedule_count` | `int` | Скільки разів сенсор ішов на переплановування |
| `params` | `dict` | Параметри DAG-а і те, що ввели при ручному запуску |
| `var` | — | Доступ до Variables: `var.value.ім'я`, `var.json.ім'я` |
| `conn` | — | Доступ до Connections: `conn.ім'я.host` |
| `macros` | — | `macros.ds_add`, `macros.datetime`, `macros.uuid` тощо |
| `exception` | — | Виняток, доступний у callback при помилці |
| `reason` | `str` | Причина виклику callback |
| `prev_start_date_success` | `DateTime` | Коли востаннє успішно стартував цей DAG |
| `prev_data_interval_start_success` / `..._end_success` | `DateTime` | Інтервал останнього успішного запуску |
| `test_mode` | `bool` | Чи це запуск через `airflow dags test` |
| `expanded_ti_count` | `int` | Скільки копій у динамічної задачі |
| `inlets` / `outlets` / `inlet_events` / `outlet_events` | — | Assets — дані, від яких залежить задача |
| `triggering_asset_events` | — | Які події Asset-ів спричинили цей запуск |
| `templates_dict` | `dict` | Тільки в `PythonOperator` |

## Шаблони Jinja

Другий шлях до тих самих значень — подвійні дужки в рядкових параметрах оператора:

```python
BashOperator(task_id="x", bash_command='echo "дата: {{ ds }}"')
```

| Шаблон | Приклад результату |
|---|---|
| `{{ ds }}` | `2025-03-15` |
| `{{ ds_nodash }}` | `20250315` |
| `{{ data_interval_start }}` | `2025-03-15T00:00:00+00:00` |
| `{{ run_id }}` | `scheduled__2025-03-15T00:00:00+00:00` |
| `{{ macros.ds_add(ds, -7) }}` | `2025-03-08` |
| `{{ var.value.target_schema }}` | значення Variable ([демо 13](../demo_13_variables/README.md)) |
| `{{ conn.demo_postgres.host }}` | поле Connection ([демо 14](../demo_14_postgres/README.md)) |
| `{{ params.my_param }}` | параметр запуску |
| `{{ dag_run.conf["file"] }}` | значення з «Trigger DAG w/ config» |

Шаблони працюють **не всюди**, а лише в полях зі списку `template_fields` конкретного оператора. Для `BashOperator` це `bash_command` та `env`, для `SQLExecuteQueryOperator` — `sql` і `parameters`. Подивитися список: `BashOperator.template_fields`.

## Типові помилки

| Симптом | Причина |
|---|---|
| `{{ ds }}` надрукувалось як текст | Поле не входить у `template_fields` цього оператора |
| Перезапуск старого дня дав свіжі дані | У коді `datetime.now()` замість `logical_date` |
| `logical_date` порожня | Ручний запуск без дати — код має це передбачати |
| Дані «з'їхали» на добу | Сплутали `logical_date` і фактичний час старту |

## Що спробувати

- Запустити DAG через **Trigger DAG w/ config** з `{"file": "orders.csv"}` і прочитати це в `dag_run.conf`.
- Додати в `BashOperator` `{{ macros.ds_add(ds, -1) }}` — отримаєте вчорашню дату.
