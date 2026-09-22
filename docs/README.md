# Теорія до демо-DAG-ів

До кожного файлу в папці [`dags/`](../dags/) є окремий конспект: що демонструє демо, як працює механізм, повна довідка параметрів і типові помилки.

Версія Airflow — **3.3.1**. Усі параметри в таблицях звірені саме з нею: код із туторіалів для Airflow 2 у більшості випадків тут не запуститься.

## Порядок вивчення

| № | Тема | Конспект | DAG |
|---|---|---|---|
| 1 | Базовий DAG і TaskFlow | [demo_01_basics.md](demo_01_basics.md) | [`.py`](../dags/demo_01_basics.py) |
| 2 | Класичні оператори | [demo_02_operators.md](demo_02_operators.md) | [`.py`](../dags/demo_02_operators.py) |
| 3 | Форма графа | [demo_03_dependencies.md](demo_03_dependencies.md) | [`.py`](../dags/demo_03_dependencies.py) |
| 4 | XCom — обмін даними | [demo_04_xcom.md](demo_04_xcom.md) | [`.py`](../dags/demo_04_xcom.py) |
| 5 | Контекст, дати, шаблони | [demo_05_context.md](demo_05_context.md) | [`.py`](../dags/demo_05_context.py) |
| 6 | Розгалуження | [demo_06_branching.md](demo_06_branching.md) | [`.py`](../dags/demo_06_branching.py) |
| 7 | Повтори при помилці | [demo_07_retries.md](demo_07_retries.md) | [`.py`](../dags/demo_07_retries.py) |
| 8 | Trigger rules | [demo_08_trigger_rules.md](demo_08_trigger_rules.md) | [`.py`](../dags/demo_08_trigger_rules.py) |
| 9 | Динамічні задачі | [demo_09_dynamic_tasks.md](demo_09_dynamic_tasks.md) | [`.py`](../dags/demo_09_dynamic_tasks.py) |
| 10 | Групи задач | [demo_10_task_groups.md](demo_10_task_groups.md) | [`.py`](../dags/demo_10_task_groups.py) |
| 11 | Сенсори | [demo_11_sensor.md](demo_11_sensor.md) | [`.py`](../dags/demo_11_sensor.py) |
| 12 | Розклад і catchup | [demo_12_schedule.md](demo_12_schedule.md) | [`.py`](../dags/demo_12_schedule.py) |
| 13 | Variables | [demo_13_variables.md](demo_13_variables.md) | [`.py`](../dags/demo_13_variables.py) |
| 14 | Connections і Postgres | [demo_14_postgres.md](demo_14_postgres.md) | [`.py`](../dags/demo_14_postgres.py) |
| 15 | Assets — запуск за даними | [demo_15_assets.md](demo_15_assets.md) | [`.py`](../dags/demo_15_assets.py) |

## Наскрізні теми

Деякі речі згадуються в кількох конспектах — ось де про них написано докладно:

| Тема | Де шукати |
|---|---|
| Повний список параметрів `@dag` | [демо 1](demo_01_basics.md#довідка-параметри-dag) |
| Повний список параметрів `@task` | [демо 1](demo_01_basics.md#довідка-параметри-task) |
| Ключі контексту запуску | [демо 5](demo_05_context.md) |
| Jinja-шаблони | [демо 5](demo_05_context.md) |
| Усі trigger rules | [демо 8](demo_08_trigger_rules.md) |
| Ідемпотентність | [демо 7](demo_07_retries.md), [демо 14](demo_14_postgres.md) |
| Обмеження паралелізму | [демо 3](demo_03_dependencies.md) |
| Зв'язок між DAG-ами | [демо 15](demo_15_assets.md), [демо 11](demo_11_sensor.md) |

## Головні відмінності Airflow 3 від Airflow 2

Найчастіша причина, чому код з інтернету не працює:

| Airflow 2 | Airflow 3 |
|---|---|
| `from airflow.decorators import dag, task` | `from airflow.sdk import dag, task` |
| `from airflow.models import Variable` | `from airflow.sdk import Variable` |
| `schedule_interval=...` | `schedule=...` |
| `Variable.get(k, default_var=...)` | `Variable.get(k, default=...)` |
| `from airflow.operators.bash import BashOperator` | `from airflow.providers.standard.operators.bash import BashOperator` |
| `PostgresOperator` | `SQLExecuteQueryOperator` |
| `execution_date` | `logical_date` |
| `Dataset` | `Asset` |
| `provide_context=True` | не потрібен, контекст підставляється сам |
