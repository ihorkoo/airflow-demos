# ДЕМО 14. Connection — підключення до Postgres.
# Теорія: docs/demo_14_postgres.md
#
# Що показує: логін і пароль до бази НІКОЛИ не пишуть у файлі DAG-а.
# Вони лежать в Airflow під іменем (conn_id), а код знає лише це ім'я.
# Поміняли пароль — правите його в одному місці, DAG-и не чіпаєте.
#
# Де завести підключення (досить одного зі способів):
#
#   1) В інтерфейсі: Admin → Connections → плюс
#        Connection Id   demo_postgres
#        Connection Type Postgres
#        Host            адреса сервера
#        Database        назва бази
#        Login           ваш логін
#        Password        ваш пароль
#        Port            5432
#
#   2) Через docker-compose.yaml, у секції environment:
#        AIRFLOW_CONN_DEMO_POSTGRES: "postgresql://логін:пароль@хост:5432/база"
#      Ім'я змінної — це conn_id великими літерами з префіксом AIRFLOW_CONN_.
#      Після правки: docker compose up -d
#
# Увага до пароля в рядку підключення: символи @ : / ? # треба закодувати
# (@ → %40, # → %23). Якщо пароль «складний» — простіше завести в інтерфейсі.

from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

# Єдине, що знає код про базу, — ім'я підключення
CONN_ID = "demo_postgres"


@dag(
    dag_id="demo_14_postgres",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo"],
)
def demo_14_postgres():

    # Спосіб 1: оператор. Просто виконує SQL, нічого не повертає в код.
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS demo_orders (
                id     SERIAL PRIMARY KEY,
                client TEXT NOT NULL,
                amount NUMERIC(10, 2) NOT NULL,
                loaded TIMESTAMP DEFAULT now()
            );
        """,
    )

    # Спосіб 2: хук. Потрібен, коли результат треба обробити в Python.
    @task
    def insert_rows() -> int:
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        rows = [
            ("Молочарня", 1250.00),
            ("Пекарня", 830.50),
            ("Кав'ярня", 2100.75),
        ]
        # %s — плейсхолдери. Значення підставляє драйвер, не ми рядками:
        # так неможливо зламати запит через дані (SQL-ін'єкція).
        hook.insert_rows(table="demo_orders", rows=rows, target_fields=["client", "amount"])
        print(f"вставили рядків: {len(rows)}")
        return len(rows)

    @task
    def read_back() -> None:
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        # get_records повертає список кортежів — звичайний Python
        records = hook.get_records(
            "SELECT client, amount FROM demo_orders ORDER BY amount DESC LIMIT 5;"
        )
        for client, amount in records:
            print(f"{client}: {amount}")

        total = hook.get_first("SELECT sum(amount) FROM demo_orders;")[0]
        print(f"разом у таблиці: {total}")

    @task
    def cleanup() -> None:
        """Прибираємо за собою, щоб демо можна було запускати скільки завгодно разів."""
        PostgresHook(postgres_conn_id=CONN_ID).run("DROP TABLE IF EXISTS demo_orders;")
        print("таблицю demo_orders видалено")

    create_table >> insert_rows() >> read_back() >> cleanup()


demo_14_postgres()
