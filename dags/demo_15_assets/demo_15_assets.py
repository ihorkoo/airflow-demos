# ДЕМО 15. Assets — запуск за готовністю даних.
# Теорія: README.md у цій папці. Той самий текст видно в інтерфейсі
# Airflow на сторінці DAG-а у вкладці Docs — його підставляє doc_md нижче.
#
# Що показує: досі кожен DAG запускався або вручну, або за годинником.
# Asset дає третій варіант: DAG-споживач стартує сам, щойно DAG-постачальник
# оновив потрібні йому дані. Розклад споживача — не час, а дані.
#
# У файлі два DAG-и. Запускати треба ЛИШЕ перший: другий стартує сам
# за хвилину-дві після того, як перший відпрацює.

from datetime import datetime
from pathlib import Path

from airflow.sdk import Asset, Metadata, dag, get_current_context, task

DATA_DIR = Path("/opt/airflow/data")

# Asset — це іменований набір даних. Airflow його не читає і не перевіряє:
# для нього це просто мітка, за оновленням якої він стежить.
orders_raw = Asset(
    name="orders_raw",
    uri="file:///opt/airflow/data/orders_raw.csv",
    group="raw",                          # для групування на сторінці Assets
)

customers_raw = Asset(
    name="customers_raw",
    uri="file:///opt/airflow/data/customers_raw.csv",
    group="raw",
)


# Конспект із сусіднього README.md — показується у вкладці Docs в інтерфейсі
README = (Path(__file__).parent / "README.md").read_text(encoding="utf-8")


@dag(
    dag_id="demo_15_assets_producer",
    doc_md=README,
    start_date=datetime(2025, 1, 1),
    schedule=None,                        # постачальника запускаємо вручну
    catchup=False,
    tags=["demo", "assets"],
)
def demo_15_assets_producer():

    # outlets каже: успішне завершення цієї задачі означає,
    # що набір orders_raw оновився
    @task(outlets=[orders_raw])
    def load_orders() -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        rows = ["id,client,amount", "1,Молочарня,1250.00", "2,Пекарня,830.50"]
        path = DATA_DIR / "orders_raw.csv"
        path.write_text("\n".join(rows))
        print(f"записали {len(rows) - 1} замовлень у {path}")

    # Разом із подією можна передати дані — Metadata кладе їх у extra,
    # і споживач зможе їх прочитати
    @task(outlets=[customers_raw])
    def load_customers():
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        rows = ["id,name", "1,Іваненко", "2,Петренко", "3,Коваль"]
        path = DATA_DIR / "customers_raw.csv"
        path.write_text("\n".join(rows))
        print(f"записали {len(rows) - 1} клієнтів у {path}")

        yield Metadata(customers_raw, {"row_count": len(rows) - 1, "file": str(path)})

    load_orders()
    load_customers()


@dag(
    dag_id="demo_15_assets_consumer",
    doc_md=README,
    start_date=datetime(2025, 1, 1),
    # Ось головний рядок демо: замість розкладу — список наборів даних.
    # Список означає І: DAG чекає, поки оновляться ОБИДВА.
    schedule=[orders_raw, customers_raw],
    catchup=False,
    tags=["demo", "assets"],
)
def demo_15_assets_consumer():

    @task
    def build_report() -> None:
        orders = (DATA_DIR / "orders_raw.csv").read_text().splitlines()
        customers = (DATA_DIR / "customers_raw.csv").read_text().splitlines()
        print(f"звіт: {len(orders) - 1} замовлень, {len(customers) - 1} клієнтів")

    @task
    def show_trigger_reason() -> None:
        """Споживач бачить, які саме події його розбудили."""
        context = get_current_context()
        events = context["triggering_asset_events"]

        for asset_uri, event_list in events.items():
            for event in event_list:
                print(f"подія від {asset_uri}, extra: {event.extra}")

    build_report() >> show_trigger_reason()


demo_15_assets_producer()
demo_15_assets_consumer()
