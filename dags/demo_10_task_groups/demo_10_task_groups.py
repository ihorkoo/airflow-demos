# ДЕМО 10. Групи задач.
# Теорія: README.md у цій папці. Той самий текст видно в інтерфейсі
# Airflow на сторінці DAG-а у вкладці Docs — його підставляє doc_md нижче.
#
# Що показує: коли задач стає багато, граф перетворюється на кашу.
# TaskGroup складає пов'язані задачі в один блок, який в інтерфейсі
# можна згорнути й розгорнути.

from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task, task_group


# Конспект із сусіднього README.md — показується у вкладці Docs в інтерфейсі
README = (Path(__file__).parent / "README.md").read_text(encoding="utf-8")


@dag(
    dag_id="demo_10_task_groups",
    doc_md=README,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo"],
)
def demo_10_task_groups():

    @task
    def start() -> None:
        print("починаємо завантаження")

    # Група 1: усе про замовлення
    @task_group(group_id="orders")
    def orders_group():

        @task
        def extract_orders() -> None:
            print("витягли замовлення")

        @task
        def clean_orders() -> None:
            print("почистили замовлення")

        extract_orders() >> clean_orders()

    # Група 2: усе про клієнтів. Структура така сама, але задачі окремі
    @task_group(group_id="customers")
    def customers_group():

        @task
        def extract_customers() -> None:
            print("витягли клієнтів")

        @task
        def clean_customers() -> None:
            print("почистили клієнтів")

        extract_customers() >> clean_customers()

    @task
    def build_report() -> None:
        print("зібрали звіт з обох джерел")

    # Групи використовуються як звичайні задачі — дві гілки йдуть паралельно
    start() >> [orders_group(), customers_group()] >> build_report()


demo_10_task_groups()
