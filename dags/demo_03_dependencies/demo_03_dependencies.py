# ДЕМО 3. Форма графа: послідовно, паралельно, збірка.
# Теорія: README.md у цій папці. Той самий текст видно в інтерфейсі
# Airflow на сторінці DAG-а у вкладці Docs — його підставляє doc_md нижче.
#
# Що показує: як із тих самих задач будувати різні схеми виконання.
# Найкраще дивитися цей DAG у вкладці Graph — там видно форму.

from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task


# Конспект із сусіднього README.md — показується у вкладці Docs в інтерфейсі
README = (Path(__file__).parent / "README.md").read_text(encoding="utf-8")


@dag(
    dag_id="demo_03_dependencies",
    doc_md=README,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo"],
)
def demo_03_dependencies():

    @task
    def start() -> None:
        print("початок")

    # Три задачі, які не залежать одна від одної — Airflow запустить їх одночасно
    @task
    def load_orders() -> int:
        print("вантажимо замовлення")
        return 120

    @task
    def load_customers() -> int:
        print("вантажимо клієнтів")
        return 45

    @task
    def load_products() -> int:
        print("вантажимо товари")
        return 30

    # Ця задача приймає результати всіх трьох, тому чекає, поки всі завершаться
    @task
    def summarize(orders: int, customers: int, products: int) -> None:
        print(f"разом записів: {orders + customers + products}")

    begin = start()

    orders = load_orders()
    customers = load_customers()
    products = load_products()

    # Стрілка без передавання даних: усі три чекають на begin
    begin >> [orders, customers, products]

    # А ця задача збирає три гілки в одну точку
    summarize(orders, customers, products)

    begin >> [orders, customers, products] >> summarize(orders, customers, products)



demo_03_dependencies()
