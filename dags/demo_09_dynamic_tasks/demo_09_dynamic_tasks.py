# ДЕМО 9. Динамічні задачі.
# Теорія: README.md у цій папці. Той самий текст видно в інтерфейсі
# Airflow на сторінці DAG-а у вкладці Docs — його підставляє doc_md нижче.
#
# Що показує: кількість задач не завжди відома наперед. Airflow вміє
# створити стільки копій задачі, скільки елементів прийшло у списку.
# У графі це буде одна задача з лічильником, а всередині — окремі запуски.

from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task


# Конспект із сусіднього README.md — показується у вкладці Docs в інтерфейсі
README = (Path(__file__).parent / "README.md").read_text(encoding="utf-8")


@dag(
    dag_id="demo_09_dynamic_tasks",
    doc_md=README,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo"],
)
def demo_09_dynamic_tasks():

    @task
    def list_files() -> list[str]:
        """У реальному житті список файлів приходить із папки чи з API."""
        files = ["orders.csv", "customers.csv", "products.csv", "stores.csv"]
        print(f"знайшли файлів: {len(files)}")
        return files

    @task
    def process_file(file_name: str) -> int:
        """Ця задача виконається окремо для кожного файлу зі списку."""
        rows = len(file_name) * 10          # умовна кількість рядків
        print(f"обробили {file_name}: {rows} рядків")
        return rows

    @task
    def total(results: list[int]) -> None:
        """Отримує список результатів усіх копій задачі."""
        print(f"копій відпрацювало: {len(results)}, разом рядків: {sum(results)}")

    # .expand() створює по одній копії задачі на кожен елемент списку
    results = process_file.expand(file_name=list_files())

    total(results)


demo_09_dynamic_tasks()
