# ДЕМО 9. Динамічні задачі.
# Теорія: docs/demo_09_dynamic_tasks.md
#
# Що показує: кількість задач не завжди відома наперед. Airflow вміє
# створити стільки копій задачі, скільки елементів прийшло у списку.
# У графі це буде одна задача з лічильником, а всередині — окремі запуски.

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="demo_09_dynamic_tasks",
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
