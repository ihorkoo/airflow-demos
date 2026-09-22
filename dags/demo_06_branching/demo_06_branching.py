# ДЕМО 6. Розгалуження — виконати одну гілку з кількох.
# Теорія: README.md у цій папці. Той самий текст видно в інтерфейсі
# Airflow на сторінці DAG-а у вкладці Docs — його підставляє doc_md нижче.
#
# Що показує: задача-розгалужувач повертає task_id тієї гілки, яку треба
# виконати. Решта гілок отримають статус skipped (пропущено) — у графі
# вони будуть блідо-рожеві.

from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task


# Конспект із сусіднього README.md — показується у вкладці Docs в інтерфейсі
README = (Path(__file__).parent / "README.md").read_text(encoding="utf-8")


@dag(
    dag_id="demo_06_branching",
    doc_md=README,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo"],
)
def demo_06_branching():

    @task
    def count_rows() -> int:
        rows = 1500
        print(f"у вивантаженні рядків: {rows}")
        return rows

    # @task.branch замість значення повертає НАЗВУ задачі, яку виконувати далі
    @task.branch
    def choose_path(rows: int) -> str:
        if rows == 0:
            return "no_data"
        if rows < 1000:
            return "small_load"
        return "big_load"

    @task
    def no_data() -> None:
        print("даних немає, нічого не робимо")

    @task
    def small_load() -> None:
        print("маленьке завантаження — вантажимо одним шматком")

    @task
    def big_load() -> None:
        print("велике завантаження — вантажимо частинами")

    rows = count_rows()
    branch = choose_path(rows)

    # Усі три гілки чіпляються до розгалужувача, але виконається лише одна
    
    branch >> [no_data(), small_load(), big_load()]


demo_06_branching()
