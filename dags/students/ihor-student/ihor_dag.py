# ДЕМО 1. Найпростіший DAG: три задачі одна за одною.
# Теорія: README.md у цій папці. Той самий текст видно в інтерфейсі
# Airflow на сторінці DAG-а у вкладці Docs — його підставляє doc_md нижче.
#
# Що показує: як оголосити DAG, як оголосити задачу і як Airflow сам
# вибудовує порядок виконання за тим, хто чий результат використовує.

from datetime import datetime
from pathlib import Path
from airflow.sdk import dag, task


# Конспект із сусіднього README.md — показується у вкладці Docs в інтерфейсі
README = (Path(__file__).parent / "README.md").read_text(encoding="utf-8")


@dag(
    dag_id="demo_01_basics",
    doc_md=README,
    start_date=datetime(2025, 1, 1),  # з якої дати Airflow рахує запуски
    schedule=None,                    # None = запускається тільки вручну
    catchup=False,                    # не доганяти пропущені дати
    tags=["demo"],
)
def demo_01_basics():

    # @task перетворює звичайну функцію на задачу Airflow.
    # Назва задачі в інтерфейсі = назва функції.
    @task
    def extract() -> list[int]:
        print("читаємо дані з джерела")
        return [10, 20, 30, 40]

    @task
    def transform(numbers: list[int]) -> int:
        total = sum(numbers)
        print(f"порахували суму: {total}")
        return total

    @task
    def load(total: int) -> None:
        # Навмисний збій: щоб побачити червону задачу і почитати її логи.
        # Після розбору цей рядок можна прибрати — DAG стане зеленим.
        raise EOFError
        print(f"записуємо результат у сховище: {total}")

    # Порядок задач Airflow визначає сам: transform чекає на extract,
    # бо приймає його результат, а load чекає на transform.
    numbers = extract()
    total = transform(numbers)
    load(total)


# Цей рядок обов'язковий: без виклику функції DAG не з'явиться в інтерфейсі.
demo_01_basics()