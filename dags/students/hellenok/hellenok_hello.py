# Шаблон студентського DAG-а. Скопіюйте цей файл у свою папку:
#   dags/students/<ваш_github_логін>/<ваш_github_логін>_<тема>.py
# і замініть усі місця, позначені ЗМІНИТИ.

from datetime import datetime

from airflow.sdk import dag, task

# ЗМІНИТИ: ваш GitHub-логін (маленькими літерами, без пробілів)
STUDENT = "hellenok"


@dag(
    # dag_id має бути унікальним на весь Airflow, тому починається з вашого логіна
    dag_id=f"{STUDENT}_first_dag",  # ЗМІНИТИ: _hello -> тема вашого DAG-а
    start_date=datetime(2026, 1, 1),
    schedule=None,       # None = запускається тільки вручну
    catchup=False,
    tags=["student", STUDENT],  # за міткою зручно знайти свій DAG у списку
)
def student_dag():

    @task
    def extract() -> list[int]:
        print("читаємо дані")
        return [1, 2, 3]

    @task
    def transform(numbers: list[int]) -> int:
        total = sum(numbers)
        print(f"сума: {total}")
        return total

    transform(extract())


student_dag()
