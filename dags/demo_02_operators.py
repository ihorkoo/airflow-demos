# ДЕМО 2. Класичні оператори.
# Теорія: docs/demo_02_operators.md
#
# Що показує: крім @task, задачі можна створювати з готових операторів.
# BashOperator виконує команду в терміналі, PythonOperator викликає функцію,
# EmptyOperator нічого не робить і потрібен як точка збірки в графі.

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag


def say_hello(name: str) -> None:
    """Звичайна функція — PythonOperator викличе її як задачу."""
    print(f"Привіт, {name}!")


@dag(
    dag_id="demo_02_operators",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo"],
)
def demo_02_operators():

    # task_id задається вручну — саме він видно в інтерфейсі
    start = EmptyOperator(task_id="start")

    show_date = BashOperator(
        task_id="show_date",
        bash_command="date",          # звичайна команда терміналу
    )

    count_files = BashOperator(
        task_id="count_files",
        bash_command="ls /opt/airflow/dags | wc -l",
    )

    greet = PythonOperator(
        task_id="greet",
        python_callable=say_hello,    # яку функцію викликати
        op_kwargs={"name": "Data Lab"},  # з якими аргументами
    )

    finish = EmptyOperator(task_id="finish")

    # Оператори не передають результат один одному, тому порядок
    # задається вручну стрілкою >>. Список у середині означає,
    # що ці дві задачі йдуть паралельно.
    start >> [show_date, count_files] >> greet >> finish


demo_02_operators()
