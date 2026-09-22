# ДЕМО 5. Контекст запуску і дати.
# Теорія: docs/demo_05_context.md
#
# Що показує: кожен запуск DAG знає свою дату. Саме за нею завантажують
# «дані за вчора», а не за поточним часом на сервері — тому перезапуск
# старого дня дає той самий результат.

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, get_current_context, task


@dag(
    dag_id="demo_05_context",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo"],
)
def demo_05_context():

    @task
    def show_context() -> None:
        # get_current_context дає словник із даними про поточний запуск
        context = get_current_context()

        print("dag_id:", context["dag"].dag_id)
        print("task_id:", context["ti"].task_id)
        print("номер спроби:", context["ti"].try_number)
        print("тип запуску:", context["dag_run"].run_type)   # manual або scheduled
        print("логічна дата:", context["logical_date"])       # дата, за яку рахуємо дані

    # Другий спосіб дістатися дати — шаблон у подвійних дужках.
    # Airflow підставляє значення перед запуском задачі.
    # {{ ds }} це логічна дата у форматі РРРР-ММ-ДД.
    show_with_template = BashOperator(
        task_id="show_with_template",
        bash_command='echo "дата запуску: {{ ds }}"',
    )

    @task
    def build_path() -> str:
        """Типове застосування: скласти шлях до файлу за датою запуску."""
        context = get_current_context()
        date = context["logical_date"]
        # Якщо DAG запустили вручну без дати, logical_date може бути порожньою
        day = date.strftime("%Y-%m-%d") if date else "no-date"
        path = f"/data/orders/{day}.csv"
        print(f"працюємо з файлом: {path}")
        return path

    show_context() >> show_with_template >> build_path()


demo_05_context()
