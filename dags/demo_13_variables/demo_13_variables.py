# ДЕМО 13. Variables — змінні Airflow.
# Теорія: README.md у цій папці. Той самий текст видно в інтерфейсі
# Airflow на сторінці DAG-а у вкладці Docs — його підставляє doc_md нижче.
#
# Що показує: значення, які не варто тримати в коді (назва схеми, ліміт
# рядків, шлях до папки), лежать в Airflow, а задача їх зчитує. Змінили
# значення в інтерфейсі — DAG працює по-новому, код не чіпали.
#
# Де їх заводять:
#   1) в інтерфейсі: Admin → Variables → плюс
#   2) через змінну середовища в docker-compose.yaml: AIRFLOW_VAR_ІМ'Я
#      (ім'я у великих літерах, у коді пишемо маленькими)
#
# ВАЖЛИВО: Variable.get не можна викликати на верхньому рівні файлу.
# Верхній рівень Airflow перечитує кожні кілька секунд — вийде постійний
# потік звернень до бази. Викликаємо тільки всередині задачі.

from datetime import datetime
from pathlib import Path

from airflow.sdk import Variable, dag, task


# Конспект із сусіднього README.md — показується у вкладці Docs в інтерфейсі
README = (Path(__file__).parent / "README.md").read_text(encoding="utf-8")


@dag(
    dag_id="demo_13_variables",
    doc_md=README,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo"],
)
def demo_13_variables():

    @task
    def read_simple() -> None:
        # default — що повернути, якщо змінної немає.
        # Без нього відсутня змінна валить задачу помилкою.
        # (в Airflow 2 цей параметр звався default_var — тут уже ні)
        schema = Variable.get("target_schema", default="public")
        limit = int(Variable.get("rows_limit", default="100"))
        print(f"схема: {schema}, ліміт рядків: {limit}")

    @task
    def read_json() -> None:
        # Одна змінна може тримати цілий словник налаштувань.
        # У полі Val в інтерфейсі пишемо JSON: {"email": "...", "retries": 2}
        settings = Variable.get(
            "report_settings",
            default={"email": "data@example.com", "retries": 1},
            deserialize_json=True,
        )
        print(f"кому слати: {settings['email']}, спроб: {settings['retries']}")

    @task
    def write_variable() -> None:
        # Змінну можна не лише читати, а й записувати з коду —
        # так зручно запам'ятовувати, де зупинилися минулого разу.
        Variable.set("last_run_marker", datetime.now().isoformat(timespec="seconds"))
        print(f"записали мітку: {Variable.get('last_run_marker')}")

    # Змінну видно і в шаблонах, без жодного Python-коду:
    #   bash_command="echo {{ var.value.target_schema }}"
    #   для JSON-змінної: "{{ var.json.report_settings.email }}"

    read_simple() >> read_json() >> write_variable()


demo_13_variables()
