# ДЕМО 7. Повтори при помилці.
# Теорія: docs/demo_07_retries.md
#
# Що показує: мережа й бази падають, тому задачі не здаються з першого разу.
# Тут задача навмисно падає двічі й спрацьовує з третьої спроби.
# У логах буде видно всі три спроби, а підсумковий статус — success.

from datetime import datetime, timedelta

from airflow.sdk import dag, get_current_context, task


@dag(
    dag_id="demo_07_retries",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo"],
    # default_args діють на всі задачі DAG одразу
    default_args={
        "retries": 3,                            # скільки разів повторити
        "retry_delay": timedelta(seconds=10),    # пауза між спробами
    },
)
def demo_07_retries():

    @task
    def unstable_source() -> str:
        """Падає на першій і другій спробі, на третій повертає дані."""
        context = get_current_context()
        attempt = context["ti"].try_number       # номер поточної спроби, починається з 1

        print(f"спроба номер {attempt}")
        if attempt < 3:
            raise ConnectionError("з'єднання з джерелом розірвано")

        print("цього разу вдалося")
        return "дані отримано"

    # Окремій задачі можна задати власні налаштування, вони перекриють default_args
    @task(retries=0)
    def save(data: str) -> None:
        print(f"зберігаємо: {data}")

    save(unstable_source())


demo_07_retries()
