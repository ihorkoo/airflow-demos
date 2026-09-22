# ДЕМО 11. Сенсор — задача, яка чекає на подію.
# Теорія: docs/demo_11_sensor.md
#
# Що показує: часто пайплайн не можна починати, поки не з'явиться файл
# або не відпрацює сусідня система. Сенсор періодично перевіряє умову
# і завершується успіхом, щойно вона виконалася.

from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task

# Файл, появи якого чекатиме сенсор
FLAG_FILE = Path("/tmp/demo_sensor_ready.txt")


@dag(
    dag_id="demo_11_sensor",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo"],
)
def demo_11_sensor():

    @task
    def create_file() -> None:
        """У житті цей файл клала б інша система — тут створюємо його самі."""
        FLAG_FILE.write_text("готово")
        print(f"файл створено: {FLAG_FILE}")

    # @task.sensor перетворює функцію на сенсор.
    # Функція має повертати True (умова виконалася) або False (чекаємо далі).
    @task.sensor(
        poke_interval=5,     # перевіряти кожні 5 секунд
        timeout=60,          # здатися через 60 секунд
        mode="reschedule",   # між перевірками звільняти слот, а не тримати його
    )
    def wait_for_file() -> bool:
        exists = FLAG_FILE.exists()
        print("файл на місці" if exists else "файлу ще немає, чекаємо")
        return exists

    @task
    def process() -> None:
        print(f"читаємо файл: {FLAG_FILE.read_text()}")
        FLAG_FILE.unlink(missing_ok=True)   # прибираємо за собою
        print("файл оброблено і видалено")

    create_file() >> wait_for_file() >> process()


demo_11_sensor()
