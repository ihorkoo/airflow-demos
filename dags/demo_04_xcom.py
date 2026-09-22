# ДЕМО 4. XCom — як задачі передають дані одна одній.
# Теорія: docs/demo_04_xcom.md
#
# Що показує: XCom це маленьке сховище всередині Airflow. Кожна задача
# може покласти туди значення, а наступна — забрати. У TaskFlow це
# відбувається саме собою, але корисно побачити механізм явно.
#
# ВАЖЛИВО: XCom не призначений для великих даних. Туди кладуть числа,
# короткі рядки, шляхи до файлів — але не самі таблиці.

from datetime import datetime

from airflow.sdk import dag, get_current_context, task


@dag(
    dag_id="demo_04_xcom",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo"],
)
def demo_04_xcom():

    # Спосіб 1: звичайний return — Airflow сам кладе значення в XCom
    @task
    def count_rows() -> int:
        rows = 1500
        print(f"порахували рядків: {rows}")
        return rows

    @task
    def report(rows: int) -> None:
        # Значення приїхало сюди через XCom, хоча в коді це схоже на звичайний виклик
        print(f"отримали через XCom: {rows}")

    # Спосіб 2: покласти кілька значень під різними іменами
    @task
    def push_many() -> None:
        context = get_current_context()
        ti = context["ti"]                      # ti = task instance, поточний запуск задачі
        ti.xcom_push(key="file_path", value="/data/orders.csv")
        ti.xcom_push(key="file_size", value=2048)
        print("поклали два значення в XCom")

    @task
    def pull_many() -> None:
        context = get_current_context()
        ti = context["ti"]
        # Забираємо за іменем ключа і назвою задачі, яка його поклала
        path = ti.xcom_pull(task_ids="push_many", key="file_path")
        size = ti.xcom_pull(task_ids="push_many", key="file_size")
        print(f"забрали з XCom: файл {path}, розмір {size}")

    report(count_rows())
    
    push_many() >> pull_many()


demo_04_xcom()
