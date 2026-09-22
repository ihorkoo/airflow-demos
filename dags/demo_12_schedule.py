# ДЕМО 12. Розклад і catchup.
# Теорія: docs/demo_12_schedule.md
#
# Що показує: цей DAG — єдиний у наборі, що має розклад. Він працює сам,
# раз на добу, без натискання кнопки.
#
# catchup=False означає: не доганяти всі дати від start_date до сьогодні.
# Якщо поставити catchup=True і start_date рік тому, Airflow одразу
# створить 365 запусків — так ламають собі середовище найчастіше.

from datetime import datetime

from airflow.sdk import dag, get_current_context, task

# Варіанти розкладу:
#   None            — тільки вручну
#   "@daily"        — щодня опівночі
#   "@hourly"       — щогодини
#   "@weekly"       — щопонеділка опівночі
#   "0 6 * * *"     — cron: щодня о 06:00
#   "0 9 * * 1-5"   — cron: о 09:00 з понеділка по п'ятницю


@dag(
    dag_id="demo_12_schedule",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",     # запускається раз на добу
    catchup=False,         # починаємо з поточного дня, минуле не доганяємо
    max_active_runs=1,     # не більше одного запуску одночасно
    tags=["demo"],
)
def demo_12_schedule():

    @task
    def daily_report() -> None:
        context = get_current_context()
        date = context["logical_date"]
        day = date.strftime("%Y-%m-%d") if date else "вручну, без дати"
        print(f"готуємо звіт за {day}")

    daily_report()


demo_12_schedule()
