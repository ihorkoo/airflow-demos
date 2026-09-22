# ДЕМО 8. Trigger rules — коли задача має запускатися.
# Теорія: docs/demo_08_trigger_rules.md
#
# Що показує: за замовчуванням задача чекає, поки ВСІ попередні завершаться
# успішно. Якщо хоч одна впала — наступні не запускаються взагалі.
# Trigger rule змінює цю умову: наприклад, прибирання тимчасових файлів
# має виконатися в будь-якому разі.
#
# УВАГА: задача load_data падає навмисно — у графі вона буде червона.
# При цьому весь DAG матиме статус success, бо Airflow дивиться на останні
# задачі в ланцюжку, а вони відпрацювали успішно. Це ще один корисний урок:
# зелений DAG не завжди означає, що всередині все пройшло гладко.

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="demo_08_trigger_rules",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo"],
)
def demo_08_trigger_rules():

    @task
    def prepare() -> None:
        print("створили тимчасову таблицю")
        raise ValueError("джерело віддало зіпсовані дані")

    @task(retries=0)
    def load_data() -> None:
        print("починаємо завантаження")
        raise ValueError("джерело віддало зіпсовані дані")


    # Без trigger_rule ця задача НЕ запустилася б, бо попередня впала
    @task(trigger_rule="all_done", retries=0)
    def cleanup() -> None:
        print("прибрали тимчасову таблицю — виконалося попри помилку вище")

    # Найчастіші правила:
    #   all_success (за замовчуванням) — усі попередні успішні
    #   all_done                       — усі попередні завершилися, байдуже як
    #   one_failed                     — хоча б одна впала
    #   none_failed_min_one_success    — жодна не впала, хоча б одна успішна
    #                                    (зручно після розгалуження)
    @task(trigger_rule="one_failed", retries=0)
    def send_alert() -> None:
        print("надсилаємо сповіщення команді про збій")

    step1 = prepare()
    step2 = load_data()

    step1 >> step2 >> [cleanup(), send_alert()]

demo_08_trigger_rules()
