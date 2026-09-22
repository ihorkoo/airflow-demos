# ДЕМО 0. Архітектура Airflow 3: де насправді виконується ваш код.
# Теорія: README.md у цій папці. Той самий текст видно в інтерфейсі
# Airflow на сторінці DAG-а у вкладці Docs — його підставляє doc_md нижче.
#
# Що показує: одна задача, яка друкує версію Airflow, ім'я машини,
# номер свого процесу і ключі контексту. Порівняйте це з виводом
# `docker compose top` — задача живе в окремому процесі, не в scheduler-і.

import os
import socket
from datetime import datetime
from pathlib import Path

import airflow
from airflow.sdk import __version__ as sdk_version
from airflow.sdk import dag, get_current_context, task

# Конспект із сусіднього README.md — показується у вкладці Docs в інтерфейсі
README = (Path(__file__).parent / "README.md").read_text(encoding="utf-8")


@dag(
    dag_id="demo_00_architecture",
    doc_md=README,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo"],
)
def demo_00_architecture():

    @task
    def where_am_i() -> None:
        context = get_current_context()
        print(f"Airflow          : {airflow.__version__}")
        print(f"Task SDK         : {sdk_version}")
        print(f"машина           : {socket.gethostname()}")
        print(f"процес задачі    : pid={os.getpid()}, батько pid={os.getppid()}")
        print(f"DAG / задача     : {context['dag'].dag_id} / {context['ti'].task_id}")
        print(f"спроба           : {context['ti'].try_number}")
        print(f"ключі контексту  : {sorted(context.keys())}")

    where_am_i()


demo_00_architecture()
