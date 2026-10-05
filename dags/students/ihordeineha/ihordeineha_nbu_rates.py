from datetime import datetime

from airflow.sdk import dag

STUDENT = "ihordeineha"


@dag(
    dag_id=f"{STUDENT}_nbu_rates",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["student", STUDENT],
)
def nbu_rates():
    pass


nbu_rates()