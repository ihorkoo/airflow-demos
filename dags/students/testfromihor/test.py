from datetime import date, datetime, timedelta

from airflow.sdk import BaseHook, Variable, dag, task

STUDENT = "ihordeineha"
PREFIX = "deineha_ihor"

ENDPOINT = "/NBUStatService/v1/statdirectory/exchange"
# без User-Agent НБУ інколи відповідає 403
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def fetch_rates(day: date | None = None) -> list[dict]:
    """Курс на одну дату (None — на сьогодні), лише валюти з Variable."""
    from airflow.providers.http.hooks.http import HttpHook

    # хост і протокол HttpHook бере з Connection nbu_api
    hook = HttpHook(method="GET", http_conn_id="nbu_api")
    params = "json" if day is None else f"date={day:%Y%m%d}&json"
    rates = hook.run(endpoint=f"{ENDPOINT}?{params}", headers=HEADERS).json()

    wanted = Variable.get("nbu_currencies", deserialize_json=True)
    return [
        {
            "currency": r["cc"],
            "rate": float(r["rate"]),
            "rate_date": datetime.strptime(r["exchangedate"], "%d.%m.%Y").date().isoformat(),
        }
        for r in rates
        if r["cc"] in wanted
    ]


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


def rates():
    pass
rates()

