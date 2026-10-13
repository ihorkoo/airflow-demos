# DAG, який забирає курс валют з API НБУ і обирає вивантажити курс за сьогодні або за цілий період

from datetime import date, datetime, timedelta
from airflow.sdk import BaseHook, Variable, dag, task


STUDENT = "viktoriiatyshyk"        # GitHub-логін 
PREFIX="tyshyk_viktoriia"          #папка в Blob Storage

ENDPOINT = "/NBUStatService/v1/statdirectory/exchange"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


# Функція отримання курсу валют на одну дату
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


# DAG завантаження і вивантаження курсу валют
@dag(
    dag_id=f"{STUDENT}_nbu_rates",  
    start_date=datetime(2025, 1, 1),
    schedule=None,       # None = запускається тільки вручну
    catchup=False,
    tags=["student", STUDENT],  
)
def viktoriiatyshyk_nbu_rates():

    #Розгалуження на завантаження за сьогодні або за період; повертає task_id задачі, яка виконується далі
    @task.branch
    def choose_mode():
        mode= Variable.get("nbu_load_mode", default="today")
        print(f"режим: {mode}")
        return "load_period" if mode=="period" else "load_today"

    #Завдання завантаження курсу валют за сьогодні
    @task
    def load_today():
        today_rows=fetch_rates(day=None)
        print(f"сьогодні: {len(today_rows)} рядків")
        return today_rows

    #Завдання завантаження курсу валют за період
    @task
    def load_period() -> list[dict]:
        start = date.fromisoformat(Variable.get("nbu_period_start"))
        end = date.fromisoformat(Variable.get("nbu_period_end"))
        rows = []
        day = start
        while day <= end:
            rows += fetch_rates(day)
            day += timedelta(days=1)
        print(f"період {start} — {end}: {len(rows)} рядків")
        return rows

    #Завдання збереження результату в Azure Blob Storage; виводить список файлів у контейнері після запису
    @task(trigger_rule="none_failed_min_one_success")
    def save_report(today_rows: list[dict] | None, period_rows: list[dict] | None) -> None:
        import pandas as pd
        from azure.storage.blob import ContainerClient

        rows = today_rows or period_rows or []
        if not rows:
            print("даних немає — нічого не записуємо")
            return
        df = pd.DataFrame(rows).sort_values(["rate_date", "currency"])

        # назва файлу: одна дата — за день, дві — за період
        first, last = df["rate_date"].min(), df["rate_date"].max()
        suffix = first if first == last else f"{first}_{last}"
        blob_name = f"{PREFIX}/nbu_rates/nbu_rates_{suffix}.csv"

        # SAS-посилання на контейнер — секрет, тому лежить у Connection
        sas_url = BaseHook.get_connection("blob_lake").password
        lake = ContainerClient.from_container_url(sas_url)
        # overwrite=True: повторний запуск за ті самі дати перезаписує файл
        lake.upload_blob(name=blob_name, data=df.to_csv(index=False).encode("utf-8"), overwrite=True)
        print(f"записано {blob_name}: {len(df)} рядків")

        for blob in lake.list_blobs(name_starts_with=f"{PREFIX}/nbu_rates/"):
            print(f"  {blob.name} {blob.size} байт")

    #Визначення послідовності виконання завдань
    mode = choose_mode()
    today_rows = load_today()
    period_rows = load_period()
    mode >> [today_rows, period_rows]
    save_report(today_rows, period_rows)


viktoriiatyshyk_nbu_rates()
