from datetime import date, datetime, timedelta
from airflow.sdk import BaseHook, Variable, dag, task

# 1. Константы
STUDENT = "hellenok"
PREFIX = "olkhovets_olena"

ENDPOINT = "/NBUStatService/v1/statdirectory/exchange"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# 2. Обычная функция запроса к API
def fetch_rates(day: date | None = None) -> list[dict]:
    """Курс на одну дату (None – на сегодня), только валюты из Variable."""
    from airflow.providers.http.hooks.http import HttpHook

    hook = HttpHook(method="GET", http_conn_id="nbu_api")
    params = "json" if day is None else f"date={day:%Y%m%d}&json"
    rates = hook.run(endpoint=f"{ENDPOINT}?{params}", headers=HEADERS).json()

    wanted = Variable.get("nbu_currencies", deserialize_json=True)
    return [
        {
            "currency": r["cc"],
            "rate": float(r["rate"]),
            "rate_date": datetime.strptime(r["exchangedate"], "%d.%m.%Y")
            .date()
            .isoformat(),
        }
        for r in rates
        if r["cc"] in wanted
    ]

# 3. Структура DAG-а
@dag(
    dag_id=f"{STUDENT}_nbu_rates",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["student", STUDENT],
)
def hellenok_nbu_rates():
    """DAG завантаження курсів НБУ за сьогодні або за період у Azure Blob Storage"""

    @task.branch
    def choose_mode() -> str:
        """Повертає task_id гілки завантаження відповідно до обраного режиму"""
        mode = Variable.get("nbu_load_mode", default="today")
        print(f"режим: {mode}")
        return "load_period" if mode == "period" else "load_today"

    @task
    def load_today() -> list[dict]:
        """Отримує та повертає список курсів обраних валют за сьогодні"""
        today_rates = fetch_rates(day=None)
        print(f"сьогодні: {len(today_rates)} рядків")
        return today_rates

    @task
    def load_period() -> list[dict]:
        """Отримує та повертає курси обраних валют за період включно з його межами"""
        start = date.fromisoformat(Variable.get("nbu_period_start"))
        end = date.fromisoformat(Variable.get("nbu_period_end"))
        rows = []
        day = start
        while day <= end:
            rows += fetch_rates(day)
            day += timedelta(days=1)
        print(f"період {start} — {end}: {len(rows)} рядків")
        return rows

    @task(trigger_rule="none_failed_min_one_success")
    def save_report(
        today_rows: list[dict] | None, period_rows: list[dict] | None
    ):
        """Зберігає курси з виконаної гілки у CSV в Blob Storage та виводить перелік файлів"""
        import pandas as pd
        from azure.storage.blob import ContainerClient

        rows = today_rows or period_rows or []
        if not rows:
            print("даних немає — нічого не записуємо")
            return

        # Инициализируем DataFrame и сразу сортируем
        df = pd.DataFrame(rows, columns=["currency", "rate", "rate_date"])
        df = df.sort_values(by=["rate_date", "currency"])

        # Определяем имя файла
        first_date = df["rate_date"].min()
        last_date = df["rate_date"].max()
        if first_date == last_date:
            file_name = f"nbu_rates_{first_date}.csv"
        else:
            file_name = f"nbu_rates_{first_date}_{last_date}.csv"

        blob_path = f"{PREFIX}/nbu_rates/{file_name}"
        print(f"Путь к файлу в Blob: {blob_path}")

        # --- ФИНАЛЬНЫЙ БЛОК ОТПРАВКИ В AZURE BLOB STORAGE ---
        # Получаем объект соединения через стандартный get_connection
        conn = BaseHook.get_connection("blob_lake")
        sas_url = conn.password

       
        container_client = ContainerClient.from_container_url(sas_url)

        # Переводим таблицу в CSV строку в памяти
        csv_data = df.to_csv(index=False)

        container_client.upload_blob(
            name=blob_path, data=csv_data, overwrite=True
        )
        print(f"записано {file_name}: {len(df)} рядків")

    # Порядок выполнения задач внутри DAG
    mode_choice = choose_mode()
    rates_today = load_today()
    rates_period = load_period()

    mode_choice >> [rates_today, rates_period]
    save_report(rates_today, rates_period)

hellenok_nbu_rates()

