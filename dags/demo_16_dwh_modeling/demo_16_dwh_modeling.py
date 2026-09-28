# ДЕМО 16. Моделювання DWH — зоряна схема на pandas.
# Теорія: README.md у цій папці. Той самий текст видно в інтерфейсі
# Airflow на сторінці DAG-а у вкладці Docs — його підставляє doc_md нижче.
#
# Що показує: невеликий, але справжній пайплайн. З двох CSV (бронювання
# готелів і довідник гостей) будуємо зоряну схему: 3 виміри + 2 таблиці
# фактів, перевіряємо її і рахуємо аналітику. Кожна таблиця — окрема
# задача, тому в графі видно головне правило: спершу виміри, потім факти.
#
# ВАЖЛИВО: самі таблиці через XCom не передаємо (див. демо 4). Задача пише
# файл на диск, а в XCom кладе лише шлях до нього.

from datetime import datetime
from pathlib import Path

from airflow.sdk import TaskGroup, dag, get_current_context, task

# pandas імпортуємо всередині задач, а не тут: цей файл Airflow перечитує
# постійно, і важкий імпорт гальмував би розбір усіх DAG-ів

SOURCE_DIR = Path(__file__).parent / "data"    # джерело: CSV поруч із DAG-ом
DWH_DIR = Path("/opt/airflow/data/dwh")        # готова модель
STAGING_DIR = DWH_DIR / "staging"              # почищені дані, проміжний шар

TRACKED = ["city", "country", "loyalty_tier"]  # зміна цих полів = нова версія гостя
FOREVER = "2100-12-31"                         # «діє досі» для SCD2


# Конспект із сусіднього README.md — показується у вкладці Docs в інтерфейсі
README = (Path(__file__).parent / "README.md").read_text(encoding="utf-8")


def read_table(path, dates=None):
    import pandas as pd

    # keep_default_na=False: інакше pandas прочитає "N/A" і порожній рядок як NaN
    return pd.read_csv(path, parse_dates=dates or [], keep_default_na=False)


def save_table(df, path: Path) -> str:
    """Пише таблицю на диск і повертає шлях — саме він поїде в XCom."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    print(f"{path.name}: {len(df)} рядків")
    return str(path)


def add_audit(df):
    """Службові колонки виміру: коли і яким запуском записано рядок."""
    dag_run = get_current_context()["dag_run"]
    df["dw_load_ts"] = dag_run.start_date.strftime("%Y-%m-%d %H:%M:%S")
    df["dw_loaded_by"] = dag_run.run_id
    return df


@dag(
    dag_id="demo_16_dwh_modeling",
    doc_md=README,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "dwh"],
)
def demo_16_dwh_modeling():

    # --- 1. Чистимо джерела ------------------------------------------------

    @task
    def clean_bookings() -> str:
        import pandas as pd

        bookings = pd.read_csv(SOURCE_DIR / "bookings.csv")

        print("дублікати booking_id:", bookings["booking_id"].duplicated().sum())
        print("порожній guest_code :", bookings["guest_code"].isna().sum())
        print("варіанти room_type  :", sorted(bookings["room_type"].unique()))

        bookings = bookings.drop_duplicates(subset="booking_id")
        bookings["room_type"] = bookings["room_type"].str.capitalize()
        # Порожній guest_code — не помилка, а бронювання без картки гостя.
        # Такі рядки отримають ключ -1 (рядок Unknown у вимірі).
        bookings["guest_code"] = bookings["guest_code"].fillna("")

        return save_table(bookings, STAGING_DIR / "bookings.csv")

    @task
    def clean_guests() -> str:
        import pandas as pd

        # довідник гостей приходить зрізами: той самий гість на різні дати
        guests = pd.read_csv(SOURCE_DIR / "guests.csv", parse_dates=["snapshot_date"])
        guests = guests.sort_values(["guest_code", "snapshot_date"])

        print("гостей:", guests["guest_code"].nunique())
        print("зрізи :", sorted(guests["snapshot_date"].dt.date.astype(str).unique()))

        return save_table(guests, STAGING_DIR / "guests.csv")

    # --- 2. Виміри ---------------------------------------------------------

    @task
    def build_dim_date(bookings_path: str) -> str:
        import pandas as pd

        bookings = read_table(bookings_path, dates=["booking_date", "checkout_date"])

        # Календар будуємо самі, щоб у ньому були всі дні поспіль
        dates = pd.date_range(bookings["booking_date"].min(), bookings["checkout_date"].max())

        dim_date = pd.DataFrame({"full_date": dates})
        dim_date["date_key"] = dim_date["full_date"].dt.strftime("%Y%m%d").astype(int)
        dim_date["year"] = dim_date["full_date"].dt.year
        dim_date["month"] = dim_date["full_date"].dt.month
        dim_date["year_month"] = dim_date["full_date"].dt.strftime("%Y-%m")
        dim_date["day_name"] = dim_date["full_date"].dt.day_name()
        dim_date["is_weekend"] = dim_date["full_date"].dt.dayofweek >= 5

        unknown = {"full_date": pd.Timestamp("1900-01-01"), "date_key": -1, "year": -1,
                   "month": -1, "year_month": "Unknown", "day_name": "Unknown",
                   "is_weekend": False}
        dim_date = pd.concat([pd.DataFrame([unknown]), dim_date], ignore_index=True)

        return save_table(add_audit(dim_date), DWH_DIR / "dim_date.csv")

    @task
    def build_dim_hotel(bookings_path: str) -> str:
        import pandas as pd

        bookings = read_table(bookings_path)

        dim_hotel = (bookings[["hotel_code", "hotel_name", "city", "country",
                               "stars", "hotel_rooms"]]
                     .drop_duplicates()
                     .sort_values("hotel_code")
                     .reset_index(drop=True))

        # hotel_key вигадуємо ми, hotel_code прийшов із джерела
        dim_hotel.insert(0, "hotel_key", range(1, len(dim_hotel) + 1))

        unknown = {"hotel_key": -1, "hotel_code": "N/A", "hotel_name": "Unknown",
                   "city": "Unknown", "country": "Unknown", "stars": -1, "hotel_rooms": 0}
        dim_hotel = pd.concat([pd.DataFrame([unknown]), dim_hotel], ignore_index=True)

        return save_table(add_audit(dim_hotel), DWH_DIR / "dim_hotel.csv")

    @task
    def build_dim_guest(guests_path: str) -> str:
        import pandas as pd

        guests = read_table(guests_path, dates=["snapshot_date"])
        guests = guests.sort_values(["guest_code", "snapshot_date"])

        # SCD Type 2: лишаємо перший запис гостя і записи, де щось змінилося
        previous = guests.groupby("guest_code")[TRACKED].shift(1)
        changed = guests[TRACKED].ne(previous).any(axis=1)
        dim_guest = guests.loc[changed].copy()

        # Період дії версії: від свого зрізу до наступної зміни
        forever = pd.Timestamp(FOREVER)
        dim_guest["valid_from"] = dim_guest["snapshot_date"]
        dim_guest["valid_to"] = (dim_guest.groupby("guest_code")["valid_from"]
                                 .shift(-1)
                                 .fillna(forever))
        dim_guest["is_current"] = dim_guest["valid_to"].eq(forever)

        dim_guest = dim_guest.drop(columns="snapshot_date")
        dim_guest.insert(0, "guest_key", range(1, len(dim_guest) + 1))

        unknown = {"guest_key": -1, "guest_code": "N/A", "first_name": "Unknown",
                   "last_name": "Unknown", "city": "Unknown", "country": "Unknown",
                   "loyalty_tier": "Unknown", "valid_from": pd.Timestamp("1900-01-01"),
                   "valid_to": forever, "is_current": True}
        dim_guest = pd.concat([pd.DataFrame([unknown]), dim_guest], ignore_index=True)

        print(f"{guests['guest_code'].nunique()} гостей → {len(dim_guest) - 1} версій")

        return save_table(add_audit(dim_guest), DWH_DIR / "dim_guest.csv")

    # --- 3. Факти ----------------------------------------------------------

    @task
    def build_fact_bookings(bookings_path: str, dim_guest_path: str, dim_hotel_path: str) -> str:
        """Один рядок = одне бронювання."""
        bookings = read_table(bookings_path, dates=["booking_date", "checkin_date"])
        dim_guest = read_table(dim_guest_path, dates=["valid_from", "valid_to"])
        dim_hotel = read_table(dim_hotel_path)

        # всі версії гостя приєднуємо до бронювань...
        b = bookings.merge(dim_guest[["guest_code", "guest_key", "valid_from", "valid_to"]],
                           on="guest_code", how="left")
        print("рядків після merge:", len(b))

        # ...і лишаємо ту версію, яка діяла на дату бронювання
        is_actual = (b["valid_from"] <= b["booking_date"]) & (b["booking_date"] < b["valid_to"])
        b = b[is_actual | b["guest_key"].isna()].copy()
        b["guest_key"] = b["guest_key"].fillna(-1).astype(int)

        print("рядків після фільтра:", len(b))
        print("з ключем Unknown:", (b["guest_key"] == -1).sum())

        hotel_keys = dict(zip(dim_hotel["hotel_code"], dim_hotel["hotel_key"]))
        b["hotel_key"] = b["hotel_code"].map(hotel_keys)

        # два ключі дат — обидва дивляться в один dim_date (role-playing)
        b["booking_date_key"] = b["booking_date"].dt.strftime("%Y%m%d").astype(int)
        b["checkin_date_key"] = b["checkin_date"].dt.strftime("%Y%m%d").astype(int)

        fact_bookings = b[[
            "booking_id",                              # вироджений вимір
            "guest_key", "hotel_key",                  # ключі вимірів
            "booking_date_key", "checkin_date_key",    # дві ролі календаря
            "room_type", "channel", "status",
            "nights", "guests_count", "total_amount",  # метрики
        ]].reset_index(drop=True)

        return save_table(fact_bookings, DWH_DIR / "fact_bookings.csv")

    @task
    def build_fact_monthly(bookings_path: str, dim_hotel_path: str) -> str:
        """Один рядок = готель × місяць, навіть якщо бронювань не було."""
        import pandas as pd

        bookings = read_table(bookings_path, dates=["checkin_date"])
        dim_hotel = read_table(dim_hotel_path)

        sold = bookings[bookings["status"] != "Cancelled"].copy()
        sold["year_month"] = sold["checkin_date"].dt.strftime("%Y-%m")

        agg = sold.groupby(["hotel_code", "year_month"], as_index=False).agg(
            bookings_count=("booking_id", "count"),
            room_nights=("nights", "sum"),
            revenue=("total_amount", "sum"))

        # повна сітка: кожен готель × кожен місяць
        codes = dim_hotel.loc[dim_hotel["hotel_key"] != -1, "hotel_code"]
        months = sorted(sold["year_month"].unique())
        grid = pd.DataFrame([(h, m) for h in codes for m in months],
                            columns=["hotel_code", "year_month"])

        fact_monthly = grid.merge(agg, on=["hotel_code", "year_month"], how="left").fillna(0)
        for c in ["bookings_count", "room_nights", "revenue"]:
            fact_monthly[c] = fact_monthly[c].astype(int)

        print("порожніх місяців:", (fact_monthly["bookings_count"] == 0).sum())

        hotel_keys = dict(zip(dim_hotel["hotel_code"], dim_hotel["hotel_key"]))
        fact_monthly["hotel_key"] = fact_monthly["hotel_code"].map(hotel_keys)

        # avg_check сумувати не можна — це середнє, його перераховують
        fact_monthly["avg_check"] = 0.0
        has_bookings = fact_monthly["bookings_count"] > 0
        fact_monthly.loc[has_bookings, "avg_check"] = (
            fact_monthly.loc[has_bookings, "revenue"]
            / fact_monthly.loc[has_bookings, "bookings_count"]).round(2)

        fact_monthly = fact_monthly[["hotel_key", "year_month", "bookings_count",
                                     "room_nights", "revenue", "avg_check"]]

        return save_table(fact_monthly, DWH_DIR / "fact_monthly.csv")

    # --- 4. Перевірка і аналітика ------------------------------------------

    @task
    def validate(bookings_path: str, dim_date_path: str, dim_hotel_path: str,
                 dim_guest_path: str, fact_bookings_path: str, fact_monthly_path: str) -> None:
        bookings = read_table(bookings_path)
        dim_date = read_table(dim_date_path)
        dim_hotel = read_table(dim_hotel_path)
        dim_guest = read_table(dim_guest_path)
        fact_bookings = read_table(fact_bookings_path)
        fact_monthly = read_table(fact_monthly_path)

        checks = {
            "жодне бронювання не загубилося": len(fact_bookings) == len(bookings),
            "один рядок на бронювання": fact_bookings["booking_id"].is_unique,
            "один рядок на готель×місяць":
                not fact_monthly.duplicated(["hotel_key", "year_month"]).any(),
            "усі guest_key є у вимірі":
                fact_bookings["guest_key"].isin(dim_guest["guest_key"]).all(),
            "усі hotel_key є у вимірі":
                fact_bookings["hotel_key"].isin(dim_hotel["hotel_key"]).all(),
            "усі date_key є у вимірі":
                fact_bookings["booking_date_key"].isin(dim_date["date_key"]).all()
                and fact_bookings["checkin_date_key"].isin(dim_date["date_key"]).all(),
            "виторг збігся":
                bookings["total_amount"].sum() == fact_bookings["total_amount"].sum(),
        }

        for name, passed in checks.items():
            print("OK  " if passed else "FAIL", name)

        # Червона задача зупиняє пайплайн: звіт по зламаній моделі не рахуємо
        failed = [name for name, passed in checks.items() if not passed]
        if failed:
            raise ValueError(f"модель не пройшла перевірку: {failed}")

    @task
    def report(dim_date_path: str, dim_hotel_path: str, dim_guest_path: str,
               fact_bookings_path: str, fact_monthly_path: str) -> None:
        """Заради цього все й будувалося: приєднати виміри до фактів і згрупувати."""
        dim_date = read_table(dim_date_path)
        dim_hotel = read_table(dim_hotel_path)
        dim_guest = read_table(dim_guest_path)
        fact_bookings = read_table(fact_bookings_path)
        fact_monthly = read_table(fact_monthly_path)

        q = (fact_bookings[fact_bookings["status"] != "Cancelled"]
             .merge(dim_date[["date_key", "year_month"]],
                    left_on="booking_date_key", right_on="date_key")
             .merge(dim_hotel[["hotel_key", "city"]], on="hotel_key"))
        by_city = q.groupby(["year_month", "city"], as_index=False)["total_amount"].sum()
        print("виторг за місяцями та містами:")
        print(by_city.head(8).to_string(index=False))

        # рівень лояльності НА МОМЕНТ бронювання — заради цього робили SCD2
        q = fact_bookings.merge(dim_guest[["guest_key", "loyalty_tier"]], on="guest_key")
        by_tier = (q.groupby("loyalty_tier", as_index=False)
                   .agg(bookings=("booking_id", "count"), revenue=("total_amount", "sum"))
                   .sort_values("revenue", ascending=False))
        print("виторг за рівнем лояльності:")
        print(by_tier.to_string(index=False))

        year = fact_monthly[fact_monthly["year_month"].str.startswith("2025")]
        print("сума avg_check за 2025 :", round(year["avg_check"].sum(), 2), "← нісенітниця")
        print("правильний середній чек:",
              round(year["revenue"].sum() / year["bookings_count"].sum(), 2))

    # --- Граф --------------------------------------------------------------
    # Залежності ніде не прописані стрілками: Airflow виводить їх сам із того,
    # чий результат яка задача отримує. TaskGroup — той самий блок, що й
    # @task_group з демо 10, просто у формі with.

    with TaskGroup(group_id="staging"):
        bookings = clean_bookings()
        guests = clean_guests()

    with TaskGroup(group_id="dimensions"):
        dim_date = build_dim_date(bookings)
        dim_hotel = build_dim_hotel(bookings)
        dim_guest = build_dim_guest(guests)

    # Факти беруть ключі з вимірів, тому стартують лише після них
    with TaskGroup(group_id="facts"):
        fact_bookings = build_fact_bookings(bookings, dim_guest, dim_hotel)
        fact_monthly = build_fact_monthly(bookings, dim_hotel)

    checked = validate(bookings, dim_date, dim_hotel, dim_guest, fact_bookings, fact_monthly)

    # Єдина стрілка: report не бере даних у validate, але має йти після неї
    checked >> report(dim_date, dim_hotel, dim_guest, fact_bookings, fact_monthly)


demo_16_dwh_modeling()
