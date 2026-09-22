# Airflow 3 — навчальні демо

Невеликий навчальний репозиторій з Apache Airflow **3.3.1**: 15 коротких DAG-ів від «hello world» до Assets, до кожного — конспект українською з поясненням механізму, довідкою параметрів і типовими помилками.

Середовище мінімальне: один Docker-контейнер, без Celery і Redis.

## Структура

```
.
├── docker-compose.yaml   # один контейнер Airflow (standalone)
├── dags/                 # 15 демо-DAG-ів, по одній темі на файл
└── docs/                 # конспект до кожного демо, починати з docs/README.md
```

| № | Тема | Файл |
|---|---|---|
| 1 | Базовий DAG і TaskFlow | [demo_01_basics.py](dags/demo_01_basics.py) |
| 2 | Класичні оператори | [demo_02_operators.py](dags/demo_02_operators.py) |
| 3 | Форма графа | [demo_03_dependencies.py](dags/demo_03_dependencies.py) |
| 4 | XCom — обмін даними | [demo_04_xcom.py](dags/demo_04_xcom.py) |
| 5 | Контекст, дати, шаблони | [demo_05_context.py](dags/demo_05_context.py) |
| 6 | Розгалуження | [demo_06_branching.py](dags/demo_06_branching.py) |
| 7 | Повтори при помилці | [demo_07_retries.py](dags/demo_07_retries.py) |
| 8 | Trigger rules | [demo_08_trigger_rules.py](dags/demo_08_trigger_rules.py) |
| 9 | Динамічні задачі | [demo_09_dynamic_tasks.py](dags/demo_09_dynamic_tasks.py) |
| 10 | Групи задач | [demo_10_task_groups.py](dags/demo_10_task_groups.py) |
| 11 | Сенсори | [demo_11_sensor.py](dags/demo_11_sensor.py) |
| 12 | Розклад і catchup | [demo_12_schedule.py](dags/demo_12_schedule.py) |
| 13 | Variables | [demo_13_variables.py](dags/demo_13_variables.py) |
| 14 | Connections і Postgres | [demo_14_postgres.py](dags/demo_14_postgres.py) |
| 15 | Assets — запуск за даними | [demo_15_assets.py](dags/demo_15_assets.py) |

Теорія до кожного демо — у [docs/README.md](docs/README.md).

## Розгортання

## Що потрібно

- Docker Desktop (Windows, macOS) або Docker Engine (Linux)
- виділено щонайменше 2 ГБ пам'яті на Docker — у Docker Desktop це Settings → Resources

## Запуск

Склонуйте репозиторій, відкрийте в ньому термінал і виконайте:

```bash
git clone https://github.com/ihorkoo/airflow-demos.git
cd airflow-demos
docker compose up -d
```

Перший запуск триває довше: Docker завантажує образ Airflow, а сам Airflow створює базу метаданих. Наступні запуски — кілька секунд.

Подивитися, що відбувається:

```bash
docker compose logs -f
```

Коли в логах з'явиться `Airflow is ready`, відкрийте **http://localhost:8080** — інтерфейс відкриється одразу, логін і пароль не потрібні.

## Щоденна робота

| Дія | Команда |
|-----|---------|
| запустити | `docker compose up -d` |
| зупинити | `docker compose stop` |
| подивитися логи | `docker compose logs -f` |
| перезапустити | `docker compose restart` |
| зупинити і видалити контейнер | `docker compose down` |
| видалити разом із базою метаданих | `docker compose down -v` |

Команда `docker compose down` контейнер прибирає, але базу лишає — усі ваші запуски DAG-ів збережуться. Команда з ключем `-v` видаляє й базу: після неї Airflow стартує з нуля.

## Куди класти DAG-и

Усі файли DAG-ів — у папку `dags` поруч із `docker-compose.yaml`. Вона підключена всередину контейнера, тому:

- новий файл з'явиться в інтерфейсі сам — папка пересканується раз на 5 хвилин
- зміни в наявному файлі підхоплюються так само, перезапускати контейнер не треба
- щоб не чекати, додайте в `docker-compose.yaml` рядок `AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL: "30"`
- якщо DAG не з'явився — дивіться `docker compose logs`, найчастіше там помилка імпорту

Решту Airflow пише всередину тому: база метаданих і логи запусків не засмічують вашу папку, а логи задач зручніше дивитися в інтерфейсі.

## Змінні і підключення

Дві речі, яких не має бути в коді DAG-а: налаштування, що змінюються (`Variables`), і доступи до баз (`Connections`). Обидві живуть в Airflow, а код звертається до них за іменем.

**Variables** — демо `demo_13_variables.py`. Заводяться в інтерфейсі: **Admin → Variables**, або рядком у `docker-compose.yaml`:

```yaml
AIRFLOW_VAR_TARGET_SCHEMA: "public"
```

Ім'я змінної середовища — великими літерами з префіксом `AIRFLOW_VAR_`, у коді пишемо маленькими: `Variable.get("target_schema")`.

**Connections** — демо `demo_14_postgres.py`. Логін і пароль до Postgres вписують в одному з двох місць.

В інтерфейсі: **Admin → Connections → плюс**

| Поле | Значення |
|------|----------|
| Connection Id | `demo_postgres` |
| Connection Type | Postgres |
| Host | адреса сервера |
| Database | назва бази |
| Login | ваш логін |
| Password | ваш пароль |
| Port | 5432 |

Або в `docker-compose.yaml`, у секції `environment` (рядок уже є, треба зняти коментар):

```yaml
AIRFLOW_CONN_DEMO_POSTGRES: "postgresql://логін:пароль@хост:5432/база"
```

Після правки файлу — `docker compose up -d`. Якщо в паролі є `@ : / ? #`, їх треба закодувати (`@` → `%40`, `#` → `%23`) — або просто завести підключення в інтерфейсі, там кодувати нічого не треба.

Перевірити підключення: **Admin → Connections**, відкрити своє і натиснути **Test**.

## Перевірка, що все працює

Після старту:

1. відкрийте http://localhost:8080
2. знайдіть у списку DAG `demo_01_basics` і зніміть паузу перемикачем ліворуч
3. натисніть кнопку запуску праворуч
4. відкрийте задачу `transform` і подивіться її логи — там має бути рядок `порахували суму: 100`

Якщо цей рядок є — середовище готове. Задача `load` у цьому демо падає навмисно: так одразу видно, як виглядає червона задача і де читати її логи.

## Типові проблеми

**Порт 8080 зайнятий.** Змініть у `docker-compose.yaml` рядок `"8080:8080"` на `"8081:8080"` і відкривайте http://localhost:8081.

**Контейнер стартує і одразу падає.** Майже завжди бракує пам'яті. Додайте Docker ресурсів у Settings → Resources.

**DAG не з'являється в списку.** Перевірте, що файл лежить саме в `dags` і має розширення `.py`, а потім подивіться `docker compose logs` — помилки імпорту видно там.

**Код із інтернету не працює.** Більшість туторіалів написані для Airflow 2, а тут третя версія. Основні відмінності: імпорти йдуть із `airflow.sdk`, а не з `airflow` чи `airflow.decorators`; оператори переїхали в провайдери, наприклад `airflow.providers.standard.operators.python`; параметр розкладу називається `schedule`, а не `schedule_interval`.

## Ліцензія

[MIT](LICENSE)
