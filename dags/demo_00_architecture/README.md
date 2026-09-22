# Демо 0. Архітектура Airflow 3

Файл: [`demo_00_architecture.py`](demo_00_architecture.py) — одна задача, яка друкує, де саме вона виконується.

## Ідея

Airflow — це **оркестратор**, а не обчислювальний движок. Він відповідає на три питання: *що* запускати, *коли* і *в якому порядку*, а також стежить, чи все відпрацювало. Сама важка робота (SQL у сховищі, Spark-джоба, виклик API) має відбуватися деінде; задача Airflow — лише дати команду і дочекатися результату.

Тому Airflow складається не з однієї програми, а з кількох сервісів, які спілкуються через базу даних і HTTP. Розуміти, хто з них що робить, потрібно вже з першого DAG-а: без цього незрозуміло, чому файл «не з'являється», чому змінна в коді «не оновилась» і де шукати логи.

## Компоненти

| Компонент | Команда | Що робить |
|---|---|---|
| **API server** | `airflow api-server` | Віддає веб-інтерфейс, REST API (`/api/v2/...`) і **Execution API** — внутрішній канал, через який задачі отримують контекст, Variables, Connections і звітують про стан. У Airflow 2 це був `webserver` |
| **Scheduler** | `airflow scheduler` | Дивиться на розклади, створює запуски (DagRun), вирішує, які задачі готові, і віддає їх executor-у |
| **DAG processor** | `airflow dag-processor` | Читає файли з папки `dags`, виконує їх як Python і зберігає результат у базу в серіалізованому вигляді. В Airflow 3 це обов'язковий окремий сервіс |
| **Triggerer** | `airflow triggerer` | Один процес з asyncio-циклом, у якому «сплять» тисячі відкладених задач (deferrable operators, сенсори в режимі `reschedule`), не займаючи воркерів |
| **Executor** | налаштування, не сервіс | Стратегія, *як* запускати задачі: `LocalExecutor` — підпроцеси на тій самій машині, `CeleryExecutor` — черга й окремі воркери, `KubernetesExecutor` — под на кожну задачу |
| **Worker** | залежить від executor-а | Процес, у якому виконується код вашої задачі. Свій для кожної задачі |
| **Metadata DB** | Postgres, MySQL, SQLite | Єдине джерело правди: серіалізовані DAG-и, запуски, стани задач, XCom, Variables, Connections. SQLite годиться лише для навчання |

У нашому `docker-compose.yaml` команда `standalone` запускає перші чотири сервіси одним процесом, executor — `LocalExecutor`, база — SQLite у docker-томі. Це той самий Airflow, тільки в одному контейнері.

## Шлях одного запуску

1. Ви кладете файл у `dags/`. **DAG processor** за кілька секунд виконує його, будує граф і записує в базу. Помилка імпорту зупиняє процес тут, і DAG в інтерфейсі не з'явиться.
2. Ви натискаєте «запустити». **API server** створює в базі запуск (DagRun) зі станом `queued`.
3. **Scheduler** бачить новий запуск, створює екземпляри задач (TaskInstance) і ті з них, у кого всі залежності виконані, віддає **executor-у**.
4. **Executor** стартує процес-супервізор, а той запускає ваш код у дочірньому процесі. Це і є **worker**. Усе, що задачі треба від сервера (контекст, `Variable.get`, підключення, запис XCom, зміна стану), іде через супервізор по HTTP до **Execution API**. До бази задача не торкається.
5. Задача завершилась, стан пішов у базу. Scheduler бачить це і віддає наступні задачі графа.
6. Логи задачі worker пише у файл; інтерфейс через API server показує їх вам.

Запустіть DAG цього демо і подивіться логи: там буде `pid` процесу задачі. Порівняйте з `docker compose top` — це не процес scheduler-а й не процес api-server-а, а окремий, який жив лише поки виконувалась задача.

## Що змінилося в архітектурі Airflow 3

- **Задачі відрізані від бази.** У Airflow 2 код задачі міг відкрити сесію до Metadata DB і читати що завгодно. В Airflow 3 воркер спілкується лише з Execution API. Тому з'явився окремий пакет для авторів DAG-ів, **Task SDK**, і тому старий код з `from airflow.models import ...` усередині задач може не працювати.
- **DAG processor обов'язковий** і окремий від scheduler-а. Помилки парсингу не впливають на планування.
- **Версіонування DAG-ів.** Кожен новий варіант файлу зберігається як версія. Інтерфейс показує запуск таким, яким був граф на момент запуску, а не яким він став після правки.
- **DAG bundles.** Папка `dags` тепер лише один із можливих «пакунків» з DAG-ами (`dags-folder`). Іншим може бути git-репозиторій, який Airflow сам підтягує.
- **Assets** замість Datasets: DAG запускається за готовністю даних, не за годинником ([демо 15](../demo_15_assets/README.md)).
- **Новий інтерфейс** на React і API на FastAPI. Документація до REST API живе за адресою `/docs` вашого сервера: http://localhost:8080/docs.

## Task SDK: `airflow.sdk`

Це єдиний публічний і стабільний інтерфейс для авторів DAG-ів. Він живе в окремому пакеті `apache-airflow-task-sdk` зі своєю версією (у нас 1.3.1 при Airflow 3.3.1). Усе, що потрібно для написання DAG-а, імпортують звідси. Решта модулів `airflow.*` — серверна частина, її можуть змінювати без попередження.

Найкорисніше з `airflow.sdk`, згруповано:

| Група | Імена | Де в демо |
|---|---|---|
| Оголосити граф | `dag`, `task`, `task_group`, `DAG`, `TaskGroup` | [1](../demo_01_basics/README.md), [10](../demo_10_task_groups/README.md) |
| Звичайні варіанти `@task` | `@task.bash`, `@task.branch`, `@task.sensor`, `@task.short_circuit` | [6](../demo_06_branching/README.md), [11](../demo_11_sensor/README.md) |
| Залежності вручну | `chain`, `chain_linear`, `cross_downstream`, `Label` | [3](../demo_03_dependencies/README.md) |
| Контекст і параметри | `get_current_context`, `Context`, `Param`, `XComArg` | [4](../demo_04_xcom/README.md), [5](../demo_05_context/README.md) |
| Налаштування і доступи | `Variable`, `Connection`, `conf` | [13](../demo_13_variables/README.md), [14](../demo_14_postgres/README.md) |
| Дані як тригер | `Asset`, `asset`, `AssetAlias`, `Metadata`, `AssetAll`, `AssetAny` | [15](../demo_15_assets/README.md) |
| Базові класи для своїх операторів | `BaseOperator`, `BaseSensorOperator`, `BaseHook`, `BaseNotifier` | — |
| Правила запуску | `TriggerRule`, `WeightRule`, `setup`, `teardown` | [8](../demo_08_trigger_rules/README.md) |
| Розклади | `CronTriggerTimetable`, `CronDataIntervalTimetable`, `DeltaTriggerTimetable`, `EventsTimetable`, `AssetOrTimeSchedule` | [12](../demo_12_schedule/README.md) |
| Файли в хмарі як шляхи | `ObjectStoragePath` | — |

Повний список імен можна побачити прямо з контейнера:

```bash
docker compose exec airflow python -c "import airflow.sdk as s; print(sorted(s.__all__))"
```

## Інші частини бібліотеки

**Провайдери, `airflow.providers.*`.** Оператори, сенсори й хуки для конкретних систем живуть в окремих пакетах. В Airflow 3 навіть базові речі переїхали в провайдер `standard`:

| Модуль | Що там |
|---|---|
| `airflow.providers.standard.operators.python` | `PythonOperator`, `BranchPythonOperator`, `ShortCircuitOperator` |
| `airflow.providers.standard.operators.bash` | `BashOperator` |
| `airflow.providers.standard.operators.empty` | `EmptyOperator` |
| `airflow.providers.standard.operators.trigger_dagrun` | `TriggerDagRunOperator` — запустити інший DAG |
| `airflow.providers.standard.operators.latest_only` | `LatestOnlyOperator` — пропускати задачі при catchup |
| `airflow.providers.standard.sensors.*` | `FileSensor`, `PythonSensor`, `TimeDeltaSensor`, `DateTimeSensor`, `ExternalTaskSensor` |
| `airflow.providers.common.sql` | `SQLExecuteQueryOperator` — універсальний SQL для будь-якої бази |
| `airflow.providers.postgres` | `PostgresHook` і підключення типу Postgres |
| `airflow.providers.http`, `amazon`, `google`, `microsoft.azure`, ... | усе інше, ставиться окремо |

Який провайдер потрібен, підказує документація оператора; у Docker-образ вже входить кілька найпоширеніших. Перевірити, що встановлено: `docker compose exec airflow airflow providers list`.

**Пакети.** `apache-airflow` — це метапакет. Усередині: `apache-airflow-core` (scheduler, API server, база), `apache-airflow-task-sdk` (те, що імпортують DAG-и) і мінімальний набір провайдерів.

**Серверні модулі: `airflow.models`, `airflow.utils`, `airflow.jobs`.** Ними користується сам Airflow. У коді DAG-ів їх не імпортують: усе, що потрібно, вже є в `airflow.sdk`, а прямий доступ до бази з задачі в Airflow 3 не працює.

**CLI, команда `airflow`.** Усе, що робить інтерфейс, можна зробити з терміналу контейнера. Найкорисніше під час навчання:

| Команда | Навіщо |
|---|---|
| `airflow dags list` | Які DAG-и бачить Airflow |
| `airflow dags list-import-errors` | Чому DAG не з'явився |
| `airflow dags trigger demo_01_basics` | Запустити з терміналу |
| `airflow tasks test demo_01_basics extract` | Виконати одну задачу тут і зараз, без scheduler-а і без запису в базу. Найшвидший спосіб налагодити код |
| `airflow config get-value core executor` | Подивитися будь-яке налаштування |
| `airflow providers list` | Які провайдери встановлені |

Запускати через `docker compose exec airflow <команда>`.

## Типові помилки

| Симптом | Причина |
|---|---|
| `ModuleNotFoundError: airflow.operators.bash` | Код для Airflow 2. Оператори переїхали в `airflow.providers.standard.operators.*` |
| `cannot import name 'dag' from 'airflow.decorators'` | Те саме: в Airflow 3 імпорт із `airflow.sdk` |
| Усередині задачі `from airflow.models import Variable` дає помилку або зависає | Задача не має доступу до бази. Беріть `Variable` з `airflow.sdk` |
| Поміняли Variable, а задача бачить старе значення | Значення читається під час *виконання* задачі, а не парсингу. Якщо `Variable.get` стоїть на верхньому рівні файлу, його виконує DAG processor і кешує до наступного парсингу |
| Змінна з файлу «зникає» між задачами | Кожна задача — окремий процес. Пам'ять не спільна, дані передають через XCom ([демо 4](../demo_04_xcom/README.md)) |
| DAG виконує важкі обчислення в pandas і все гальмує | Airflow — оркестратор. Обчислення мають іти в сховище чи движок, задача лише дає команду |

## Що спробувати

- Запустити DAG, знайти в логах `pid` і порівняти з `docker compose top airflow`: цього процесу вже нема, він жив лише під час задачі.
- Виконати задачу без scheduler-а: `docker compose exec airflow airflow tasks test demo_00_architecture where_am_i`. Зверніть увагу, що запуск в інтерфейсі не з'явиться.
- Відкрити http://localhost:8080/docs і знайти там ендпоінт, який повертає список DAG-ів. Викликати його: `curl http://localhost:8080/api/v2/dags`.
- Додати на верхньому рівні файлу `print("парсинг")` і подивитися в `docker compose logs -f`, як часто DAG processor перечитує файл.
