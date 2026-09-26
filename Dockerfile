# Образ для Railway: той самий Airflow 3.3.1, що й у docker-compose.yaml,
# але DAG-и запечені прямо в образ. Кожен merge у main = нова збірка = новий деплой.
#
# Локально цей файл не потрібен: там працює docker-compose.yaml.

FROM apache/airflow:3.3.1

# без десятків прикладів Airflow — у списку лише DAG-и з цього репозиторію
ENV AIRFLOW__CORE__LOAD_EXAMPLES=false
# вхід тільки за паролем (користувачі й паролі — у railway-entrypoint.sh)
ENV AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=false

# DAG-и лежать в образі, поза томом: кожен новий образ приносить свіжий код
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
COPY --chown=airflow:0 dags/ /opt/airflow/dags/
COPY --chown=airflow:0 --chmod=755 railway-entrypoint.sh /railway-entrypoint.sh

# Том Railway належить root, а Airflow працює від користувача airflow.
# Тому скрипт стартує від root, виставляє власника тому і одразу переходить
# на airflow (див. railway-entrypoint.sh).
USER root

# Офіційний образ стартує через dumb-init + /entrypoint; підставляємо свій
# скрипт перед ним, решта поведінки не змінюється.
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/railway-entrypoint.sh"]
CMD ["standalone"]
