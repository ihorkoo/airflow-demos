#!/usr/bin/env bash
# Готує налаштування, специфічні для Railway, і передає керування штатному
# entrypoint Airflow. Запускається як користувач airflow.
set -euo pipefail

# Railway підказує порт через змінну PORT; Airflow слухає його
export AIRFLOW__API__PORT="${PORT:-8080}"

# Публічний URL без пароля неприпустимий: пароль обов'язковий
: "${AIRFLOW_ADMIN_PASSWORD:?Задайте змінну AIRFLOW_ADMIN_PASSWORD у Railway (Variables)}"
: "${AIRFLOW_STUDENT_PASSWORD:?Задайте змінну AIRFLOW_STUDENT_PASSWORD у Railway (Variables)}"

# admin — викладач, student — спільний вхід для студентів (може запускати DAG-и)
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS="admin:admin,student:user"
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE="/opt/airflow/simple_auth_manager_passwords.json"

python3 - <<'PY'
import json, os

path = os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"]
passwords = {
    "admin": os.environ["AIRFLOW_ADMIN_PASSWORD"],
    "student": os.environ["AIRFLOW_STUDENT_PASSWORD"],
}
with open(path, "w", encoding="utf-8") as f:
    json.dump(passwords, f)
os.chmod(path, 0o600)
PY

exec /entrypoint "$@"
