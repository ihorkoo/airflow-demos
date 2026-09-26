#!/usr/bin/env bash
# Готує налаштування, специфічні для Railway, і передає керування штатному
# entrypoint Airflow. Стартує від root (потрібно для тому), далі працює як airflow.
set -euo pipefail

# Де зберігати базу і логи. Якщо до сервісу підключено том Railway, Railway
# передає шлях до нього в RAILWAY_VOLUME_MOUNT_PATH: тоді SQLite і логи
# переживають деплої. Без тому все лишається в контейнері (як було раніше).
if [ -n "${RAILWAY_VOLUME_MOUNT_PATH:-}" ]; then
  export AIRFLOW_HOME="${RAILWAY_VOLUME_MOUNT_PATH%/}/airflow"
fi
export AIRFLOW_HOME="${AIRFLOW_HOME:-/opt/airflow}"

# Крок 1 (root): підготувати каталог на томі й перезапустити себе від airflow
if [ "$(id -u)" = "0" ]; then
  mkdir -p "$AIRFLOW_HOME"
  chown -R airflow:0 "$AIRFLOW_HOME"
  exec runuser -u airflow -- "$0" "$@"
fi

# Крок 2 (airflow): далі працюємо вже без прав root
# Railway підказує порт через змінну PORT; Airflow слухає його
export AIRFLOW__API__PORT="${PORT:-8080}"

# Публічний URL без пароля неприпустимий: пароль обов'язковий
: "${AIRFLOW_ADMIN_PASSWORD:?Задайте змінну AIRFLOW_ADMIN_PASSWORD у Railway (Variables)}"
: "${AIRFLOW_STUDENT_PASSWORD:?Задайте змінну AIRFLOW_STUDENT_PASSWORD у Railway (Variables)}"

# admin — викладач, student — спільний вхід для студентів (може запускати DAG-и)
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS="admin:admin,student:user"
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE="$AIRFLOW_HOME/simple_auth_manager_passwords.json"

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
