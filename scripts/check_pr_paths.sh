#!/usr/bin/env bash
# Студент може міняти тільки dags/students/<свій_логін>/.
# Змінні: PR_AUTHOR, BASE_REF (наприклад origin/main), OWNER (логін викладача).
set -euo pipefail

if [ "${PR_AUTHOR,,}" = "${OWNER,,}" ]; then
  echo "PR власника репозиторію — обмежень немає"
  exit 0
fi

allowed="dags/students/${PR_AUTHOR,,}/"
bad=0
while IFS= read -r file; do
  lower="${file,,}"
  if [[ "$lower" != "$allowed"* ]]; then
    echo "::error file=$file::Файл поза вашою папкою. Дозволено тільки $allowed"
    bad=1
  fi
done < <(git diff --name-only "$BASE_REF"...HEAD)

exit $bad
