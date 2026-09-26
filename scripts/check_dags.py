"""Перевіряє, що всі DAG-и в dags/ імпортуються без помилок.

Запуск: python scripts/check_dags.py  (потрібен встановлений apache-airflow)
Завершується з кодом 1, якщо хоч один файл не імпортується або є дублікати dag_id.
"""
import os
import sys
from pathlib import Path

os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402

dags_dir = Path(__file__).resolve().parent.parent / "dags"
bag = DagBag(dag_folder=str(dags_dir), safe_mode=False)

if bag.import_errors:
    print("Помилки імпорту DAG-ів:\n")
    for file, error in bag.import_errors.items():
        print(f"--- {Path(file).relative_to(dags_dir.parent)}\n{error}\n")
    sys.exit(1)

print(f"Ок: {len(bag.dags)} DAG-ів імпортуються без помилок")
