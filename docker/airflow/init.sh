#!/usr/bin/env bash
set -euo pipefail

airflow_uid="${AIRFLOW_UID:-50000}"
passwords_file="${AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE:-/opt/airflow/logs/simple_auth_manager_passwords.json}"

install -d -o "${airflow_uid}" -g 0 -m 0770 /opt/airflow/logs

/usr/python/bin/python3 -c '
import json
import os
from pathlib import Path

path = Path(os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"])
path.write_text(
    json.dumps({os.environ["AIRFLOW_ADMIN_USERNAME"]: os.environ["AIRFLOW_ADMIN_PASSWORD"]}),
    encoding="utf-8",
)
os.chmod(path, 0o600)
os.chown(path, int(os.environ.get("AIRFLOW_UID", "50000")), 0)
'

su airflow -s /bin/bash -c 'airflow db migrate'
chown -R "${airflow_uid}:0" /opt/airflow/logs
