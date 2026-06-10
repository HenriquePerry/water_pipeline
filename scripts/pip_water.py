# -*- coding: utf-8 -*-
"""Legacy compatibility entrypoint for the refactored PIP Water pipeline."""

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import CONFIG
from data_parser import (
    TOKEN,
    apply_date_window,
    build_meter_anomaly_report,
    light_clean,
    list_candidate_files,
    list_from_github,
    list_from_json_file_urls,
    list_from_known_names,
    list_from_local_dir,
    normalize_records,
    read_json_source,
    save_outputs,
)
from db_writer import save_to_cratedb, save_to_mongodb, save_to_tidb
from pipeline import run_pipeline
from notifier import send_email_summary


__all__ = [
    'CONFIG',
    'TOKEN',
    'apply_date_window',
    'build_meter_anomaly_report',
    'light_clean',
    'list_candidate_files',
    'list_from_github',
    'list_from_json_file_urls',
    'list_from_known_names',
    'list_from_local_dir',
    'normalize_records',
    'read_json_source',
    'run_pipeline',
    'save_outputs',
    'save_to_cratedb',
    'save_to_mongodb',
    'save_to_tidb',
    'send_email_summary',
]


if __name__ == '__main__':
    import json

    print('=' * 60)
    print('PIP WATER | SCRIPT')
    print('=' * 60)
    result = run_pipeline()
    print(json.dumps(result, indent=2, ensure_ascii=False))
    try:
        email_dispatch = send_email_summary(result)
        print('Email dispatch:', json.dumps(email_dispatch, ensure_ascii=False))
    except Exception as exc:
        print('Falha no envio de email:', exc)
