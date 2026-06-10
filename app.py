from __future__ import annotations

import os
import threading
import uuid
from pathlib import Path
from datetime import datetime, timezone

from flask import Flask, jsonify, render_template, request
from pipeline import CONFIG, list_candidate_files, run_pipeline, send_email_summary

# In-memory job store: job_id -> {status, result, error, started_at}
_JOBS: dict[str, dict] = {}
_JOBS_LOCK = threading.Lock()

app = Flask(__name__)


def _env_bool(name: str, default: str = 'false') -> bool:
    return str(os.getenv(name, default)).strip().lower() in {'1', 'true', 'yes', 'on'}


def _request_runtime_label() -> str:
    return 'render' if os.getenv('RENDER') or os.getenv('RENDER_SERVICE_ID') or os.getenv('RENDER_EXTERNAL_URL') else 'flask'


def _source_mode_from_request(value: object) -> str:
    source = str(value or '').strip().lower()
    if source in {'github', 'fronius', 'repo', 'github_api'}:
        return 'github'
    if source in {'local', 'local_json_dir'}:
        return 'local'
    if source in {'urls', 'json_file_urls', 'custom'}:
        return 'urls'
    return 'github'


def _snapshot_pipeline_config() -> dict[str, object]:
    keys = ['json_file_urls', 'local_json_dir', 'repo_json_files', 'start_date', 'days_back', 'max_files']
    return {key: CONFIG.get(key) for key in keys}


def _apply_request_overrides(payload: dict) -> dict[str, object]:
    backup = _snapshot_pipeline_config()

    if payload.get('start_date'):
        CONFIG['start_date'] = str(payload.get('start_date'))
    if payload.get('days_back') is not None:
        try:
            CONFIG['days_back'] = int(payload.get('days_back'))
        except Exception:
            pass
    if payload.get('max_files') is not None:
        try:
            CONFIG['max_files'] = int(payload.get('max_files'))
        except Exception:
            pass

    source = _source_mode_from_request(payload.get('source'))
    if source == 'github':
        CONFIG['json_file_urls'] = ''
        CONFIG['local_json_dir'] = ''
        CONFIG['repo_json_files'] = ''
    elif source == 'local':
        CONFIG['json_file_urls'] = ''
        CONFIG['repo_json_files'] = ''
        default_local_dir = str(Path(__file__).resolve().parent / 'localINPUTS')
        CONFIG['local_json_dir'] = str(payload.get('local_json_dir') or CONFIG.get('local_json_dir') or default_local_dir)
    elif source == 'urls':
        CONFIG['local_json_dir'] = ''
        CONFIG['repo_json_files'] = ''
        if payload.get('json_file_urls') is not None:
            CONFIG['json_file_urls'] = str(payload.get('json_file_urls') or '')

    return backup


def _restore_request_overrides(backup: dict[str, object]) -> None:
    for key, value in backup.items():
        CONFIG[key] = value


def _build_file_list_payload(source: str = 'github') -> dict[str, object]:
    payload = {'source': source}
    backup = _apply_request_overrides(payload)
    try:
        files, mode = list_candidate_files()
        return {
            'source': source,
            'mode': mode,
            'files': [item['name'] for item in files],
            'count': len(files),
        }
    finally:
        _restore_request_overrides(backup)


def _email_readiness() -> dict:
    recipients = [item.strip() for item in str(CONFIG.get('email_to', '')).split(',') if item.strip()]
    backend = str(CONFIG.get('email_backend', 'smtp')).strip().lower()

    brevo_ready = bool(CONFIG.get('brevo_api_key')) and bool(CONFIG.get('brevo_sender_email'))

    checks = {
        'email_enabled': bool(CONFIG.get('email_enabled')),
        'email_backend': backend,
        'smtp_ready': bool(CONFIG.get('email_from'))
        and bool(CONFIG.get('email_username'))
        and bool(CONFIG.get('email_password')),
        'brevo_ready': brevo_ready,
        'email_recipients_count': len(recipients),
    }

    checks['ready'] = all(
        [
            checks['email_enabled'],
            checks['email_recipients_count'] > 0,
            checks['brevo_ready']
            if backend == 'brevo' or os.getenv('RENDER')
            else checks['smtp_ready'],
        ]
    )

    return checks


# =========================
# HOME PAGE
# =========================
@app.get('/')
def home():
    config = {
        'runtime': _request_runtime_label(),
        'email_backend': str(CONFIG.get('email_backend', 'smtp')).strip().lower(),
        'email_enabled': bool(CONFIG.get('email_enabled')),
        'brevo_ready': bool(CONFIG.get('brevo_api_key')) and bool(CONFIG.get('brevo_sender_email')),
        'run_send_email_default': _env_bool('RUN_SEND_EMAIL_DEFAULT', 'true'),
    }
    return render_template('index.html', config=config, timestamp_utc=datetime.now(timezone.utc).isoformat())


# =========================
# HEALTHCHECK
# =========================
@app.get('/health')
def health() -> tuple[dict, int]:
    return {
        'status': 'ok',
        'service': 'pip-water-flask',
        'timestamp_utc': datetime.now(timezone.utc).isoformat(),
    }, 200


# =========================
# FILES LIST
# =========================
@app.get('/files')
def files_list() -> tuple[dict, int]:
    source = request.args.get('source', 'github')
    payload = _build_file_list_payload(source)
    return payload, 200


# =========================
# BACKGROUND WORKER
# =========================
def _run_pipeline_job(job_id: str, payload: dict, send_email: bool, runtime_label: str) -> None:
    previous_runtime = os.getenv('PIPELINE_RUNTIME')
    config_backup = _apply_request_overrides(payload)
    os.environ['PIPELINE_RUNTIME'] = runtime_label

    try:
        result = run_pipeline()
    except Exception as exc:
        result = {'status': 'error', 'error': str(exc)}
    finally:
        _restore_request_overrides(config_backup)
        if previous_runtime is None:
            os.environ.pop('PIPELINE_RUNTIME', None)
        else:
            os.environ['PIPELINE_RUNTIME'] = previous_runtime

    if send_email and result.get('status') in {'ok', 'error'}:
        readiness = _email_readiness()
        if not readiness.get('ready'):
            result['email_dispatch'] = {
                'status': 'skipped',
                'reason': 'email configuration is incomplete or disabled',
                'checks': readiness,
            }
        else:
            try:
                send_result = send_email_summary(result)
                result['email_dispatch'] = {
                    'status': str(send_result.get('status', 'attempted')),
                    'provider': send_result.get('provider'),
                    'reason': send_result.get('reason'),
                    'error': send_result.get('error'),
                    'checks': readiness,
                }
            except Exception as exc:
                result['email_dispatch'] = {
                    'status': 'error',
                    'error': str(exc),
                    'checks': readiness,
                }

    with _JOBS_LOCK:
        _JOBS[job_id]['status'] = 'done'
        _JOBS[job_id]['result'] = result


def run_pipeline_now(payload: dict | None = None, send_email: bool | None = None, runtime_label: str = 'google-colab') -> dict:
    """Runs the pipeline synchronously and returns the result payload.

    Useful for notebooks (e.g. Google Colab) that need direct execution
    without starting Flask routes or background threads.
    """

    payload = payload or {}
    if send_email is None:
        send_email = _env_bool('RUN_SEND_EMAIL_DEFAULT', 'true')

    previous_runtime = os.getenv('PIPELINE_RUNTIME')
    config_backup = _apply_request_overrides(payload)
    os.environ['PIPELINE_RUNTIME'] = runtime_label

    try:
        result = run_pipeline()
    except Exception as exc:
        result = {'status': 'error', 'error': str(exc)}
    finally:
        _restore_request_overrides(config_backup)
        if previous_runtime is None:
            os.environ.pop('PIPELINE_RUNTIME', None)
        else:
            os.environ['PIPELINE_RUNTIME'] = previous_runtime

    if send_email and result.get('status') in {'ok', 'error'}:
        readiness = _email_readiness()
        if not readiness.get('ready'):
            result['email_dispatch'] = {
                'status': 'skipped',
                'reason': 'email configuration is incomplete or disabled',
                'checks': readiness,
            }
        else:
            try:
                send_result = send_email_summary(result)
                result['email_dispatch'] = {
                    'status': str(send_result.get('status', 'attempted')),
                    'provider': send_result.get('provider'),
                    'reason': send_result.get('reason'),
                    'error': send_result.get('error'),
                    'checks': readiness,
                }
            except Exception as exc:
                result['email_dispatch'] = {
                    'status': 'error',
                    'error': str(exc),
                    'checks': readiness,
                }

    return result


# =========================
# RUN PIPELINE (async)
# =========================
@app.post('/run')
def run_once() -> tuple[dict, int]:
    """Starts a pipeline job in background and returns a job_id immediately."""
    payload = request.get_json(silent=True) or {}

    send_email_default = _env_bool('RUN_SEND_EMAIL_DEFAULT', 'true')
    send_email_raw = payload.get('send_email')
    if send_email_raw is None:
        send_email = send_email_default
    elif isinstance(send_email_raw, str):
        send_email = send_email_raw.strip().lower() in {'1', 'true', 'yes', 'on'}
    else:
        send_email = bool(send_email_raw)

    job_id = str(uuid.uuid4())
    with _JOBS_LOCK:
        _JOBS[job_id] = {
            'status': 'running',
            'result': None,
            'started_at': datetime.now(timezone.utc).isoformat(),
        }

    t = threading.Thread(
        target=_run_pipeline_job,
        args=(job_id, payload, send_email, _request_runtime_label()),
        daemon=True,
    )
    t.start()

    return jsonify({'job_id': job_id, 'status': 'running'}), 202


# =========================
# JOB STATUS
# =========================
@app.get('/status/<job_id>')
def job_status(job_id: str) -> tuple[dict, int]:
    with _JOBS_LOCK:
        job = _JOBS.get(job_id)
    if not job:
        return jsonify({'error': 'job not found'}), 404
    if job['status'] == 'running':
        return jsonify({'job_id': job_id, 'status': 'running', 'started_at': job['started_at']}), 202
    result = job.get('result') or {}
    http_status = 200 if result.get('status') == 'ok' else 500
    return jsonify({'job_id': job_id, 'status': 'done', 'result': result}), http_status


# =========================
# CONFIG DEBUG
# =========================
@app.get('/config')
def config_snapshot() -> tuple[dict, int]:
    """
    Safe config view for quick debugging without secrets.
    """

    return {
        'repo_owner': CONFIG.get('repo_owner'),
        'repo_name': CONFIG.get('repo_name'),
        'repo_folder': CONFIG.get('repo_folder'),
        'output_folder': CONFIG.get('output_folder'),
        'json_file_urls_set': bool(CONFIG.get('json_file_urls')),
        'local_json_dir': CONFIG.get('local_json_dir'),
        'repo_json_files_set': bool(CONFIG.get('repo_json_files')),
        'mongo_enabled': bool(CONFIG.get('mongo_enabled')),
        'tidb_enabled': bool(CONFIG.get('tidb_enabled')),
        'cratedb_enabled': bool(CONFIG.get('cratedb_enabled')),
        'email_enabled': bool(CONFIG.get('email_enabled')),
        'email_backend': str(CONFIG.get('email_backend', 'smtp')).strip().lower(),
        'email_to_count': len(
            [item.strip() for item in str(CONFIG.get('email_to', '')).split(',') if item.strip()]
        ),
        'smtp_ready': bool(CONFIG.get('email_from'))
        and bool(CONFIG.get('email_username'))
        and bool(CONFIG.get('email_password')),
        'brevo_ready': bool(CONFIG.get('brevo_api_key'))
        and bool(CONFIG.get('brevo_sender_email')),
        'run_send_email_default': _env_bool('RUN_SEND_EMAIL_DEFAULT', 'true'),
    }, 200


# =========================
# START FLASK
# =========================
if __name__ == '__main__':

    host = os.getenv('FLASK_HOST', '0.0.0.0')

    port = int(os.getenv('PORT', os.getenv('FLASK_PORT', '5000')))

    debug = (
        os.getenv('FLASK_DEBUG', 'false')
        .strip()
        .lower()
        in {'1', 'true', 'yes', 'on'}
    )

    app.run(host=host, port=port, debug=debug)