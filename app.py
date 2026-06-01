from __future__ import annotations

import os
from datetime import datetime, timezone

from flask import Flask, jsonify, request

from scripts.pip_water import CONFIG, run_pipeline, send_email_summary

app = Flask(__name__)


def _email_readiness() -> dict:
    recipients = [item.strip() for item in str(CONFIG.get('email_to', '')).split(',') if item.strip()]
    backend = str(CONFIG.get('email_backend', 'smtp')).strip().lower()
    brevo_ready = bool(CONFIG.get('brevo_api_key')) and bool(CONFIG.get('brevo_sender_email') or CONFIG.get('email_from'))
    checks = {
        'email_enabled': bool(CONFIG.get('email_enabled')),
        'email_backend': backend,
        'smtp_ready': bool(CONFIG.get('email_from')) and bool(CONFIG.get('email_username')) and bool(CONFIG.get('email_password')),
        'brevo_ready': brevo_ready,
        'email_recipients_count': len(recipients),
    }
    checks['ready'] = all(
        [
            checks['email_enabled'],
            checks['email_recipients_count'] > 0,
            checks['brevo_ready'] if backend == 'brevo' or os.getenv('RENDER') else checks['smtp_ready'],
        ]
    )
    return checks


@app.get('/health')
def health() -> tuple[dict, int]:
    return {
        'status': 'ok',
        'service': 'pip-water-flask',
        'timestamp_utc': datetime.now(timezone.utc).isoformat(),
    }, 200


@app.post('/run')
def run_once() -> tuple[dict, int]:
    """Triggers one pipeline execution and returns the result JSON."""
    payload = request.get_json(silent=True) or {}
    send_email = bool(payload.get('send_email', False))

    result = run_pipeline()

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
                send_email_summary(result)
                result['email_dispatch'] = {'status': 'attempted', 'checks': readiness}
            except Exception as exc:
                result['email_dispatch'] = {'status': 'error', 'error': str(exc), 'checks': readiness}

    http_status = 200 if result.get('status') == 'ok' else 500
    return jsonify(result), http_status


@app.get('/config')
def config_snapshot() -> tuple[dict, int]:
    """Safe config view for quick debugging without secrets."""
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
        'email_to_count': len([item.strip() for item in str(CONFIG.get('email_to', '')).split(',') if item.strip()]),
        'smtp_ready': bool(CONFIG.get('email_from')) and bool(CONFIG.get('email_username')) and bool(CONFIG.get('email_password')),
        'brevo_ready': bool(CONFIG.get('brevo_api_key')) and bool(CONFIG.get('brevo_sender_email') or CONFIG.get('email_from')),
    }, 200


if __name__ == '__main__':
    host = os.getenv('FLASK_HOST', '0.0.0.0')
    port = int(os.getenv('PORT', os.getenv('FLASK_PORT', '5000')))
    debug = os.getenv('FLASK_DEBUG', 'false').strip().lower() in {'1', 'true', 'yes', 'on'}
    app.run(host=host, port=port, debug=debug)
