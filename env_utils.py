"""Environment and runtime utility helpers for the PIP Water project."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from urllib.parse import quote_plus, urlparse


def _env_bool(name: str, default: str = 'false') -> bool:
    return os.getenv(name, default).strip().lower() in {'1', 'true', 'yes', 'on'}


def _strip_wrapping_quotes(text: str) -> str:
    value = text.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1].strip()
    return value


def _json_dict_from_text(raw: str) -> dict | None:
    if not raw:
        return None
    text = _strip_wrapping_quotes(str(raw))
    try:
        payload = json.loads(text)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _colab_secret_text(name: str) -> str:
    if not is_colab():
        return ''
    try:
        from google.colab import userdata  # type: ignore

        raw = userdata.get(name)
        if raw is None:
            return ''
        return str(raw).strip()
    except Exception:
        return ''


def _secret_text(name: str) -> str:
    value = (os.getenv(name) or '').strip()
    if value:
        return value
    return _colab_secret_text(name)


def _candidate_user_prefixes() -> list[str]:
    explicit = _secret_text('USER').strip()
    candidates = [explicit, 'TO', 'PI']
    unique: list[str] = []
    for candidate in candidates:
        item = candidate.strip()
        if item and item not in unique:
            unique.append(item)
    return unique


def _prefixed_json_payload(suffix: str) -> dict | None:
    for prefix in _candidate_user_prefixes():
        payload = _json_dict_from_text(_secret_text(f'{prefix}_{suffix}'))
        if payload:
            return payload
    return None


def _gmail_json_payload() -> dict | None:
    for prefix in _candidate_user_prefixes():
        payload = _json_dict_from_text(_secret_text(f'configGMail_{prefix}_json'))
        if payload:
            return payload
    return None


def _legacy_value(name: str) -> str:
    key = name.strip().upper()

    if key == 'GITHUB_TOKEN':
        github = _prefixed_json_payload('github_json')
        if github:
            token = str(github.get('key') or github.get('token') or '').strip()
            if token:
                return token

    if key.startswith('TIDB_'):
        tidb = _prefixed_json_payload('tidb_json') or {}
        mapping = {
            'TIDB_HOST': str(tidb.get('dest_host') or '').strip(),
            'TIDB_PORT': str(tidb.get('port') or '').strip(),
            'TIDB_USER': str(tidb.get('username') or '').strip(),
            'TIDB_PASSWORD': str(tidb.get('password') or '').strip(),
            'TIDB_DATABASE': str(tidb.get('database') or '').strip(),
        }
        value = mapping.get(key, '')
        if value:
            return value

    if key.startswith('CRATEDB_'):
        crate = _prefixed_json_payload('crate_json') or {}
        dest_host = str(crate.get('dest_host') or '').strip()
        parsed = urlparse(dest_host) if dest_host else None
        host_only = parsed.hostname if parsed and parsed.hostname else dest_host
        port_only = parsed.port if parsed and parsed.port else crate.get('port')
        mapping = {
            'CRATEDB_HOST': str(host_only or '').strip(),
            'CRATEDB_PORT': str(port_only or '').strip(),
            'CRATEDB_USER': str(crate.get('username') or '').strip(),
            'CRATEDB_PASSWORD': str(crate.get('password') or '').strip(),
            'CRATEDB_DATABASE': str(crate.get('database') or '').strip(),
        }
        value = mapping.get(key, '')
        if value:
            return value

    if key.startswith('MONGO_'):
        mongo = _prefixed_json_payload('mongodb_json') or {}
        host = str(mongo.get('dest_host') or '').strip()
        username = str(mongo.get('username') or '').strip()
        password = str(mongo.get('password') or '').strip()
        database = str(mongo.get('database') or '').strip()
        port = str(mongo.get('port') or '').strip()
        if key == 'MONGO_DB' and database:
            return database
        if key == 'MONGO_URI' and host:
            if host.startswith('mongodb://') or host.startswith('mongodb+srv://'):
                return host
            if username and password:
                user_quoted = quote_plus(username)
                pwd_quoted = quote_plus(password)
                if host.endswith('mongodb.net'):
                    return f'mongodb+srv://{user_quoted}:{pwd_quoted}@{host}/'
                if port:
                    return f'mongodb://{user_quoted}:{pwd_quoted}@{host}:{port}/'
                return f'mongodb://{user_quoted}:{pwd_quoted}@{host}/'

    if key == 'EMAIL_ENABLED':
        return _secret_text('EMAIL_SEND')

    if key == 'EMAIL_TO':
        return _secret_text('EMAIL_ADDRESSES')

    if key == 'EMAIL_SMTP_HOST':
        explicit = _secret_text('BREVO_SMTP_HOST').strip()
        if explicit:
            return explicit
        # Legacy Brevo SMTP credentials usually use smtp-relay.brevo.com on 587.
        if _secret_text('BREVO_USER').strip() and _secret_text('BREVO_PASSWORD').strip():
            return 'smtp-relay.brevo.com'

    if key == 'EMAIL_SMTP_PORT':
        explicit = _secret_text('BREVO_SMTP_PORT').strip()
        if explicit:
            return explicit
        if _secret_text('BREVO_USER').strip() and _secret_text('BREVO_PASSWORD').strip():
            return '587'

    if key == 'EMAIL_BACKEND':
        if _secret_text('BREVO_API_KEY').strip():
            return 'brevo'
        if _secret_text('BREVO_USER').strip() and _secret_text('BREVO_PASSWORD').strip():
            return 'smtp'

    if key in {'EMAIL_FROM', 'EMAIL_USERNAME', 'EMAIL_PASSWORD'}:
        gmail = _gmail_json_payload() or {}
        mapping = {
            'EMAIL_FROM': str(gmail.get('UserFrom') or '').strip(),
            'EMAIL_USERNAME': str(gmail.get('UserName') or '').strip(),
            'EMAIL_PASSWORD': str(gmail.get('UserPwd') or '').strip(),
        }
        value = mapping.get(key, '')
        if value:
            return value

    if key == 'EMAIL_FROM':
        return _secret_text('BREVO_FROM')
    if key == 'EMAIL_USERNAME':
        return _secret_text('BREVO_USER')
    if key == 'EMAIL_PASSWORD':
        return _secret_text('BREVO_PASSWORD')

    return ''


def is_colab() -> bool:
    try:
        import google.colab  # type: ignore

        return google.colab is not None
    except Exception:
        return False


def is_render() -> bool:
    return bool(os.getenv('RENDER') or os.getenv('RENDER_SERVICE_ID') or os.getenv('RENDER_EXTERNAL_URL'))


def _local_default(value_if_local: str, value_if_colab: str) -> str:
    return value_if_local if not is_colab() else value_if_colab


def detect_runtime() -> str:
    if runtime := os.getenv('PIPELINE_RUNTIME'):
        return runtime.strip().lower()

    if is_colab():
        return 'google-colab'
    if is_render():
        return 'render'
    if os.getenv('AIRFLOW_HOME') or os.getenv('AIRFLOW_CTX_DAG_ID'):
        return 'airflow'

    argv0 = Path(sys.argv[0]).name.lower() if sys.argv else ''
    if argv0 in {'pip_water.py', 'pip_water'}:
        return 'local'

    if os.getenv('WERKZEUG_RUN_MAIN'):
        return 'flask'

    return 'local'


def env_or_colab_secret(name: str, default: str = '') -> str:
    value = _secret_text(name)
    if value:
        payload = _json_dict_from_text(value)
        if payload:
            for key in ('value', 'uri', 'token', 'key', 'password'):
                candidate = payload.get(key)
                if candidate:
                    return str(candidate).strip()
        return _strip_wrapping_quotes(value)

    legacy_value = _legacy_value(name)
    if legacy_value:
        return _strip_wrapping_quotes(legacy_value)

    return default


def _env_or_default(name: str, default: str = '') -> str:
    return (os.getenv(name) or default).strip()


def load_dotenv_if_available() -> None:
    if is_colab():
        return
    try:
        from dotenv import find_dotenv, load_dotenv

        dotenv_file = find_dotenv('.env', usecwd=True)
        if dotenv_file:
            load_dotenv(dotenv_file, override=True)
            return

        script_dir = Path(__file__).resolve().parent
        for candidate in (script_dir / '.env', script_dir.parent / '.env'):
            if candidate.exists():
                load_dotenv(candidate, override=True)
                return
    except Exception:
        return


__all__ = [
    '_env_bool',
    '_env_or_default',
    '_local_default',
    'detect_runtime',
    'env_or_colab_secret',
    'is_colab',
    'is_render',
    'load_dotenv_if_available',
]
