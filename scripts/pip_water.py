# -*- coding: utf-8 -*-
"""pip_water.py"""

import hashlib
import html as ihtml
import json
import os
import re
import random
import smtplib
import ssl
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

import pandas as pd
import requests

try:
    from pymongo import MongoClient, UpdateOne
except Exception:  # pragma: no cover - optional dependency at runtime
    MongoClient = None  # type: ignore[assignment]
    UpdateOne = None  # type: ignore[assignment]

try:
    import pymysql
except Exception:  # pragma: no cover - optional dependency at runtime
    pymysql = None  # type: ignore[assignment]

try:
    import psycopg2
except Exception:  # pragma: no cover - optional dependency at runtime
    psycopg2 = None  # type: ignore[assignment]

try:
    import certifi
except Exception:  # pragma: no cover - optional dependency at runtime
    certifi = None  # type: ignore[assignment]


def _env_bool(name: str, default: str = 'false') -> bool:
    return os.getenv(name, default).strip().lower() in {'1', 'true', 'yes', 'on'}


def _local_default(value_if_local: str, value_if_colab: str) -> str:
    return value_if_local if not is_colab() else value_if_colab


def is_colab() -> bool:
    try:
        import google.colab  # type: ignore
        return google.colab is not None
    except Exception:
        return False


def is_render() -> bool:
    return bool(os.getenv('RENDER') or os.getenv('RENDER_SERVICE_ID') or os.getenv('RENDER_EXTERNAL_URL'))


def detect_runtime() -> str:
    if runtime := os.getenv('PIPELINE_RUNTIME'):
        return runtime.strip().lower()

    if is_colab():
        return 'google-colab'
    if is_render():
        return 'render'
    if os.getenv('AIRFLOW_HOME') or os.getenv('AIRFLOW_CTX_DAG_ID'):
        return 'airflow'

    # Running this file directly should stay local even if Werkzeug env vars leak in the shell.
    argv0 = Path(sys.argv[0]).name.lower() if sys.argv else ''
    if argv0 in {'pip_water.py', 'pip_water'}:
        return 'local'

    # Fallback: developer server workers usually expose this marker.
    if os.getenv('WERKZEUG_RUN_MAIN'):
        return 'flask'

    return 'local'


def env_or_colab_secret(name: str, default: str = '') -> str:
    value = (os.getenv(name) or '').strip()
    if value:
        return value

    if not is_colab():
        return default

    try:
        from google.colab import userdata  # type: ignore

        raw = userdata.get(name)
        if not raw:
            return default
        raw_text = str(raw).strip()
        try:
            payload = json.loads(raw_text)
            if isinstance(payload, dict):
                for key in ('value', 'uri', 'token', 'key', 'password'):
                    candidate = payload.get(key)
                    if candidate:
                        return str(candidate).strip()
        except Exception:
            pass
        return raw_text
    except Exception:
        return default


def _env_or_default(name: str, default: str = '') -> str:
    return (os.getenv(name) or default).strip()


if not is_colab():
    try:
        from dotenv import find_dotenv, load_dotenv

        dotenv_file = find_dotenv('.env', usecwd=True)
        if dotenv_file:
            load_dotenv(dotenv_file, override=True)
        else:
            script_dir = Path(__file__).resolve().parent
            for candidate in (script_dir / '.env', script_dir.parent / '.env'):
                if candidate.exists():
                    load_dotenv(candidate, override=True)
                    break
    except Exception:
        pass


# ============================================================
# Configuration and runtime settings
# ============================================================

mongo_uri_value = env_or_colab_secret('MONGO_URI', '').strip()
tidb_host_value = env_or_colab_secret('TIDB_HOST', '').strip()
tidb_port_value = int(env_or_colab_secret('TIDB_PORT', '4000'))
tidb_user_value = env_or_colab_secret('TIDB_USER', '').strip()
tidb_password_value = env_or_colab_secret('TIDB_PASSWORD', '').strip()
tidb_database_value = env_or_colab_secret('TIDB_DATABASE', 'pi_water').strip()
tidb_table_value = env_or_colab_secret('TIDB_TABLE', 'water_records').strip()
tidb_ca_path_value = env_or_colab_secret('TIDB_CA_PATH', '').strip()

cratedb_host_value = env_or_colab_secret('CRATEDB_HOST', '').strip()
cratedb_port_value = int(env_or_colab_secret('CRATEDB_PORT', '5432'))
cratedb_user_value = env_or_colab_secret('CRATEDB_USER', '').strip()
cratedb_password_value = env_or_colab_secret('CRATEDB_PASSWORD', '').strip()
cratedb_database_value = env_or_colab_secret('CRATEDB_DATABASE', 'crate').strip()
cratedb_table_value = env_or_colab_secret('CRATEDB_TABLE', 'water_records').strip()
cratedb_sslmode_value = env_or_colab_secret('CRATEDB_SSLMODE', 'require').strip()

CONFIG = {
    'repo_owner': os.getenv('REPO_OWNER', 'pedroccpimenta'),
    'repo_name': os.getenv('REPO_NAME', 'datafiles'),
    'repo_folder': os.getenv('REPO_FOLDER', 'aqualog'),
    'repo_branch': os.getenv('REPO_BRANCH', 'master'),
    'request_timeout': int(os.getenv('REQUEST_TIMEOUT', _local_default('15', '30'))),
    'days_back': int(os.getenv('DAYS_BACK', '0')),
    'start_date': os.getenv('START_DATE', datetime.now(timezone.utc).strftime('%Y-%m-%d')),
    'output_folder': os.getenv('OUTPUT_FOLDER', '/content/output_water' if is_colab() else './output_water'),
    'json_file_urls': os.getenv('JSON_FILE_URLS', ''),
    'local_json_dir': os.getenv('LOCAL_JSON_DIR', ''),
    'repo_json_files': os.getenv('REPO_JSON_FILES', ''),
    'verbose': _env_bool('VERBOSE', 'true'),
    'email_enabled': _env_bool('EMAIL_ENABLED', 'false'),
    'email_smtp_host': os.getenv('EMAIL_SMTP_HOST', 'smtp.gmail.com'),
    'email_smtp_port': int(os.getenv('EMAIL_SMTP_PORT', '587')),
    'email_from': os.getenv('EMAIL_FROM', ''),
    'email_to': os.getenv('EMAIL_TO', ''),
    'email_username': os.getenv('EMAIL_USERNAME', ''),
    'email_password': os.getenv('EMAIL_PASSWORD', ''),
    'email_backend': _env_or_default('EMAIL_BACKEND', 'brevo' if is_render() else 'smtp'),
    'brevo_api_key': _env_or_default('BREVO_API_KEY', ''),
    'brevo_sender_name': _env_or_default('BREVO_SENDER_NAME', ''),
    'brevo_sender_email': _env_or_default('BREVO_SENDER_EMAIL', ''),
    'github_secret_name': os.getenv('GITHUB_SECRET_NAME', '').strip(),
    'mongo_uri': mongo_uri_value,
    'mongo_enabled': _env_bool('MONGO_ENABLED', 'false') or bool(mongo_uri_value),
    'mongo_db': os.getenv('MONGO_DB', 'pi_water').strip(),
    'mongo_collection': os.getenv('MONGO_COLLECTION', 'water_records').strip(),
    'mongo_app_name': os.getenv('MONGO_APP_NAME', 'pip-water').strip(),
    'mongo_server_selection_timeout_ms': int(os.getenv('MONGO_SERVER_SELECTION_TIMEOUT_MS', _local_default('4000', '10000'))),
    'tidb_host': tidb_host_value,
    'tidb_port': tidb_port_value,
    'tidb_user': tidb_user_value,
    'tidb_password': tidb_password_value,
    'tidb_database': tidb_database_value,
    'tidb_table': tidb_table_value,
    'tidb_ca_path': tidb_ca_path_value,
    'tidb_connect_timeout': int(env_or_colab_secret('TIDB_CONNECT_TIMEOUT', _local_default('4', '10'))),
    'tidb_enabled': _env_bool('TIDB_ENABLED', 'false')
    or bool(tidb_host_value and tidb_user_value and tidb_password_value and tidb_database_value and tidb_table_value),
    'cratedb_host': cratedb_host_value,
    'cratedb_port': cratedb_port_value,
    'cratedb_user': cratedb_user_value,
    'cratedb_password': cratedb_password_value,
    'cratedb_database': cratedb_database_value,
    'cratedb_table': cratedb_table_value,
    'cratedb_sslmode': cratedb_sslmode_value,
    'cratedb_enabled': _env_bool('CRATEDB_ENABLED', 'false')
    or bool(
        cratedb_host_value and cratedb_user_value and cratedb_password_value and cratedb_database_value and cratedb_table_value
    ),
    'anomaly_threshold_ratio': float(os.getenv('ANOMALY_THRESHOLD_RATIO', '0.8')),
    'anomaly_upper_threshold_ratio': float(os.getenv('ANOMALY_UPPER_THRESHOLD_RATIO', '1.2')),
    'anomaly_lookback_days': int(os.getenv('ANOMALY_LOOKBACK_DAYS', '2')),
    'anomaly_history_local_json': os.getenv('ANOMALY_HISTORY_LOCAL_JSON', '').strip(),
    'max_files': int(os.getenv('MAX_FILES', '0')),
    'repo_sample_size': int(os.getenv('REPO_SAMPLE_SIZE', '0')),
    'skip_db_writes': _env_bool('SKIP_DB_WRITES', 'false'),
}

print('Runtime:', detect_runtime())
print('Repo target:', f"{CONFIG['repo_owner']}/{CONFIG['repo_name']}/{CONFIG['repo_folder']}")
print('Output folder:', CONFIG['output_folder'])
print('Mongo enabled:', CONFIG['mongo_enabled'])
print('Mongo URI set:', bool(CONFIG['mongo_uri']))
print('TiDB enabled:', CONFIG['tidb_enabled'])
print('TiDB host set:', bool(CONFIG['tidb_host']))
print('CrateDB enabled:', CONFIG['cratedb_enabled'])
print('CrateDB host set:', bool(CONFIG['cratedb_host']))


@dataclass
class PipelineContext:
    runtime: str
    run_id: str
    started_at: str


# ============================================================
# GitHub authentication and discovery of JSON files to process
# ============================================================


def load_colab_secret(secret_name: str):
    if not is_colab():
        return None
    try:
        from google.colab import userdata  # type: ignore

        raw = userdata.get(secret_name)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return {'token': str(raw).strip()}
    except Exception:
        return None


def get_github_token() -> str | None:
    token = (os.getenv('GITHUB_TOKEN') or '').strip()
    if token:
        return token

    candidates = []
    if CONFIG['github_secret_name']:
        candidates.append(CONFIG['github_secret_name'])
    candidates.extend(['GITHUB_TOKEN', 'github_token', 'TO-github.json', 'TO-github_token.json', 'github_token.json'])

    for secret_name in candidates:
        payload = load_colab_secret(secret_name)
        if not payload:
            continue
        value = payload.get('key') or payload.get('token')
        if value:
            return str(value).strip()

    return None


def headers(token: str | None) -> dict[str, str]:
    h = {'Accept': 'application/vnd.github+json'}
    if token:
        h['Authorization'] = f'Bearer {token}'
    return h


TOKEN = get_github_token()
print('GitHub token presente:', bool(TOKEN))

#função que gera a URL raw do ficheiro JSON no GitHub para download direto pela pipeline
# (ele gera: https://raw.githubusercontent.com/pedroccpimenta/datafiles/master/aqualog/file1.json)
def raw_url(file_path: str, branch: str) -> str:
    return f"https://raw.githubusercontent.com/{CONFIG['repo_owner']}/{CONFIG['repo_name']}/{branch}/{file_path}"

#lista de branch possiveis 
def branch_candidates() -> list[str]:
    preferred = CONFIG['repo_branch'].strip()
    values = [preferred, 'main', 'master']
    unique: list[str] = []
    for value in values:
        if value and value not in unique:
            unique.append(value)
    return unique

#lê URls defenidos manualmente no config (caso queira ler fichieros JSON de URLs externas, não do GitHub)
def list_from_json_file_urls() -> list[dict[str, str]]:
    items = [u.strip() for u in CONFIG['json_file_urls'].split(',') if u.strip()]
    return [{'name': Path(item).name, 'source': item} for item in items]

#procura JSON de fichieros locais 
def list_from_local_dir() -> list[dict[str, str]]:
    local_dir = CONFIG['local_json_dir'].strip()
    if not local_dir:
        return []
    folder = Path(local_dir)
    if not folder.exists():
        return []
    return [{'name': p.name, 'source': str(p)} for p in sorted(folder.glob('*.json'))]

#procura JSON de ficheiros listados manualmente no config (caso queira ler ficheiros JSON de um repositorio GitHub específico, mas sem usar a API, apenas construindo as URLs raw)
def list_from_known_names() -> list[dict[str, str]]:
    names = [n.strip() for n in CONFIG['repo_json_files'].split(',') if n.strip()]
    chosen_branch = branch_candidates()[0]
    return [
        {'name': Path(name).name, 'source': raw_url(f"{CONFIG['repo_folder']}/{name}", chosen_branch)} for name in names
    ]

#liga á GIThub API , lista ficheiros, encontra .json gera links de URLs, e retorna uma lista de dicionarios com nome e URL de cada ficheiro JSON encontrado
def list_from_github(token: str | None) -> list[dict[str, str]]:
    last_error: str | None = None

    for branch in branch_candidates():
        api_url = f"https://api.github.com/repos/{CONFIG['repo_owner']}/{CONFIG['repo_name']}/contents/{CONFIG['repo_folder']}"
        response = requests.get(api_url, headers=headers(token), params={'ref': branch}, timeout=CONFIG['request_timeout'])

        if response.ok:
            payload = response.json()
            files = [
                {'name': item['name'], 'source': item['download_url']}
                for item in payload
                if item.get('type') == 'file' and str(item.get('name', '')).endswith('.json')
            ]
            if files:
                return files

        last_error = f'API {branch}: {response.status_code} {response.reason}'

        tree_url = f"https://github.com/{CONFIG['repo_owner']}/{CONFIG['repo_name']}/tree/{branch}/{CONFIG['repo_folder']}"
        page = requests.get(tree_url, timeout=CONFIG['request_timeout'])
        if page.ok:
            pattern = (
                rf'href="/{re.escape(CONFIG["repo_owner"])}/{re.escape(CONFIG["repo_name"])}'
                rf'/blob/{re.escape(branch)}/{re.escape(CONFIG["repo_folder"])}/([^\"]+?\.json)"'
            )
            matches = sorted(set(re.findall(pattern, page.text)))
            if matches:
                return [
                    {'name': Path(name).name, 'source': raw_url(f"{CONFIG['repo_folder']}/{name}", branch)}
                    for name in matches
                ]

    if last_error:
        raise RuntimeError(
            f'GitHub listing failed for branches {branch_candidates()}: {last_error}. '
            'Confirma token, branch e permissao read no repositorio privado.'
        )
    return []

#funcao que define a origem dos ficheiros JSON a processar pela pipeline 
def extract_date_from_name(file_name: str):
    match = re.search(r'(20\d{6})', file_name)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), '%Y%m%d').date()
    except ValueError:
        return None


def apply_date_window(files: list[dict[str, str]]) -> list[dict[str, str]]:
    days_back = CONFIG['days_back']
    if days_back <= 0:
        return files

    end_date = datetime.strptime(CONFIG['start_date'], '%Y-%m-%d').date()
    start_date = end_date - timedelta(days=days_back)

    selected = []
    for file_info in files:
        file_date = extract_date_from_name(file_info['name'])
        if file_date is None or (start_date <= file_date <= end_date):
            selected.append(file_info)
    return selected


def limit_repo_files_randomly(files: list[dict[str, str]]) -> list[dict[str, str]]:
    sample_size = int(CONFIG.get('repo_sample_size', 30) or 30)
    if sample_size <= 0 or len(files) <= sample_size:
        return files
    return random.sample(files, sample_size)


# ============================================================
# Transformation, outputs and persistence
# ============================================================


def read_json_source(source: str, token: str | None) -> Any:
    if source.startswith('http://') or source.startswith('https://'):
        response = requests.get(source, headers=headers(token), timeout=CONFIG['request_timeout'])
        response.raise_for_status()
        return response.json()

    path = Path(source)
    if not path.exists():
        raise FileNotFoundError(f'JSON file not found: {source}')

    with path.open('r', encoding='utf-8') as fh:
        return json.load(fh)


def normalize_records(data: Any, source_name: str, run_id: str) -> list[dict[str, Any]]:
    ingested_at = datetime.now(timezone.utc).isoformat()
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = [data]
    else:
        rows = [{'value': data}]

    out = []
    for row in rows:
        record = dict(row) if isinstance(row, dict) else {'value': row}
        record['_source_file'] = source_name
        record['_ingested_at'] = ingested_at
        record['_run_id'] = run_id
        out.append(record)
    return out


def light_clean(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    dedup_key = df.apply(lambda row: json.dumps(row.to_dict(), sort_keys=True, default=str), axis=1)
    df = df.loc[~dedup_key.duplicated()].copy()

    for col in ['timestamp', 'time', 'datetime', 'datahora', 'date']:
        if col in df.columns:
            parsed = pd.to_datetime(df[col], errors='coerce', utc=True)
            df[col] = parsed.dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    return df


def save_outputs(df: pd.DataFrame):
    output_dir = Path(CONFIG['output_folder'])
    output_dir.mkdir(parents=True, exist_ok=True)

    json_path = output_dir / 'water_data_raw.json'
    csv_path = output_dir / 'water_data_clean.csv'
    result_path = output_dir / 'water_result.json'

    df.to_json(json_path, orient='records', force_ascii=False)
    df.to_csv(csv_path, index=False)

    return {
        'json_path': str(json_path),
        'csv_path': str(csv_path),
        'result_path': str(result_path),
    }


def _doc_hash(document: dict[str, Any]) -> str:
    payload = json.dumps(document, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def _stable_record_identity(record: dict[str, Any]) -> str:
    identity = dict(record)
    for volatile_key in ('_ingested_at', '_run_id', '_saved_at', '_last_run_id'):
        identity.pop(volatile_key, None)
    return _doc_hash(identity)


def _utc_sql_datetime(value: str | datetime | None) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if not value:
        return datetime.now(timezone.utc).replace(tzinfo=None)
    try:
        parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        if parsed.tzinfo is not None:
            return parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed
    except Exception:
        return datetime.now(timezone.utc).replace(tzinfo=None)


def _sanitize_mongo_uri_for_colab(uri: str) -> str:
    if not uri or not is_colab() or '?' not in uri:
        return uri

    base, query = uri.split('?', 1)
    keep = []
    remove_keys = {'tlscafile', 'ssl_ca_certs', 'tlscertificatekeyfile'}
    for item in query.split('&'):
        key = item.split('=', 1)[0].strip().lower()
        if key in remove_keys:
            continue
        keep.append(item)
    return f"{base}?{'&'.join(keep)}" if keep else base


def save_to_mongodb(df: pd.DataFrame, run_id: str) -> dict[str, Any]:
    if not CONFIG['mongo_enabled']:
        return {'enabled': False, 'status': 'disabled'}
    if not CONFIG['mongo_uri']:
        return {'enabled': True, 'status': 'error', 'error': 'MONGO_URI is empty.'}
    if MongoClient is None or UpdateOne is None:
        return {
            'enabled': True,
            'status': 'error',
            'error': 'pymongo is not installed. Install dependencies from requirements.txt.',
        }

    records = json.loads(df.to_json(orient='records', force_ascii=False, date_format='iso'))
    if not records:
        return {'enabled': True, 'status': 'ok', 'inserted': 0, 'duplicates': 0, 'total': 0}

    operations = []
    for record in records:
        doc = dict(record)
        doc['_saved_at'] = datetime.now(timezone.utc).isoformat()
        doc['_last_run_id'] = run_id
        doc_id = _stable_record_identity(doc)
        doc['_id'] = doc_id
        operations.append(UpdateOne({'_id': doc_id}, {'$setOnInsert': doc}, upsert=True))

    client = None
    try:
        mongo_uri = _sanitize_mongo_uri_for_colab(CONFIG['mongo_uri'])
        client_kwargs: dict[str, Any] = {
            'appname': CONFIG['mongo_app_name'] or 'pip-water',
            'serverSelectionTimeoutMS': CONFIG['mongo_server_selection_timeout_ms'],
        }
        if certifi is not None:
            client_kwargs['tlsCAFile'] = certifi.where()

        client = MongoClient(mongo_uri, **client_kwargs)
        client.admin.command('ping')
        collection = client[CONFIG['mongo_db']][CONFIG['mongo_collection']]
        result = collection.bulk_write(operations, ordered=False)
        inserted = int(getattr(result, 'upserted_count', 0) or 0)
        total = len(operations)
        duplicates = total - inserted
        return {
            'enabled': True,
            'status': 'ok',
            'db': CONFIG['mongo_db'],
            'collection': CONFIG['mongo_collection'],
            'inserted': inserted,
            'duplicates': duplicates,
            'total': total,
        }
    except Exception as exc:
        error_text = str(exc)
        hint = None
        if 'SSL handshake failed' in error_text or 'tlsv1 alert' in error_text.lower():
            hint = (
                'Mongo TLS handshake failed. On Render verify: '
                '1) PYTHON_VERSION is stable (3.11/3.12), '
                '2) certifi is installed, '
                '3) Atlas Network Access allows Render egress (or 0.0.0.0/0 for test), '
                '4) MONGO_URI is correct and URL-encoded.'
            )
        return {
            'enabled': True,
            'status': 'error',
            'error': error_text,
            'hint': hint,
            'db': CONFIG['mongo_db'],
            'collection': CONFIG['mongo_collection'],
        }
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass


def save_to_tidb(df: pd.DataFrame, run_id: str) -> dict[str, Any]:
    if not CONFIG['tidb_enabled']:
        return {'enabled': False, 'status': 'disabled'}

    if pymysql is None:
        return {
            'enabled': True,
            'status': 'error',
            'error': 'pymysql is not installed. Install dependencies from requirements.txt.',
        }

    required = ['tidb_host', 'tidb_user', 'tidb_password', 'tidb_database', 'tidb_table']
    missing = [name for name in required if not CONFIG.get(name)]
    if missing:
        return {
            'enabled': True,
            'status': 'error',
            'error': f"Missing TiDB settings: {', '.join(missing)}",
        }

    records = json.loads(df.to_json(orient='records', force_ascii=False, date_format='iso'))
    if not records:
        return {'enabled': True, 'status': 'ok', 'written': 0, 'table_count': 0}

    ca_path = (CONFIG.get('tidb_ca_path') or '').strip()
    if ca_path and not os.path.exists(ca_path):
        if certifi is not None:
            ca_path = certifi.where()
        else:
            return {
                'enabled': True,
                'status': 'error',
                'error': f'TIDB_CA_PATH not found: {CONFIG.get("tidb_ca_path")}',
            }
    if not ca_path and 'tidbcloud' in str(CONFIG.get('tidb_host', '')).lower() and certifi is not None:
        ca_path = certifi.where()

    ssl_kwargs = {'ca': ca_path} if ca_path else None
    try:
        connection = pymysql.connect(
            host=CONFIG['tidb_host'],
            port=CONFIG['tidb_port'],
            user=CONFIG['tidb_user'],
            password=CONFIG['tidb_password'],
            autocommit=False,
            charset='utf8mb4',
            connect_timeout=CONFIG['tidb_connect_timeout'],
            read_timeout=30,
            write_timeout=30,
            ssl=ssl_kwargs,
        )
    except Exception as exc:
        return {
            'enabled': True,
            'status': 'error',
            'error': str(exc),
            'hint': (
                'TiDB connection failed. If running on Render leave TIDB_CA_PATH empty '
                'or rely on certifi fallback; verify host/user/password/database/table.'
            ),
        }

    create_database_sql = f"CREATE DATABASE IF NOT EXISTS `{CONFIG['tidb_database']}`"
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{CONFIG['tidb_table']}` (
            `id` VARCHAR(64) NOT NULL,
            `run_id` VARCHAR(32) NOT NULL,
            `source_file` VARCHAR(255) NOT NULL,
            `ingested_at` DATETIME(6) NULL,
            `saved_at` DATETIME(6) NOT NULL,
            `payload_json` JSON NOT NULL,
            PRIMARY KEY (`id`),
            KEY `idx_run_id` (`run_id`),
            KEY `idx_source_file` (`source_file`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
    """
    insert_sql = f"""
        INSERT INTO `{CONFIG['tidb_table']}`
            (`id`, `run_id`, `source_file`, `ingested_at`, `saved_at`, `payload_json`)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            `run_id` = VALUES(`run_id`),
            `source_file` = VALUES(`source_file`),
            `ingested_at` = VALUES(`ingested_at`),
            `saved_at` = VALUES(`saved_at`),
            `payload_json` = VALUES(`payload_json`)
    """

    rows = []
    saved_at = _utc_sql_datetime(None)
    for record in records:
        payload = dict(record)
        row_id = _stable_record_identity(payload)
        rows.append(
            (
                row_id,
                run_id,
                str(payload.get('_source_file', '')),
                _utc_sql_datetime(payload.get('_ingested_at')),
                saved_at,
                json.dumps(payload, ensure_ascii=False, default=str),
            )
        )

    try:
        with connection.cursor() as cursor:
            cursor.execute(create_database_sql)
            connection.select_db(CONFIG['tidb_database'])
            cursor.execute(create_table_sql)
            cursor.executemany(insert_sql, rows)
            connection.commit()
            cursor.execute(f"SELECT COUNT(*) FROM `{CONFIG['tidb_table']}`")
            table_count = int(cursor.fetchone()[0] or 0)

        return {
            'enabled': True,
            'status': 'ok',
            'db': CONFIG['tidb_database'],
            'table': CONFIG['tidb_table'],
            'written': len(rows),
            'table_count': table_count,
        }
    except Exception as exc:
        connection.rollback()
        return {
            'enabled': True,
            'status': 'error',
            'error': str(exc),
        }
    finally:
        connection.close()


def save_to_cratedb(df: pd.DataFrame, run_id: str) -> dict[str, Any]:
    if not CONFIG['cratedb_enabled']:
        return {'enabled': False, 'status': 'disabled'}

    if psycopg2 is None:
        return {
            'enabled': True,
            'status': 'error',
            'error': 'psycopg2-binary is not installed. Install dependencies from requirements.txt.',
        }

    required = ['cratedb_host', 'cratedb_user', 'cratedb_password', 'cratedb_database', 'cratedb_table']
    missing = [name for name in required if not CONFIG.get(name)]
    if missing:
        return {
            'enabled': True,
            'status': 'error',
            'error': f"Missing CrateDB settings: {', '.join(missing)}",
        }

    records = json.loads(df.to_json(orient='records', force_ascii=False, date_format='iso'))
    if not records:
        return {'enabled': True, 'status': 'ok', 'written': 0, 'table_count': 0}

    try:
        connection = psycopg2.connect(
            host=CONFIG['cratedb_host'],
            port=CONFIG['cratedb_port'],
            user=CONFIG['cratedb_user'],
            password=CONFIG['cratedb_password'],
            dbname=CONFIG['cratedb_database'],
            connect_timeout=CONFIG['tidb_connect_timeout'],
            sslmode=CONFIG['cratedb_sslmode'],
        )
    except Exception as exc:
        error_text = str(exc)
        hint = None
        if 'SSL SYSCALL error: EOF detected' in error_text or 'EOF detected' in error_text:
            hint = (
                'CrateDB SSL EOF. Check CRATEDB_HOST/PORT, ensure cluster is running, '
                'and verify network/IP allowlist and credentials on the provider side.'
            )
        return {
            'enabled': True,
            'status': 'error',
            'error': error_text,
            'hint': hint,
            'db': CONFIG['cratedb_database'],
            'table': CONFIG['cratedb_table'],
        }

    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {CONFIG['cratedb_table']} (
            id VARCHAR PRIMARY KEY,
            run_id VARCHAR,
            source_file VARCHAR,
            ingested_at TIMESTAMP WITH TIME ZONE,
            saved_at TIMESTAMP WITH TIME ZONE,
            payload_json TEXT
        )
    """
    insert_sql = f"""
        INSERT INTO {CONFIG['cratedb_table']}
            (id, run_id, source_file, ingested_at, saved_at, payload_json)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            run_id = excluded.run_id,
            source_file = excluded.source_file,
            ingested_at = excluded.ingested_at,
            saved_at = excluded.saved_at,
            payload_json = excluded.payload_json
    """

    rows = []
    saved_at = _utc_sql_datetime(None)
    for record in records:
        payload = dict(record)
        row_id = _stable_record_identity(payload)
        payload_json = json.dumps(payload, ensure_ascii=False, default=str)
        if len(payload_json) > 30000:
            payload_json = payload_json[:30000]
        rows.append(
            (
                row_id,
                run_id,
                str(payload.get('_source_file', '')),
                _utc_sql_datetime(payload.get('_ingested_at')),
                saved_at,
                payload_json,
            )
        )

    try:
        with connection.cursor() as cursor:
            cursor.execute(create_table_sql)
            cursor.executemany(insert_sql, rows)
            connection.commit()
            cursor.execute(f"SELECT COUNT(*) FROM {CONFIG['cratedb_table']}")
            table_count = int(cursor.fetchone()[0] or 0)
        return {
            'enabled': True,
            'status': 'ok',
            'db': CONFIG['cratedb_database'],
            'table': CONFIG['cratedb_table'],
            'written': len(rows),
            'table_count': table_count,
        }
    except Exception as exc:
        error_text = str(exc)
        hint = None
        if 'SSL SYSCALL error: EOF detected' in error_text or 'EOF detected' in error_text:
            hint = (
                'CrateDB write failed with SSL EOF. Confirm cluster is active and '
                'CRATEDB_HOST on Render matches the current cluster endpoint.'
            )
        return {
            'enabled': True,
            'status': 'error',
            'error': error_text,
            'hint': hint,
            'db': CONFIG['cratedb_database'],
            'table': CONFIG['cratedb_table'],
        }
    finally:
        connection.close()


# ============================================================
# Anomaly detection
# ============================================================


def _coerce_payload_dict(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode('utf-8', errors='ignore')
    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except Exception:
            return {}
    return {}


def _parse_numeric_liters(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except Exception:
            return None

    text = str(value).strip()
    if not text:
        return None

    text = text.replace('\u00a0', '').replace(' ', '')
    if ',' in text and '.' not in text:
        text = text.replace(',', '.')
    elif ',' in text and '.' in text:
        text = text.replace(',', '')

    try:
        return float(text)
    except Exception:
        return None

#transforma JSOSN em formato tabular, extrai contador, alias, device, timestamp e valor em litros, normaliza e retorna como lista de dicionarios
def _flatten_meter_payload(record: dict[str, Any], source_tag: str = '') -> list[dict[str, Any]]:
    header = record.get('header')
    body = record.get('body')
    if not isinstance(header, list) or not isinstance(body, list):
        return []

    columns = [str(col).strip() for col in header]
    rows: list[dict[str, Any]] = []
    for entry in body:
        if not isinstance(entry, (list, tuple)):
            continue
        mapped = {columns[idx]: entry[idx] for idx in range(min(len(columns), len(entry)))}
        device = str(
            mapped.get('Device')
            or mapped.get('device')
            or mapped.get('Contador')
            or mapped.get('contador')
            or mapped.get('Detalhes')
            or mapped.get('detalhes')
            or ''
        ).strip()
        alias = str(mapped.get('Alias') or mapped.get('alias') or mapped.get('Nome') or mapped.get('nome') or '').strip()
        timestamp_raw = (
            mapped.get('Date/Time')
            or mapped.get('timestamp')
            or mapped.get('Timestamp')
            or mapped.get('datahora')
            or mapped.get('Data e hora')
            or mapped.get('data e hora')
        )
        timestamp = pd.to_datetime(timestamp_raw, dayfirst=True, errors='coerce')
        value_l = _parse_numeric_liters(
            mapped.get('Valor (l)')
            or mapped.get('valor_l')
            or mapped.get('value_l')
            or mapped.get('Value (l)')
            or mapped.get('Value')
            or mapped.get('value')
        )
        contador = device or alias
        if not contador:
            continue
        rows.append(
            {
                'contador': contador,
                'device': device,
                'alias': alias,
                'timestamp': timestamp,
                'value_l': value_l,
                'source_file': record.get('_source_file', source_tag or ''),
                '_run_id': record.get('_run_id'),
            }
        )
    return rows


def _flatten_meter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=['contador', 'device', 'alias', 'timestamp', 'value_l', 'source_file', '_run_id'])

    flattened: list[dict[str, Any]] = []
    for record in df.to_dict(orient='records'):
        flattened.extend(_flatten_meter_payload(record))

    if not flattened:
        return pd.DataFrame(columns=['contador', 'device', 'alias', 'timestamp', 'value_l', 'source_file', '_run_id'])

    flat_df = pd.DataFrame(flattened)
    flat_df['timestamp'] = pd.to_datetime(flat_df['timestamp'], errors='coerce')
    return flat_df


def _load_tidb_history_for_anomalies(lookback_days: int) -> pd.DataFrame:

    local_history_source = str(CONFIG.get('anomaly_history_local_json', '') or '').strip()
    if local_history_source:
        local_path = Path(local_history_source)
        if not local_path.exists():
            return pd.DataFrame(columns=['contador', 'device', 'alias', 'timestamp', 'value_l', 'source_file', '_run_id'])

        try:
            payload = json.loads(local_path.read_text(encoding='utf-8'))
        except Exception:
            return pd.DataFrame(columns=['contador', 'device', 'alias', 'timestamp', 'value_l', 'source_file', '_run_id'])

        records = payload if isinstance(payload, list) else [payload]
        rows: list[dict[str, Any]] = []
        for record in records:
            if isinstance(record, dict):
                rows.extend(_flatten_meter_payload(record, source_tag=local_path.name))

        if not rows:
            return pd.DataFrame(columns=['contador', 'device', 'alias', 'timestamp', 'value_l', 'source_file', '_run_id'])

        history_df = pd.DataFrame(rows)
        history_df['timestamp'] = pd.to_datetime(history_df['timestamp'], errors='coerce')
        return history_df

    if not CONFIG['tidb_enabled'] or pymysql is None:
        return pd.DataFrame(columns=['contador', 'device', 'alias', 'timestamp', 'value_l', 'source_file', '_run_id'])

    required = ['tidb_host', 'tidb_user', 'tidb_password', 'tidb_database', 'tidb_table']
    missing = [name for name in required if not CONFIG.get(name)]
    if missing:
        return pd.DataFrame(columns=['contador', 'device', 'alias', 'timestamp', 'value_l', 'source_file', '_run_id'])

    ca_path = (CONFIG.get('tidb_ca_path') or '').strip()
    if ca_path and not os.path.exists(ca_path):
        if certifi is not None:
            ca_path = certifi.where()
        else:
            return pd.DataFrame(columns=['contador', 'device', 'alias', 'timestamp', 'value_l', 'source_file', '_run_id'])
    if not ca_path and 'tidbcloud' in str(CONFIG.get('tidb_host', '')).lower() and certifi is not None:
        ca_path = certifi.where()

    ssl_kwargs = {'ca': ca_path} if ca_path else None
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - pd.Timedelta(days=lookback_days)
    rows: list[dict[str, Any]] = []

    try:
        connection = pymysql.connect(
            host=CONFIG['tidb_host'],
            port=CONFIG['tidb_port'],
            user=CONFIG['tidb_user'],
            password=CONFIG['tidb_password'],
            autocommit=True,
            charset='utf8mb4',
            connect_timeout=CONFIG['tidb_connect_timeout'],
            read_timeout=30,
            write_timeout=30,
            ssl=ssl_kwargs,
        )
        connection.select_db(CONFIG['tidb_database'])
    except Exception:
        return pd.DataFrame(columns=['contador', 'device', 'alias', 'timestamp', 'value_l', 'source_file', '_run_id'])

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT `payload_json`, `saved_at`, `run_id`, `source_file`
                FROM `{CONFIG['tidb_table']}`
                WHERE `saved_at` >= %s
                ORDER BY `saved_at` ASC
                """,
                (cutoff,),
            )
            for payload_json, saved_at, run_id_value, source_file in cursor.fetchall():
                payload = _coerce_payload_dict(payload_json)
                if not payload:
                    continue
                payload['_db_saved_at'] = saved_at.isoformat() if saved_at else None
                payload['_run_id'] = run_id_value or payload.get('_run_id')
                payload['_source_file'] = source_file or payload.get('_source_file', '')
                rows.extend(_flatten_meter_payload(payload, source_tag=str(source_file or '')))
    except Exception:
        return pd.DataFrame(columns=['contador', 'device', 'alias', 'timestamp', 'value_l', 'source_file', '_run_id'])
    finally:
        connection.close()

    if not rows:
        return pd.DataFrame(columns=['contador', 'device', 'alias', 'timestamp', 'value_l', 'source_file', '_run_id'])

    history_df = pd.DataFrame(rows)
    history_df['timestamp'] = pd.to_datetime(history_df['timestamp'], errors='coerce')
    return history_df


def build_meter_anomaly_report(current_df: pd.DataFrame, run_id: str, lookback_days: int = 2) -> dict[str, Any]:
    threshold_ratio = float(CONFIG.get('anomaly_threshold_ratio', 0.8) or 0.8)
    upper_threshold_ratio = float(CONFIG.get('anomaly_upper_threshold_ratio', 1.2) or 1.2)
    history_load_seconds = 0.0
    current_flat = _flatten_meter_dataframe(current_df)

    if current_flat.empty:
        return {
            'status': 'skipped',
            'reason': 'No meter readings found in the current batch.',
            'lookback_days': lookback_days,
            'threshold_ratio': threshold_ratio,
            'upper_threshold_ratio': upper_threshold_ratio,
            'run_id': run_id,
        }

    current_flat = current_flat.dropna(subset=['timestamp']).copy()
    if current_flat.empty:
        return {
            'status': 'skipped',
            'reason': 'Current batch has no parseable timestamps.',
            'lookback_days': lookback_days,
            'threshold_ratio': threshold_ratio,
            'upper_threshold_ratio': upper_threshold_ratio,
            'run_id': run_id,
        }

    current_flat['day'] = current_flat['timestamp'].dt.floor('D')
    day_stats = (
        current_flat.groupby('day', as_index=False)
        .agg(
            readings=('contador', 'size'),
            counters=('contador', 'nunique'),
        )
        .sort_values('day')
        .reset_index(drop=True)
    )
    day_stats['avg_per_counter'] = day_stats['readings'] / day_stats['counters'].replace(0, pd.NA)

    current_day = day_stats['day'].iloc[-1]
    current_day_partial = False
    current_day_selected_from = str(current_day.date())

    if len(day_stats) >= 2:
        prev_stats = day_stats.iloc[:-1].tail(min(max(lookback_days, 2), 5)).copy()
        baseline_avg = float(prev_stats['avg_per_counter'].median() or 0.0)
        latest_avg = float(day_stats['avg_per_counter'].iloc[-1] or 0.0)

        # If the latest day has much fewer readings per counter than recent days, treat it as partial.
        if baseline_avg > 0 and latest_avg < baseline_avg * 0.5:
            valid_prev = day_stats.iloc[:-1].loc[day_stats.iloc[:-1]['avg_per_counter'] >= baseline_avg * 0.5]
            if not valid_prev.empty:
                current_day = valid_prev['day'].iloc[-1]
                current_day_partial = True
                current_day_selected_from = str(day_stats['day'].iloc[-1].date())

    current_day_flat = current_flat.loc[current_flat['day'] == current_day].copy()
    if current_day_flat.empty:
        return {
            'status': 'skipped',
            'reason': 'Current batch has no rows for the latest day.',
            'lookback_days': lookback_days,
            'threshold_ratio': threshold_ratio,
            'run_id': run_id,
        }

    current_counts = (
        current_day_flat.groupby('contador', as_index=False)
        .agg(
            device=('device', 'first'),
            alias=('alias', 'first'),
            current_count=('contador', 'size'),
            current_water_sum_l=('value_l', 'sum'),
        )
        .copy()
    )
    current_counts['current_water_sum_l'] = pd.to_numeric(current_counts['current_water_sum_l'], errors='coerce').fillna(0.0)

    history_load_started = time.perf_counter()
    history_flat = _load_tidb_history_for_anomalies(lookback_days)
    history_load_seconds = round(time.perf_counter() - history_load_started, 4)
    if history_flat.empty:
        return {
            'status': 'skipped',
            'reason': f'No TiDB history found for the last {lookback_days} day(s).',
            'lookback_days': lookback_days,
            'history_load_seconds': history_load_seconds,
            'threshold_ratio': threshold_ratio,
            'upper_threshold_ratio': upper_threshold_ratio,
            'run_id': run_id,
            'current_readings': int(len(current_flat)),
            'current_counters': int(len(current_counts)),
        }

    history_flat = history_flat.dropna(subset=['timestamp']).copy()
    if history_flat.empty:
        return {
            'status': 'skipped',
            'reason': 'Historical TiDB rows have no parseable timestamps.',
            'lookback_days': lookback_days,
            'history_load_seconds': history_load_seconds,
            'threshold_ratio': threshold_ratio,
            'upper_threshold_ratio': upper_threshold_ratio,
            'run_id': run_id,
            'current_readings': int(len(current_flat)),
            'current_counters': int(len(current_counts)),
        }

    history_flat['day'] = history_flat['timestamp'].dt.floor('D')
    history_start_day = current_day - pd.Timedelta(days=lookback_days)
    history_window = history_flat.loc[
        (history_flat['day'] >= history_start_day) & (history_flat['day'] < current_day)
    ].copy()
    if history_window.empty:
        return {
            'status': 'skipped',
            'reason': f'No historical rows available in the {lookback_days} day window before {current_day.date()}.',
            'lookback_days': lookback_days,
            'history_load_seconds': history_load_seconds,
            'threshold_ratio': threshold_ratio,
            'upper_threshold_ratio': upper_threshold_ratio,
            'run_id': run_id,
            'current_day': str(current_day.date()),
            'current_readings': int(len(current_day_flat)),
            'current_counters': int(len(current_counts)),
        }

    history_window['value_l'] = pd.to_numeric(history_window['value_l'], errors='coerce').fillna(0.0)
    history_daily = (
        history_window.groupby(['contador', 'day'], as_index=False)
        .agg(history_count=('contador', 'size'), history_water_l=('value_l', 'sum'))
    )
    history_daily_recent = (
        history_daily.sort_values(['contador', 'day'], ascending=[True, False])
        .groupby('contador', as_index=False)
        .head(lookback_days)
        .copy()
    )
    compared_history_days = sorted(
        {str(day.date()) for day in history_daily_recent['day'].dropna().unique()},
        reverse=True,
    )
    history_stats = (
        history_daily_recent.groupby('contador', as_index=False)
        .agg(
            history_avg=('history_count', 'mean'),
            history_median=('history_count', 'median'),
            history_min=('history_count', 'min'),
            history_max=('history_count', 'max'),
            history_days=('day', 'nunique'),
            history_avg_water_l=('history_water_l', 'mean'),
        )
        .copy()
    )

    comparison = current_counts.merge(history_stats, on='contador', how='left')
    comparison['has_history'] = comparison['history_avg'].notna()
    comparison['current_count'] = comparison['current_count'].fillna(0).astype(int)
    comparison['current_water_sum_l'] = comparison['current_water_sum_l'].fillna(0.0)
    comparison['expected_count'] = comparison['history_avg'].fillna(0.0)
    comparison['expected_water_l'] = comparison['history_avg_water_l'].fillna(0.0)
    comparison['count_ratio'] = (comparison['current_count'] / comparison['expected_count']).where(comparison['expected_count'] > 0)
    comparison['is_low_anomaly'] = (
        comparison['has_history']
        & comparison['expected_count'].gt(0)
        & comparison['current_count'].lt(comparison['expected_count'] * threshold_ratio)
    )
    comparison['is_high_anomaly'] = (
        comparison['has_history']
        & comparison['expected_count'].gt(0)
        & comparison['current_count'].gt(comparison['expected_count'] * upper_threshold_ratio)
    )
    comparison['is_anomaly'] = comparison['is_low_anomaly'] | comparison['is_high_anomaly']

    missing_history = comparison.loc[~comparison['has_history']].copy()
    missing_history = missing_history.sort_values(['contador'], ascending=[True])
    missing_history_details = [
        {
            'contador': row.get('contador', ''),
            'device': row.get('device', ''),
            'alias': row.get('alias', ''),
            'current_count': int(row.get('current_count') or 0),
            'history_missing': True,
            'flag': 'no_history_in_tidb',
        }
        for row in missing_history.to_dict(orient='records')
    ]

    flagged = comparison.loc[comparison['is_anomaly']].copy()
    flagged['anomaly_direction'] = flagged.apply(
        lambda row: 'above_expected' if bool(row.get('is_high_anomaly')) else 'below_expected',
        axis=1,
    )
    flagged['abs_delta_count'] = (flagged['current_count'] - flagged['expected_count']).abs()
    flagged = flagged.sort_values(['abs_delta_count', 'contador'], ascending=[False, True])

    details: list[dict[str, Any]] = []
    for row in flagged.to_dict(orient='records'):
        expected_count = round(float(row.get('expected_count') or 0.0), 2)
        current_count = int(row.get('current_count') or 0)
        delta_count = round(current_count - expected_count, 2)
        missing_count = round(max(0.0, expected_count - current_count), 2)
        excess_count = round(max(0.0, current_count - expected_count), 2)
        delta_percentage = round((abs(delta_count) / expected_count) * 100, 1) if expected_count else 0.0
        is_high = bool(row.get('is_high_anomaly'))
        anomaly_direction = 'above_expected' if is_high else 'below_expected'
        details.append(
            {
                'contador': row.get('contador', ''),
                'device': row.get('device', ''),
                'alias': row.get('alias', ''),
                'current_count': current_count,
                'current_count_n': current_count,
                'current_water_sum_l': round(float(row.get('current_water_sum_l') or 0.0), 2),
                'expected_count': expected_count,
                'expected_count_n': expected_count,
                'expected_water_l': round(float(row.get('expected_water_l') or 0.0), 2),
                'delta_count': delta_count,
                'delta_percentage': delta_percentage,
                'anomaly_direction': anomaly_direction,
                'missing_count': missing_count,
                'excess_count': excess_count,
                'lost_percentage': round((missing_count / expected_count) * 100, 1) if expected_count else 0.0,
                'excess_percentage': round((excess_count / expected_count) * 100, 1) if expected_count else 0.0,
                'expected_total_count': expected_count,
                'missing_total_count': missing_count,
                'lost_total_percentage': round((missing_count / expected_count) * 100, 1) if expected_count else 0.0,
                'excess_total_count': excess_count,
                'excess_total_percentage': round((excess_count / expected_count) * 100, 1) if expected_count else 0.0,
                'history_avg': round(float(row.get('history_avg') or 0.0), 2),
                'history_median': round(float(row.get('history_median') or 0.0), 2),
                'history_min': int(row.get('history_min') or 0),
                'history_max': int(row.get('history_max') or 0),
                'history_days': int(row.get('history_days') or 0),
                'history_missing': False,
                'missing_readings': missing_count,
            }
        )

    total_expected = float(comparison['expected_count'].sum() or 0.0)
    total_current = int(comparison['current_count'].sum() or 0)
    return {
        'status': 'ok',
        'source': 'tidb',
        'run_id': run_id,
        'lookback_days': lookback_days,
        'current_day': str(current_day.date()),
        'current_day_selected_from': current_day_selected_from,
        'current_day_partial': current_day_partial,
        'compared_history_days': compared_history_days,
        'history_load_seconds': history_load_seconds,
        'comparison_basis': f'current day vs average of previous {lookback_days} day(s)',
        'threshold_ratio': threshold_ratio,
        'upper_threshold_ratio': upper_threshold_ratio,
        'current_readings': int(len(current_day_flat)),
        'current_counters': int(len(current_counts)),
        'history_readings': int(len(history_window)),
        'history_counters': int(len(history_stats)),
        'expected_readings_total': round(total_expected, 2),
        'current_readings_total': total_current,
        'below_threshold_counters': int(comparison['is_low_anomaly'].sum()),
        'above_threshold_counters': int(comparison['is_high_anomaly'].sum()),
        'anomalous_counters': int(len(details)),
        'counters_without_history': int(len(missing_history_details)),
        'without_history_details': missing_history_details,
        'details': details,
    }

#função que define de onde vem os ficheiros JSON que a pipeline vai processar
def list_candidate_files() -> tuple[list[dict[str, str]], str]:
    files = list_from_json_file_urls() #ficheiros
    mode = 'JSON_FILE_URLS'
    if not files:
        files = list_from_local_dir()
        mode = 'LOCAL_JSON_DIR'
    if not files:
        files = list_from_known_names()
        mode = 'REPO_JSON_FILES'
    if not files:
        files = list_from_github(TOKEN) #a que esta a ser usada atualmente, pesquisa no github os ficheiros JSON
        mode = 'GITHUB_API'
    files = apply_date_window(files)
    if mode in {'REPO_JSON_FILES', 'GITHUB_API'}:
        files = limit_repo_files_randomly(files)
    return files, mode

#tabela das anomalias 
def _build_anomaly_table_html(anomaly_report: dict[str, Any] | None) -> str:
    if not anomaly_report:
        return ''

    status = anomaly_report.get('status', 'skipped')
    anomalous_count = int(anomaly_report.get('anomalous_counters', 0) or 0)
    missing_history_count = int(anomaly_report.get('counters_without_history', 0) or 0)
    current_day = str(anomaly_report.get('current_day') or '')
    compared_days = anomaly_report.get('compared_history_days', []) or []
    compared_days_text = ', '.join(str(day) for day in compared_days)
    comparison_days_html = (
        "<p style='margin:6px 0;color:#555'>"
        f"Dia analisado (ficheiro): <b>{ihtml.escape(current_day)}</b><br>"
        f"Dias comparados da BD: <b>{ihtml.escape(compared_days_text or '-')}</b>"
        "</p>"
    )
    if status != 'ok':
        reason = str(anomaly_report.get('reason') or 'anomaly report unavailable')
        return (
            "<p style='margin:12px 0 6px;color:#8a6d3b;font-weight:bold'>"
            f"Analise de anomalias indisponivel: {ihtml.escape(reason)}"
            "</p>"
        )

    if anomalous_count == 0 and missing_history_count == 0:
        lookback_days = int(anomaly_report.get('lookback_days', 2) or 2)
        current_day = str(anomaly_report.get('current_day') or '')
        day_text = f" no dia {ihtml.escape(current_day)}" if current_day else ''
        return (
            "<p style='margin:12px 0 6px;color:#2e7d32;font-weight:bold'>"
            f"Sem anomalias detetadas{day_text} (janela de {lookback_days} dia(s))."
            "</p>"
        ) + comparison_days_html

    if anomalous_count == 0 and missing_history_count > 0:
        details = anomaly_report.get('without_history_details', []) or []
        preview = ', '.join(str(item.get('contador', '')) for item in details[:10] if item.get('contador'))
        suffix = '...' if missing_history_count > 10 else ''
        return (
            "<p style='margin:12px 0 6px;color:#8a6d3b;font-weight:bold'>"
            f"Sem anomalias de contagem, mas {missing_history_count} contador(es) sem historico na TiDB"
            f" ({preview}{suffix})."
            "</p>"
        ) + comparison_days_html

    if anomalous_count <= 0:
        return ''

    details = anomaly_report.get('details', []) or []
    if not details:
        return ''

    top_details = details[:10]
    lookback_days = int(anomaly_report.get('lookback_days', 2) or 2)
    below_count = int(anomaly_report.get('below_threshold_counters', 0) or 0)
    above_count = int(anomaly_report.get('above_threshold_counters', 0) or 0)
    table_rows: list[str] = []
    for item in top_details:
        contador = str(item.get('contador', ''))
        expected_water = float(item.get('expected_water_l', item.get('expected_total_count', item.get('expected_count', 0))) or 0.0)
        current_water = float(item.get('current_water_sum_l', item.get('current_count', 0)) or 0.0)
        expected_n = int(round(float(item.get('expected_count_n', item.get('expected_count', 0)) or 0.0)))
        current_n = int(round(float(item.get('current_count_n', item.get('current_count', 0)) or 0.0)))
        direction = str(item.get('anomaly_direction', 'below_expected'))
        delta_water = round(current_water - expected_water, 2)
        if expected_water > 0:
            delta_water_pct = round((abs(delta_water) / expected_water) * 100, 1)
        else:
            delta_water_pct = 0.0 if current_water == 0 else 100.0
        delta_n = current_n - expected_n
        if expected_n > 0:
            delta_n_pct = round((abs(delta_n) / expected_n) * 100, 1)
        else:
            delta_n_pct = 0.0 if current_n == 0 else 100.0
        type_label = 'Acima do esperado' if direction == 'above_expected' else 'Abaixo do esperado'
        delta_color = '#f0ad4e' if direction == 'above_expected' else '#d9534f'
        delta_color_water = '#2e7d32' if delta_water == 0 and delta_water_pct == 0.0 else delta_color
        delta_color_n = '#2e7d32' if delta_n == 0 and delta_n_pct == 0.0 else delta_color
        type_label_water = 'Esperado' if delta_water_pct == 0.0 else type_label
        type_color_water = '#2e7d32' if delta_water_pct == 0.0 else delta_color_water
        type_label_n = 'Esperado' if delta_n_pct == 0.0 else type_label
        type_color_n = '#2e7d32' if delta_n_pct == 0.0 else delta_color_n
        delta_water_prefix = '+' if delta_water > 0 else ''
        delta_n_prefix = '+' if delta_n > 0 else ''

        table_rows.append(
            "<tr>"
            f"<td style='border:1px solid #ccc;padding:4px 6px;text-align:left'>{contador}</td>"
            f"<td style='border:1px solid #ccc;padding:4px 6px;text-align:right'>{expected_water:.2f}</td>"
            f"<td style='border:1px solid #ccc;padding:4px 6px;text-align:right'>{current_water:.2f}</td>"
            f"<td style='border:1px solid #ccc;padding:4px 6px;text-align:right;color:{delta_color_water}'>{delta_water_prefix}{delta_water:.2f}</td>"
            f"<td style='border:1px solid #ccc;padding:4px 6px;text-align:right;color:{delta_color_water}'>{delta_water_pct:.1f}%</td>"
            f"<td style='border:1px solid #ccc;padding:4px 6px;text-align:left;color:{type_color_water}'>{type_label_water}</td>"
            f"<td style='border:1px solid #ccc;padding:4px 6px;text-align:right'>{expected_n:d}</td>"
            f"<td style='border:1px solid #ccc;padding:4px 6px;text-align:right'>{current_n:d}</td>"
            f"<td style='border:1px solid #ccc;padding:4px 6px;text-align:right;color:{delta_color_n}'>{delta_n_prefix}{delta_n:d}</td>"
            f"<td style='border:1px solid #ccc;padding:4px 6px;text-align:right;color:{delta_color_n}'>{delta_n_pct:.1f}%</td>"
            f"<td style='border:1px solid #ccc;padding:4px 6px;text-align:left;color:{type_color_n}'>{type_label_n}</td>"
            "</tr>"
        )

    head = (
        "<tr>"
        "<th style='border:1px solid #ccc;padding:4px 6px;text-align:left;background:#f9f9f9;color:#d9534f'>Contador</th>"
        "<th style='border:1px solid #ccc;padding:4px 6px;text-align:right;background:#f9f9f9;color:#d9534f'>Contagem esperada</th>"
        "<th style='border:1px solid #ccc;padding:4px 6px;text-align:right;background:#f9f9f9;color:#d9534f'>Contagem lida</th>"
        "<th style='border:1px solid #ccc;padding:4px 6px;text-align:right;background:#f9f9f9;color:#d9534f'>Diferença</th>"
        "<th style='border:1px solid #ccc;padding:4px 6px;text-align:right;background:#f9f9f9;color:#d9534f'>Variação(%)</th>"
        "<th style='border:1px solid #ccc;padding:4px 6px;text-align:left;background:#f9f9f9;color:#d9534f'>Tipo</th>"
        f"<th style='border:1px solid #ccc;padding:4px 6px;text-align:right;background:#f9f9f9;color:#d9534f'>N leituras esperadas (media {lookback_days}d)</th>"
        "<th style='border:1px solid #ccc;padding:4px 6px;text-align:right;background:#f9f9f9;color:#d9534f'>N leituras lidas</th>"
        "<th style='border:1px solid #ccc;padding:4px 6px;text-align:right;background:#f9f9f9;color:#d9534f'>Diferença</th>"
        "<th style='border:1px solid #ccc;padding:4px 6px;text-align:right;background:#f9f9f9;color:#d9534f'>Variação(%)</th>"
        "<th style='border:1px solid #ccc;padding:4px 6px;text-align:left;background:#f9f9f9;color:#d9534f'>Tipo</th>"
        "</tr>"
    )

    content = ''
    if anomalous_count > 0 and table_rows:
        table_html = (
            "<table style='border-collapse:collapse;font-family:Arial,Helvetica,sans-serif;font-size:12px;color:#222;margin-top:12px'>"
            + head
            + ''.join(table_rows)
            + "</table>"
        )
        intro = (
            "<p style='margin:12px 0 6px;color:#d9534f;font-weight:bold'>"
            f"{anomalous_count} contador(es) fora da media dos {lookback_days} dias anteriores "
            f"(abaixo: {below_count}, acima: {above_count}):"
            "</p>"
        )
        content += intro + comparison_days_html + table_html

    if missing_history_count > 0:
        missing_items = anomaly_report.get('without_history_details', []) or []
        preview = ', '.join(str(item.get('contador', '')) for item in missing_items[:10] if item.get('contador'))
        suffix = '...' if missing_history_count > 10 else ''
        content += (
            "<p style='margin:10px 0 0;color:#8a6d3b;font-weight:bold'>"
            f"{missing_history_count} contador(es) sem historico na TiDB"
            f" ({preview}{suffix})."
            "</p>"
        )

    return content


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _build_profile_rows_from_result(result: dict[str, Any]) -> list[tuple[str, float, float]]:
    phase_seconds = ((result.get('performance') or {}).get('phase_seconds') or {})
    rows: list[tuple[str, float, float]] = []

    rows.append(('Pipeline started', 0.0, 0.0))

    cumulative = 0.0
    steps = [
        ('file_discovery', 'File discovery'),
        ('download_and_normalize', 'Download and normalize'),
        ('transform', 'Transform'),
        ('save_outputs', 'Save outputs'),
        ('save_mongodb', 'Save MongoDB'),
        ('save_tidb', 'Save TiDB'),
        ('save_cratedb', 'Save CrateDB'),
    ]

    for key, label in steps:
        cumulative += _safe_float(phase_seconds.get(key, 0.0))
        rows.append((label, round(cumulative, 2), round(cumulative, 2)))

        if key == 'save_mongodb':
            mongo = result.get('mongodb') or {}
            if mongo.get('enabled', False):
                if mongo.get('status') == 'ok':
                    rows.append(
                        (
                            f"... [mongodb] {mongo.get('collection', CONFIG['mongo_collection'])}: "
                            f"inserted {mongo.get('inserted', 0)}, skipped {mongo.get('duplicates', 0)}",
                            round(cumulative, 2),
                            round(cumulative, 2),
                        )
                    )
                elif mongo.get('status') == 'skipped':
                    rows.append(('... [mongodb] skipped', round(cumulative, 2), round(cumulative, 2)))
                else:
                    rows.append(
                        (
                            f"... [mongodb] error: {mongo.get('error', 'unknown')}",
                            round(cumulative, 2),
                            round(cumulative, 2),
                        )
                    )

        if key == 'save_tidb':
            tidb = result.get('tidb') or {}
            if tidb.get('enabled', False):
                if tidb.get('status') == 'ok':
                    rows.append(
                        (
                            f"... [tidb] {tidb.get('table', CONFIG['tidb_table'])}: "
                            f"written {tidb.get('written', 0)}, rows in table {tidb.get('table_count', 0)}",
                            round(cumulative, 2),
                            round(cumulative, 2),
                        )
                    )
                elif tidb.get('status') == 'skipped':
                    rows.append(('... [tidb] skipped', round(cumulative, 2), round(cumulative, 2)))
                else:
                    rows.append(
                        (
                            f"... [tidb] error: {tidb.get('error', 'unknown')}",
                            round(cumulative, 2),
                            round(cumulative, 2),
                        )
                    )

        if key == 'save_cratedb':
            cratedb = result.get('cratedb') or {}
            if cratedb.get('enabled', False):
                if cratedb.get('status') == 'ok':
                    rows.append(
                        (
                            f"... [cratedb] {cratedb.get('table', CONFIG['cratedb_table'])}: "
                            f"written {cratedb.get('written', 0)}, rows in table {cratedb.get('table_count', 0)}",
                            round(cumulative, 2),
                            round(cumulative, 2),
                        )
                    )
                elif cratedb.get('status') == 'skipped':
                    rows.append(('... [cratedb] skipped', round(cumulative, 2), round(cumulative, 2)))
                else:
                    rows.append(
                        (
                            f"... [cratedb] error: {cratedb.get('error', 'unknown')}",
                            round(cumulative, 2),
                            round(cumulative, 2),
                        )
                    )

    anomalies = result.get('anomalies') or {}
    history_load_seconds = _safe_float(anomalies.get('history_load_seconds', 0.0))
    if history_load_seconds > 0:
        rows.append(
            (
                f"... [anomaly] TiDB history read: {history_load_seconds:.2f}s",
                round(cumulative, 2),
                round(cumulative, 2),
            )
        )

    overall = round(cumulative, 2)
    rows.append(('Overall pipeline', overall, overall))

    elapsed = round(_safe_float(result.get('elapsed_seconds', overall)), 2)
    rows.append(('Overall (before email):', elapsed, elapsed))
    return rows


def _build_profile_table_html(rows: list[tuple[str, float, float]]) -> str:
    table_rows: list[str] = []
    for task, watch_secs, proc_secs in rows:
        table_rows.append(
            "<tr>"
            f"<td style='border:1px solid #666;padding:4px 6px;text-align:left'>{ihtml.escape(task)}</td>"
            f"<td style='border:1px solid #666;padding:4px 6px;text-align:right'>{watch_secs:.2f}</td>"
            f"<td style='border:1px solid #666;padding:4px 6px;text-align:right'>{proc_secs:.2f}</td>"
            "</tr>"
        )

    head = (
        "<tr>"
        "<th style='border:1px solid #666;padding:4px 6px;text-align:left;background:#f2f2f2'>Task(s)</th>"
        "<th style='border:1px solid #666;padding:4px 6px;text-align:right;background:#f2f2f2'>watch time (secs)</th>"
        "<th style='border:1px solid #666;padding:4px 6px;text-align:right;background:#f2f2f2'>proc time (secs)</th>"
        "</tr>"
    )

    return (
        "<table style='border-collapse:collapse;font-family:Arial,Helvetica,sans-serif;font-size:13px;color:#222'>"
        + head
        + ''.join(table_rows)
        + "</table>"
    )


def _send_email_via_brevo(subject: str, text_body: str, html_body: str, recipients: list[str]) -> dict[str, Any]:
    api_key = CONFIG.get('brevo_api_key', '').strip()
    sender_email = (CONFIG.get('brevo_sender_email') or '').strip()
    sender_name = (CONFIG.get('brevo_sender_name') or 'PIP Water').strip()

    if not api_key:
        return {'status': 'error', 'error': 'BREVO_API_KEY is missing.'}
    if not sender_email:
        return {'status': 'error', 'error': 'BREVO_SENDER_EMAIL is missing.'}
    if not recipients:
        return {'status': 'error', 'error': 'No recipients configured.'}

    payload = {
        'sender': {'name': sender_name, 'email': sender_email},
        'to': [{'email': item} for item in recipients],
        'subject': subject,
        'textContent': text_body,
        'htmlContent': html_body,
    }

    response = requests.post(
        'https://api.brevo.com/v3/smtp/email',
        headers={
            'accept': 'application/json',
            'content-type': 'application/json',
            'api-key': api_key,
        },
        json=payload,
        timeout=CONFIG['request_timeout'],
    )

    if response.ok:
        print("email enviado via brevo")
        return {'status': 'ok', 'provider': 'brevo', 'response': response.json() if response.content else {}}

    return {
        'status': 'error',
        'provider': 'brevo',
        'error': f'{response.status_code} {response.text[:500]}',
    }


def _send_email_via_smtp(subject: str, text_body: str, html_body: str, recipients: list[str]) -> dict[str, Any]:
    if not CONFIG['email_from'] or not CONFIG['email_username'] or not CONFIG['email_password']:
        return {'status': 'error', 'error': 'SMTP credentials are incomplete.'}

    message = MIMEMultipart('alternative')
    message['Subject'] = subject
    message['From'] = CONFIG['email_from']
    message['To'] = ', '.join(recipients)
    message.attach(MIMEText(text_body, 'plain'))
    message.attach(MIMEText(html_body, 'html'))

    ssl_context = ssl.create_default_context()
    smtp_host = CONFIG['email_smtp_host']
    smtp_port = CONFIG['email_smtp_port']

    if smtp_port == 465:
        with smtplib.SMTP_SSL(smtp_host, smtp_port, context=ssl_context) as server:
            server.login(CONFIG['email_username'], CONFIG['email_password'])
            server.sendmail(CONFIG['email_from'], recipients, message.as_string())
    else:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls(context=ssl_context)
            server.login(CONFIG['email_username'], CONFIG['email_password'])
            server.sendmail(CONFIG['email_from'], recipients, message.as_string())

    return {'status': 'ok', 'provider': 'smtp', 'smtp_host': smtp_host, 'smtp_port': smtp_port}


# ============================================================
# Email summary
# ============================================================


def send_email_summary(result: dict[str, Any]) -> dict[str, Any]:
    if not CONFIG['email_enabled']:
        return {'status': 'skipped', 'reason': 'EMAIL_ENABLED is false'}

    recipients = [item.strip() for item in CONFIG['email_to'].split(',') if item.strip()]
    if not recipients:
        return {'status': 'skipped', 'reason': 'EMAIL_TO has no recipients'}
    if not CONFIG['email_from'] or not CONFIG['email_username'] or not CONFIG['email_password']:
        if CONFIG.get('email_backend') != 'brevo' or not CONFIG.get('brevo_api_key'):
            return {'status': 'skipped', 'reason': 'email credentials are incomplete'}

    status = result.get('status', 'unknown')
    runtime_label = str(result.get('runtime') or detect_runtime() or 'unknown').strip().lower()
    subject = f"[PIP Water | {runtime_label}] {status.upper()} | {result.get('run_id', 'N/A')}"
    anomalies = result.get('anomalies', {}) or {}
    anom_count = int(anomalies.get('anomalous_counters', 0) or 0)
    anom_below_count = int(anomalies.get('below_threshold_counters', 0) or 0)
    anom_above_count = int(anomalies.get('above_threshold_counters', 0) or 0)
    profile_rows = _build_profile_rows_from_result(result)
    profile_table_html = _build_profile_table_html(profile_rows)
    anomaly_table_html = _build_anomaly_table_html(anomalies)

    text_lines = [
        f"Environment: {runtime_label}",
        f"Run ID: {result.get('run_id', 'N/A')}",
        f"Status: {status}",
        "",
    ]
    text_lines.extend(f"{task} | watch: {w:.2f} | proc: {p:.2f}" for task, w, p in profile_rows)

    if anom_count > 0:
        text_lines.append(
            f"\n[ANOMALIAS] {anom_count} contador(es) fora do esperado nos ultimos 2 dias "
            f"(abaixo: {anom_below_count}, acima: {anom_above_count})"
        )

    error_text = result.get('error')
    if error_text:
        text_lines.append(f"Error: {error_text}")

    text_body = "\n".join(text_lines) + "\nEsta é uma mensagem automática."

    html_body = (
        "<html><body style='font-family:Montserrat,Arial,sans-serif'>"
        f"<p><b>Environment:</b> {ihtml.escape(runtime_label)}</p>"
        f"<p><b>Run ID:</b> {ihtml.escape(str(result.get('run_id', 'N/A')))}</p>"
        f"<p><b>Status:</b> {ihtml.escape(str(status).upper())}</p>"
        + profile_table_html
    )
    if anomaly_table_html:
        html_body += "<hr color=orange>" + anomaly_table_html
    if error_text:
        html_body += f"<hr color=orange><p style='color:#b00020'><b>Error:</b> {ihtml.escape(str(error_text))}</p>"
    html_body += "<hr color=orange>"
    html_body += "This message is an automated notification from PIP Water script/Flask pipeline"
    html_body += "</body></html>"

    preferred_backend = (CONFIG.get('email_backend') or 'smtp').strip().lower()
    if preferred_backend == 'brevo' or runtime_label == 'render' or is_render():
        send_result = _send_email_via_brevo(subject, text_body, html_body, recipients)
        if send_result.get('status') != 'ok' and preferred_backend == 'brevo' and is_render():
            raise RuntimeError(send_result.get('error', 'Brevo email send failed.'))
        if send_result.get('status') != 'ok':
            raise RuntimeError(send_result.get('error', 'Email send failed.'))
        return send_result

    send_result = _send_email_via_smtp(subject, text_body, html_body, recipients)
    if send_result.get('status') != 'ok':
        raise RuntimeError(send_result.get('error', 'SMTP email send failed.'))
    return send_result


# ============================================================
# Pipeline execution
# ============================================================


def run_pipeline() -> dict[str, Any]:
    started = time.perf_counter()
    phase_started = started
    phase_seconds: dict[str, float] = {}
    context = PipelineContext(
        runtime=detect_runtime(),
        run_id=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ'),
        started_at=datetime.now(timezone.utc).isoformat(),
    )

    def close_phase(name: str) -> None:
        nonlocal phase_started
        now = time.perf_counter()
        phase_seconds[name] = round(now - phase_started, 4)
        phase_started = now

    try:
        files, mode = list_candidate_files()
        close_phase('file_discovery')

        max_files = int(CONFIG.get('max_files', 0) or 0)
        if max_files > 0:
            files = files[:max_files]

        if not files:
            days_back = int(CONFIG.get('days_back', 0) or 0)
            start_date = str(CONFIG.get('start_date', ''))
            if days_back > 0:
                raise RuntimeError(
                    f'No JSON files found to process for start_date={start_date} and days_back={days_back}. '
                    'Adjust the date window or set DAYS_BACK=0 to disable date filtering.'
                )
            raise RuntimeError('No JSON files found to process.')

        if CONFIG['verbose']:
            print('Runtime:', context.runtime)
            print('Read mode:', mode)
            print('Files selected:', len(files))

        all_records: list[dict[str, Any]] = []
        files_ok = 0
        files_error = 0

        for item in files:
            try:
                data = read_json_source(item['source'], TOKEN)
                all_records.extend(normalize_records(data, item['name'], context.run_id))
                files_ok += 1
                if CONFIG['verbose']:
                    print(f"  OK  {item['name']}")
            except Exception as exc:
                files_error += 1
                print(f"  ERR {item['name']}: {exc}")

        close_phase('download_and_normalize')

        df = pd.DataFrame(all_records)
        df = light_clean(df)
        close_phase('transform')

        anomaly_report = build_meter_anomaly_report(
            df,
            context.run_id,
            lookback_days=int(CONFIG.get('anomaly_lookback_days', 2) or 2),
        )
        anomaly_path = Path(CONFIG['output_folder']) / 'water_anomaly_report.json'
        Path(CONFIG['output_folder']).mkdir(parents=True, exist_ok=True)
        anomaly_path.write_text(json.dumps(anomaly_report, indent=2, ensure_ascii=False), encoding='utf-8')

        outputs = save_outputs(df)
        outputs['anomaly_report_path'] = str(anomaly_path)
        close_phase('save_outputs')

        if CONFIG.get('skip_db_writes', False):
            mongo_result = {'enabled': False, 'status': 'skipped', 'reason': 'SKIP_DB_WRITES=true'}
            tidb_result = {'enabled': False, 'status': 'skipped', 'reason': 'SKIP_DB_WRITES=true'}
            cratedb_result = {'enabled': False, 'status': 'skipped', 'reason': 'SKIP_DB_WRITES=true'}
            close_phase('save_mongodb')
            close_phase('save_tidb')
            close_phase('save_cratedb')
        else:
            mongo_result = save_to_mongodb(df, context.run_id)
            close_phase('save_mongodb')

            tidb_result = save_to_tidb(df, context.run_id)
            close_phase('save_tidb')

            cratedb_result = save_to_cratedb(df, context.run_id)
            close_phase('save_cratedb')

        elapsed = round(time.perf_counter() - started, 2)
        result = {
            'status': 'ok',
            'runtime': context.runtime,
            'run_id': context.run_id,
            'started_at_utc': context.started_at,
            'mode': mode,
            'files_selected': len(files),
            'files_ok': files_ok,
            'files_error': files_error,
            'records': len(df),
            'elapsed_seconds': elapsed,
            'performance': {'phase_seconds': phase_seconds},
            'output': outputs,
            'mongodb': mongo_result,
            'tidb': tidb_result,
            'cratedb': cratedb_result,
            'anomalies': anomaly_report,
            'sample': df.head(5).to_dict(orient='records'),
        }

        Path(outputs['result_path']).write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding='utf-8')
        return result

    except Exception as exc:
        elapsed = round(time.perf_counter() - started, 2)
        result = {
            'status': 'error',
            'runtime': context.runtime,
            'run_id': context.run_id,
            'started_at_utc': context.started_at,
            'error': str(exc),
            'hint': (
                'Use GITHUB_TOKEN com permissao de leitura no repo privado, ou define '
                'LOCAL_JSON_DIR, JSON_FILE_URLS, ou REPO_JSON_FILES.'
            ),
            'elapsed_seconds': elapsed,
            'performance': {'phase_seconds': phase_seconds},
        }
        result_path = Path(CONFIG['output_folder']) / 'water_result.json'
        result_path.parent.mkdir(parents=True, exist_ok=True)
        result_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding='utf-8')
        return result


    # ============================================================
    # Script entry point
    # ============================================================


if __name__ == '__main__':
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
