# -*- coding: utf-8 -*-
"""pip_water.py"""

import hashlib
import html as ihtml
import json
import os
import re
import smtplib
import ssl
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


def detect_runtime() -> str:
    if is_colab():
        return 'google-colab'
    if os.getenv('AIRFLOW_HOME') or os.getenv('AIRFLOW_CTX_DAG_ID'):
        return 'airflow'
    if os.getenv('FLASK_ENV') or os.getenv('WERKZEUG_RUN_MAIN'):
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
    'anomaly_lookback_days': int(os.getenv('ANOMALY_LOOKBACK_DAYS', '2')),
    'max_files': int(os.getenv('MAX_FILES', '0')),
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
# GitHub authentication and discovery
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


def raw_url(file_path: str, branch: str) -> str:
    return f"https://raw.githubusercontent.com/{CONFIG['repo_owner']}/{CONFIG['repo_name']}/{branch}/{file_path}"


def branch_candidates() -> list[str]:
    preferred = CONFIG['repo_branch'].strip()
    values = [preferred, 'main', 'master']
    unique: list[str] = []
    for value in values:
        if value and value not in unique:
            unique.append(value)
    return unique


def list_from_json_file_urls() -> list[dict[str, str]]:
    items = [u.strip() for u in CONFIG['json_file_urls'].split(',') if u.strip()]
    return [{'name': Path(item).name, 'source': item} for item in items]


def list_from_local_dir() -> list[dict[str, str]]:
    local_dir = CONFIG['local_json_dir'].strip()
    if not local_dir:
        return []
    folder = Path(local_dir)
    if not folder.exists():
        return []
    return [{'name': p.name, 'source': str(p)} for p in sorted(folder.glob('*.json'))]


def list_from_known_names() -> list[dict[str, str]]:
    names = [n.strip() for n in CONFIG['repo_json_files'].split(',') if n.strip()]
    chosen_branch = branch_candidates()[0]
    return [
        {'name': Path(name).name, 'source': raw_url(f"{CONFIG['repo_folder']}/{name}", chosen_branch)} for name in names
    ]


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
        return {
            'enabled': True,
            'status': 'error',
            'error': str(exc),
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
        if is_colab() and certifi is not None:
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
            'hint': 'No Colab usa TIDB_CA_PATH vazio ou /content/... e deixa o script usar certifi automaticamente.',
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
        return {
            'enabled': True,
            'status': 'error',
            'error': str(exc),
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
        return {
            'enabled': True,
            'status': 'error',
            'error': str(exc),
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
            or ''
        ).strip()
        alias = str(mapped.get('Alias') or mapped.get('alias') or '').strip()
        timestamp_raw = mapped.get('Date/Time') or mapped.get('timestamp') or mapped.get('Timestamp') or mapped.get('datahora')
        timestamp = pd.to_datetime(timestamp_raw, dayfirst=True, errors='coerce')
        contador = device or alias
        if not contador:
            continue
        rows.append(
            {
                'contador': contador,
                'device': device,
                'alias': alias,
                'timestamp': timestamp,
                'source_file': record.get('_source_file', source_tag or ''),
                '_run_id': record.get('_run_id'),
            }
        )
    return rows


def _flatten_meter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=['contador', 'device', 'alias', 'timestamp', 'source_file', '_run_id'])

    flattened: list[dict[str, Any]] = []
    for record in df.to_dict(orient='records'):
        flattened.extend(_flatten_meter_payload(record))

    if not flattened:
        return pd.DataFrame(columns=['contador', 'device', 'alias', 'timestamp', 'source_file', '_run_id'])

    flat_df = pd.DataFrame(flattened)
    flat_df['timestamp'] = pd.to_datetime(flat_df['timestamp'], errors='coerce')
    return flat_df


def _load_tidb_history_for_anomalies(lookback_days: int) -> pd.DataFrame:
    if not CONFIG['tidb_enabled'] or pymysql is None:
        return pd.DataFrame(columns=['contador', 'device', 'alias', 'timestamp', 'source_file', '_run_id'])

    required = ['tidb_host', 'tidb_user', 'tidb_password', 'tidb_database', 'tidb_table']
    missing = [name for name in required if not CONFIG.get(name)]
    if missing:
        return pd.DataFrame(columns=['contador', 'device', 'alias', 'timestamp', 'source_file', '_run_id'])

    ca_path = (CONFIG.get('tidb_ca_path') or '').strip()
    if ca_path and not os.path.exists(ca_path):
        if is_colab() and certifi is not None:
            ca_path = certifi.where()
        else:
            return pd.DataFrame(columns=['contador', 'device', 'alias', 'timestamp', 'source_file', '_run_id'])
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
        return pd.DataFrame(columns=['contador', 'device', 'alias', 'timestamp', 'source_file', '_run_id'])

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
        return pd.DataFrame(columns=['contador', 'device', 'alias', 'timestamp', 'source_file', '_run_id'])
    finally:
        connection.close()

    if not rows:
        return pd.DataFrame(columns=['contador', 'device', 'alias', 'timestamp', 'source_file', '_run_id'])

    history_df = pd.DataFrame(rows)
    history_df['timestamp'] = pd.to_datetime(history_df['timestamp'], errors='coerce')
    return history_df


def build_meter_anomaly_report(current_df: pd.DataFrame, run_id: str, lookback_days: int = 2) -> dict[str, Any]:
    threshold_ratio = float(CONFIG.get('anomaly_threshold_ratio', 0.8) or 0.8)
    current_flat = _flatten_meter_dataframe(current_df)

    if current_flat.empty:
        return {
            'status': 'skipped',
            'reason': 'No meter readings found in the current batch.',
            'lookback_days': lookback_days,
            'threshold_ratio': threshold_ratio,
            'run_id': run_id,
        }

    current_flat = current_flat.dropna(subset=['timestamp']).copy()
    if current_flat.empty:
        return {
            'status': 'skipped',
            'reason': 'Current batch has no parseable timestamps.',
            'lookback_days': lookback_days,
            'threshold_ratio': threshold_ratio,
            'run_id': run_id,
        }

    current_counts = (
        current_flat.groupby('contador', as_index=False)
        .agg(device=('device', 'first'), alias=('alias', 'first'), current_count=('contador', 'size'))
        .copy()
    )

    history_flat = _load_tidb_history_for_anomalies(lookback_days)
    if history_flat.empty:
        return {
            'status': 'skipped',
            'reason': f'No TiDB history found for the last {lookback_days} days.',
            'lookback_days': lookback_days,
            'threshold_ratio': threshold_ratio,
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
            'threshold_ratio': threshold_ratio,
            'run_id': run_id,
            'current_readings': int(len(current_flat)),
            'current_counters': int(len(current_counts)),
        }

    history_flat['day'] = history_flat['timestamp'].dt.floor('D')
    history_daily = history_flat.groupby(['contador', 'day'], as_index=False).size().rename(columns={'size': 'history_count'})
    history_stats = (
        history_daily.groupby('contador', as_index=False)
        .agg(
            history_avg=('history_count', 'mean'),
            history_median=('history_count', 'median'),
            history_min=('history_count', 'min'),
            history_max=('history_count', 'max'),
            history_days=('day', 'nunique'),
        )
        .copy()
    )

    comparison = history_stats.merge(current_counts, on='contador', how='left')
    comparison['current_count'] = comparison['current_count'].fillna(0).astype(int)
    comparison['expected_count'] = comparison['history_avg'].fillna(0.0)
    comparison['ratio'] = comparison.apply(
        lambda row: round(row['current_count'] / row['expected_count'], 3) if row['expected_count'] else None,
        axis=1,
    )
    comparison['is_anomaly'] = comparison['expected_count'].gt(0) & comparison['current_count'].lt(
        comparison['expected_count'] * threshold_ratio
    )

    flagged = comparison.loc[comparison['is_anomaly']].copy()
    flagged = flagged.sort_values(['ratio', 'current_count', 'contador'], ascending=[True, True, True])

    details: list[dict[str, Any]] = []
    for row in flagged.to_dict(orient='records'):
        details.append(
            {
                'contador': row.get('contador', ''),
                'device': row.get('device', ''),
                'alias': row.get('alias', ''),
                'current_count': int(row.get('current_count') or 0),
                'expected_count': round(float(row.get('expected_count') or 0.0), 2),
                'history_avg': round(float(row.get('history_avg') or 0.0), 2),
                'history_median': round(float(row.get('history_median') or 0.0), 2),
                'history_min': int(row.get('history_min') or 0),
                'history_max': int(row.get('history_max') or 0),
                'history_days': int(row.get('history_days') or 0),
                'ratio': row.get('ratio'),
                'missing_readings': max(
                    0,
                    int(round(float(row.get('expected_count') or 0.0) - float(row.get('current_count') or 0))),
                ),
            }
        )

    total_expected = float(comparison['expected_count'].sum() or 0.0)
    total_current = int(comparison['current_count'].sum() or 0)
    return {
        'status': 'ok',
        'source': 'tidb',
        'run_id': run_id,
        'lookback_days': lookback_days,
        'threshold_ratio': threshold_ratio,
        'current_readings': int(len(current_flat)),
        'current_counters': int(len(current_counts)),
        'history_readings': int(len(history_flat)),
        'history_counters': int(len(history_stats)),
        'expected_readings_total': round(total_expected, 2),
        'current_readings_total': total_current,
        'anomalous_counters': int(len(details)),
        'details': details,
    }


def list_candidate_files() -> tuple[list[dict[str, str]], str]:
    files = list_from_json_file_urls()
    mode = 'JSON_FILE_URLS'
    if not files:
        files = list_from_local_dir()
        mode = 'LOCAL_JSON_DIR'
    if not files:
        files = list_from_known_names()
        mode = 'REPO_JSON_FILES'
    if not files:
        files = list_from_github(TOKEN)
        mode = 'GITHUB_API'
    files = apply_date_window(files)
    return files, mode


def _build_anomaly_table_html(anomaly_report: dict[str, Any] | None) -> str:
    if not anomaly_report:
        return ''

    status = anomaly_report.get('status', 'skipped')
    anomalous_count = int(anomaly_report.get('anomalous_counters', 0) or 0)
    if status != 'ok' or anomalous_count == 0:
        return ''

    details = anomaly_report.get('details', []) or []
    if not details:
        return ''

    top_details = details[:10]
    table_rows: list[str] = []
    for item in top_details:
        contador = str(item.get('contador', ''))
        expected = int(round(float(item.get('expected_count', 0) or 0)))
        current = int(item.get('current_count', 0) or 0)
        ratio = item.get('ratio')
        ratio_str = f"{float(ratio) * 100:.1f}%" if ratio is not None else '-'
        missing = int(item.get('missing_readings', max(0, expected - current)) or 0)

        table_rows.append(
            "<tr>"
            f"<td style='border:1px solid #ccc;padding:4px 6px;text-align:left'>{contador}</td>"
            f"<td style='border:1px solid #ccc;padding:4px 6px;text-align:right'>{expected}</td>"
            f"<td style='border:1px solid #ccc;padding:4px 6px;text-align:right'>{current}</td>"
            f"<td style='border:1px solid #ccc;padding:4px 6px;text-align:right;color:#b00020'>{ratio_str}</td>"
            f"<td style='border:1px solid #ccc;padding:4px 6px;text-align:right;color:#d9534f'>-{missing}</td>"
            "</tr>"
        )

    head = (
        "<tr>"
        "<th style='border:1px solid #ccc;padding:4px 6px;text-align:left;background:#f9f9f9;color:#d9534f'>Contador</th>"
        "<th style='border:1px solid #ccc;padding:4px 6px;text-align:right;background:#f9f9f9;color:#d9534f'>Esperado</th>"
        "<th style='border:1px solid #ccc;padding:4px 6px;text-align:right;background:#f9f9f9;color:#d9534f'>Atual</th>"
        "<th style='border:1px solid #ccc;padding:4px 6px;text-align:right;background:#f9f9f9;color:#d9534f'>Racio %</th>"
        "<th style='border:1px solid #ccc;padding:4px 6px;text-align:right;background:#f9f9f9;color:#d9534f'>Faltas</th>"
        "</tr>"
    )

    table_html = (
        "<table style='border-collapse:collapse;font-family:Arial,Helvetica,sans-serif;font-size:12px;color:#222;margin-top:12px'>"
        + head
        + ''.join(table_rows)
        + "</table>"
    )

    intro = (
        "<p style='margin:12px 0 6px;color:#d9534f;font-weight:bold'>"
        f"{anomalous_count} contador(es) com menos leituras do que o esperado (ultimos 2 dias):"
        "</p>"
    )
    return intro + table_html


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


# ============================================================
# Email summary
# ============================================================


def send_email_summary(result: dict[str, Any]) -> None:
    if not CONFIG['email_enabled']:
        return

    recipients = [item.strip() for item in CONFIG['email_to'].split(',') if item.strip()]
    if not recipients:
        return
    if not CONFIG['email_from'] or not CONFIG['email_username'] or not CONFIG['email_password']:
        return

    status = result.get('status', 'unknown')
    subject = f"[PIP Water] {status.upper()} | {result.get('run_id', 'N/A')}"
    anomalies = result.get('anomalies', {}) or {}
    anom_count = int(anomalies.get('anomalous_counters', 0) or 0)
    profile_rows = _build_profile_rows_from_result(result)
    profile_table_html = _build_profile_table_html(profile_rows)
    anomaly_table_html = _build_anomaly_table_html(anomalies)

    text_lines = [f"{task} | watch: {w:.2f} | proc: {p:.2f}" for task, w, p in profile_rows]

    if anom_count > 0:
        text_lines.append(f"\n[ANOMALIAS] {anom_count} contador(es) com problema nos ultimos 2 dias")

    error_text = result.get('error')
    if error_text:
        text_lines.append(f"Error: {error_text}")

    text_body = "\n".join(text_lines) + "\nEsta é uma mensagem automática."

    html_body = "<html><body style='font-family:Montserrat,Arial,sans-serif'>" + profile_table_html
    if anomaly_table_html:
        html_body += "<hr color=orange>" + anomaly_table_html
    if error_text:
        html_body += f"<hr color=orange><p style='color:#b00020'><b>Error:</b> {ihtml.escape(str(error_text))}</p>"
    html_body += "<hr color=orange>"
    html_body += "This message is an automated notification from PIP Water script/Flask pipeline"
    html_body += "</body></html>"

    message = MIMEMultipart('alternative')
    message['Subject'] = subject
    message['From'] = CONFIG['email_from']
    message['To'] = ', '.join(recipients)
    message.attach(MIMEText(text_body, 'plain'))
    message.attach(MIMEText(html_body, 'html'))

    ssl_context = ssl.create_default_context()
    if CONFIG['email_smtp_port'] == 465:
        with smtplib.SMTP_SSL(CONFIG['email_smtp_host'], CONFIG['email_smtp_port'], context=ssl_context) as server:
            server.login(CONFIG['email_username'], CONFIG['email_password'])
            server.sendmail(CONFIG['email_from'], recipients, message.as_string())
    else:
        with smtplib.SMTP(CONFIG['email_smtp_host'], CONFIG['email_smtp_port']) as server:
            server.starttls(context=ssl_context)
            server.login(CONFIG['email_username'], CONFIG['email_password'])
            server.sendmail(CONFIG['email_from'], recipients, message.as_string())


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
        send_email_summary(result)
    except Exception as exc:
        print('Falha no envio de email:', exc)
