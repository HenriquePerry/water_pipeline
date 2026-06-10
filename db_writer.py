"""Database persistence layer (MongoDB, TiDB, CrateDB)."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from config import CONFIG
from env_utils import is_colab

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


def _doc_hash(document: dict[str, Any]) -> str:
    payload = json.dumps(document, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def stable_record_identity(record: dict[str, Any]) -> str:
    identity = dict(record)
    for volatile_key in ('_ingested_at', '_run_id', '_saved_at', '_last_run_id'):
        identity.pop(volatile_key, None)
    return _doc_hash(identity)


def utc_sql_datetime(value: str | datetime | None) -> datetime:
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


def resolve_tidb_ssl_kwargs() -> dict[str, str] | None:
    ca_path = (CONFIG.get('tidb_ca_path') or '').strip()
    if ca_path and not os.path.exists(ca_path):
        if certifi is not None:
            ca_path = certifi.where()
        else:
            return None
    if not ca_path and 'tidbcloud' in str(CONFIG.get('tidb_host', '')).lower() and certifi is not None:
        ca_path = certifi.where()
    return {'ca': ca_path} if ca_path else None


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
        doc_id = stable_record_identity(doc)
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

    ssl_kwargs = resolve_tidb_ssl_kwargs()
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
    saved_at = utc_sql_datetime(None)
    for record in records:
        payload = dict(record)
        row_id = stable_record_identity(payload)
        rows.append(
            (
                row_id,
                run_id,
                str(payload.get('_source_file', '')),
                utc_sql_datetime(payload.get('_ingested_at')),
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
    saved_at = utc_sql_datetime(None)
    for record in records:
        payload = dict(record)
        row_id = stable_record_identity(payload)
        payload_json = json.dumps(payload, ensure_ascii=False, default=str)
        if len(payload_json) > 30000:
            payload_json = payload_json[:30000]
        rows.append(
            (
                row_id,
                run_id,
                str(payload.get('_source_file', '')),
                utc_sql_datetime(payload.get('_ingested_at')),
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


__all__ = [
    'pymysql',
    'certifi',
    'resolve_tidb_ssl_kwargs',
    'save_to_cratedb',
    'save_to_mongodb',
    'save_to_tidb',
    'stable_record_identity',
    'utc_sql_datetime',
]
