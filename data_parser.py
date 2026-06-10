"""Data discovery, parsing, transformation and anomaly helpers."""

from __future__ import annotations

import json
import random
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from config import CONFIG
from env_utils import is_colab

try:
    import pymysql
except Exception:  # pragma: no cover - optional dependency at runtime
    pymysql = None  # type: ignore[assignment]

try:
    import certifi
except Exception:  # pragma: no cover - optional dependency at runtime
    certifi = None  # type: ignore[assignment]


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
    import os

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


def limit_repo_files_randomly(files: list[dict[str, str]]) -> list[dict[str, str]]:
    sample_size = int(CONFIG.get('repo_sample_size', 30) or 30)
    if sample_size <= 0 or len(files) <= sample_size:
        return files
    return random.sample(files, sample_size)


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
    if mode in {'REPO_JSON_FILES', 'GITHUB_API'}:
        files = limit_repo_files_randomly(files)
    return files, mode


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
    if ca_path and not Path(ca_path).exists():
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


__all__ = [
    'TOKEN',
    'apply_date_window',
    'build_meter_anomaly_report',
    'headers',
    'light_clean',
    'list_candidate_files',
    'list_from_github',
    'list_from_json_file_urls',
    'list_from_known_names',
    'list_from_local_dir',
    'normalize_records',
    'read_json_source',
    'save_outputs',
]
