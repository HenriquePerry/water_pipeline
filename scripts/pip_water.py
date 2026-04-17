# -*- coding: utf-8 -*-
"""pip_water.py"""

import json
import hashlib
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


def _env_bool(name: str, default: str = 'false') -> bool:
    return os.getenv(name, default).strip().lower() in {'1', 'true', 'yes', 'on'}


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


if not is_colab():
    try:
        from dotenv import find_dotenv, load_dotenv

        dotenv_file = find_dotenv('.env', usecwd=True)
        if dotenv_file:
            load_dotenv(dotenv_file, override=True)
        else:
            #procura o .env na pasta acima (é o meu caso)
            script_dir = Path(__file__).resolve().parent
            for candidate in (script_dir / '.env', script_dir.parent / '.env'):
                if candidate.exists():
                    load_dotenv(candidate, override=True)
                    break
    except Exception:
        pass


CONFIG = {
    'repo_owner': os.getenv('REPO_OWNER', 'pedroccpimenta'),
    'repo_name': os.getenv('REPO_NAME', 'datafiles'),
    'repo_folder': os.getenv('REPO_FOLDER', 'aqualog'),
    'repo_branch': os.getenv('REPO_BRANCH', 'master'),
    'request_timeout': int(os.getenv('REQUEST_TIMEOUT', '30')),
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

    'mongo_uri': os.getenv('MONGO_URI', '').strip(),
    'mongo_enabled': _env_bool('MONGO_ENABLED', 'false') or bool(os.getenv('MONGO_URI', '').strip()),
    'mongo_db': os.getenv('MONGO_DB', 'pi_water').strip(),
    'mongo_collection': os.getenv('MONGO_COLLECTION', 'water_records').strip(),
    'mongo_app_name': os.getenv('MONGO_APP_NAME', 'pip-water').strip(),
    'mongo_server_selection_timeout_ms': int(os.getenv('MONGO_SERVER_SELECTION_TIMEOUT_MS', '10000')),
}

print('Runtime:', detect_runtime())
print('Repo target:', f"{CONFIG['repo_owner']}/{CONFIG['repo_name']}/{CONFIG['repo_folder']}")
print('Output folder:', CONFIG['output_folder'])
print('Mongo enabled:', CONFIG['mongo_enabled']) #flag que indica se quero usar o mongo
print('Mongo URI set:', bool(CONFIG['mongo_uri'])) #flag que indica se a URI do mongo está configurada


@dataclass
class PipelineContext:
    runtime: str
    run_id: str
    started_at: str

#####################################################################
##---------------------Obter GITHUB_TOKEN -------------------------##


##--Tenta obter segredo do ambiente do Colab-----------------------##
def load_colab_secret(secret_name: str): 
    if not is_colab():
        return None
    try:
        from google.colab import userdata  #tenta interpretar segredo como JSON
        raw = userdata.get(secret_name)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception: #se não for JSON, retorna como string simples
            return {'token': str(raw).strip()}
    except Exception:
        return None


def get_github_token() -> str | None:
    token = (os.getenv('GITHUB_TOKEN') or '').strip()
    if token: #1 try: variavel de ambiente GITHUB_TOKEN
        return token

    candidates = []
    if CONFIG['github_secret_name']: #2 try: nomes possiveis de secrets
        candidates.append(CONFIG['github_secret_name'])
    candidates.extend([
        "GITHUB_TOKEN",
        'github_token',
        'TO-github.json',
        'TO-github_token.json',
        'github_token.json',
    ])

    for secret_name in candidates: #tenta obter um por um do ambiente do Colab
        payload = load_colab_secret(secret_name)
        if not payload:
            continue
        value = payload.get('key') or payload.get('token')
        if value:
            return str(value).strip()

    return None

##--controi os haders HTTP para chamdas API do GITHUB
def headers(token: str | None) -> dict[str, str]:
    h = {'Accept': 'application/vnd.github+json'}
    if token:
        h['Authorization'] = f'Bearer {token}'
    return h

TOKEN = get_github_token()
print('GitHub token presente:', bool(TOKEN))

##-------------------------------------------------------------------##





def raw_url(file_path: str, branch: str) -> str:
    return f"https://raw.githubusercontent.com/{CONFIG['repo_owner']}/{CONFIG['repo_name']}/{branch}/{file_path}"




#identificador da branch do repositorio-------------------------------------------------------------------------
def branch_candidates() -> list[str]:
    preferred = CONFIG['repo_branch'].strip()
    values = [preferred, 'main', 'master']
    unique: list[str] = []
    for value in values: #remove duplicados
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
        {'name': Path(name).name, 'source': raw_url(f"{CONFIG['repo_folder']}/{name}", chosen_branch)}
        for name in names
    ]


def list_from_github(token: str | None) -> list[dict[str, str]]:
    last_error: str | None = None

    for branch in branch_candidates():
        api_url = f"https://api.github.com/repos/{CONFIG['repo_owner']}/{CONFIG['repo_name']}/contents/{CONFIG['repo_folder']}"
        response = requests.get(
            api_url,
            headers=headers(token),
            params={'ref': branch},
            timeout=CONFIG['request_timeout'],
        )

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
        doc_id = _doc_hash(doc)
        doc['_id'] = doc_id
        operations.append(UpdateOne({'_id': doc_id}, {'$setOnInsert': doc}, upsert=True))

    client = MongoClient(
        CONFIG['mongo_uri'],
        appname=CONFIG['mongo_app_name'] or 'pip-water',
        serverSelectionTimeoutMS=CONFIG['mongo_server_selection_timeout_ms'],
    )
    try:
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
    finally:
        client.close()


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

    text_body = (
        f"Status: {status}\n"
        f"Run ID: {result.get('run_id', 'N/A')}\n"
        f"Mode: {result.get('mode', 'N/A')}\n"
        f"Files selected: {result.get('files_selected', 0)}\n"
        f"Files OK: {result.get('files_ok', 0)}\n"
        f"Files error: {result.get('files_error', 0)}\n"
        f"Records: {result.get('records', 0)}\n"
        f"Elapsed: {result.get('elapsed_seconds', 0)}s\n"
        f"Output JSON: {result.get('output', {}).get('json_path', 'N/A')}\n"
        f"Output CSV: {result.get('output', {}).get('csv_path', 'N/A')}\n"
    )

    error_text = result.get('error')
    if error_text:
        text_body += f"Error: {error_text}\n"

    html_body = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #222;">
        <h3>PIP Water - Resumo da Execução</h3>
        <p><b>Status:</b> {status.upper()}</p>
        <p><b>Run ID:</b> {result.get('run_id', 'N/A')}</p>
        <p><b>Mode:</b> {result.get('mode', 'N/A')}</p>
        <p><b>Files selected:</b> {result.get('files_selected', 0)}</p>
        <p><b>Files OK:</b> {result.get('files_ok', 0)}</p>
        <p><b>Files error:</b> {result.get('files_error', 0)}</p>
        <p><b>Records:</b> {result.get('records', 0)}</p>
        <p><b>Elapsed:</b> {result.get('elapsed_seconds', 0)}s</p>
        <p><b>Output JSON:</b> {result.get('output', {}).get('json_path', 'N/A')}</p>
        <p><b>Output CSV:</b> {result.get('output', {}).get('csv_path', 'N/A')}</p>
        {f'<p style="color:#b00020;"><b>Error:</b> {error_text}</p>' if error_text else ''}
      </body>
    </html>
    """

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

        outputs = save_outputs(df)
        close_phase('save_outputs')

        mongo_result = save_to_mongodb(df, context.run_id)
        close_phase('save_mongodb')

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
            'sample': df.head(5).to_dict(orient='records'),
        }

        Path(outputs['result_path']).write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding='utf-8')
        return result

    except Exception as exc:
        elapsed = round(time.perf_counter() - started, 2)
        return {
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


if __name__ == '__main__':
    print('=' * 60)
    print('PIP WATER | NOTEBOOK')
    print('=' * 60)
    result = run_pipeline()
    print(json.dumps(result, indent=2, ensure_ascii=False))
    send_email_summary(result)
