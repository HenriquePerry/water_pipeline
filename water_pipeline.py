import json
import logging
import os
import smtplib
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv


load_dotenv()


@dataclass
class PipelineConfig:
    repo_owner: str = os.getenv("REPO_OWNER", "pedroccpimenta")
    repo_name: str = os.getenv("REPO_NAME", "datafiles")
    repo_folder: str = os.getenv("REPO_FOLDER", "aqualog")
    repo_branch: str = os.getenv("REPO_BRANCH", "master")
    repo_json_files: str = os.getenv("REPO_JSON_FILES", "")
    output_folder: str = os.getenv("OUTPUT_FOLDER", "./output_water")
    request_timeout: int = int(os.getenv("REQUEST_TIMEOUT", "30"))
    json_file_urls: str = os.getenv("JSON_FILE_URLS", "")
    email_enabled: bool = os.getenv("EMAIL_ENABLED", "false").lower() == "true"
    email_smtp_host: str = os.getenv("EMAIL_SMTP_HOST", "smtp.gmail.com")
    email_smtp_port: int = int(os.getenv("EMAIL_SMTP_PORT", "587"))
    email_from: str = os.getenv("EMAIL_FROM", "")
    email_to: str = os.getenv("EMAIL_TO", "")
    email_username: str = os.getenv("EMAIL_USERNAME", "")
    email_password: str = os.getenv("EMAIL_PASSWORD", "")


def _is_colab() -> bool:
    try:
        import google.colab  # type: ignore

        return google.colab is not None
    except Exception:
        return False


def _load_json_secret(secret_name: str) -> dict[str, Any] | None:
    if not _is_colab():
        return None

    try:
        from google.colab import userdata  # type: ignore

        raw = userdata.get(secret_name)
        if not raw:
            return None
        return json.loads(raw)
    except Exception:
        return None


def get_github_token() -> str | None:
    token = (os.getenv("GITHUB_TOKEN") or "").strip()
    if token:
        return token

    acronym = os.getenv("USER_ACRONYM", "").strip()
    custom_secret = os.getenv("GITHUB_SECRET_NAME", "").strip()

    candidate_names = []
    if custom_secret:
        candidate_names.append(custom_secret)
    if acronym:
        candidate_names.extend([
            f"{acronym}-github.json",
            f"{acronym}-github_token.json",
        ])
    candidate_names.append("GITHUB_TOKEN")

    for secret_name in candidate_names:
        secret_payload = _load_json_secret(secret_name)
        if not secret_payload:
            continue

        token_value = secret_payload.get("key") or secret_payload.get("token")
        if token_value:
            return str(token_value)

    return None


def _build_headers(token: str | None) -> dict[str, str]:
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _build_raw_url(config: PipelineConfig, file_path: str) -> str:
    return (
        f"https://raw.githubusercontent.com/{config.repo_owner}/"
        f"{config.repo_name}/{config.repo_branch}/{file_path}"
    )


def _list_json_files_from_known_names(config: PipelineConfig) -> list[dict[str, Any]]:
    names = [n.strip() for n in config.repo_json_files.split(",") if n.strip()]
    if not names:
        return []

    return [
        {
            "name": Path(name).name,
            "path": f"{config.repo_folder}/{name}",
            "download_url": _build_raw_url(config, f"{config.repo_folder}/{name}"),
        }
        for name in names
    ]


def _list_json_files(config: PipelineConfig, token: str | None) -> list[dict[str, Any]]:
    # Preferred fallback: explicit file names from env, avoiding GitHub folder listing.
    known_files = _list_json_files_from_known_names(config)
    if known_files:
        return known_files

    url = f"https://api.github.com/repos/{config.repo_owner}/{config.repo_name}/contents/{config.repo_folder}"
    params = {"ref": config.repo_branch} if config.repo_branch else None
    response = requests.get(url, headers=_build_headers(token), params=params, timeout=config.request_timeout)
    if response.ok:
        payload = response.json()
        return [
            item
            for item in payload
            if item.get("type") == "file" and str(item.get("name", "")).endswith(".json")
        ]

    tree_url = f"https://github.com/{config.repo_owner}/{config.repo_name}/tree/{config.repo_branch}/{config.repo_folder}"
    tree_response = requests.get(tree_url, timeout=config.request_timeout)
    tree_response.raise_for_status()

    html = tree_response.text
    pattern = rf'href="/{re.escape(config.repo_owner)}/{re.escape(config.repo_name)}/blob/{re.escape(config.repo_branch)}/{re.escape(config.repo_folder)}/([^"]+?\.json)"'
    matches = re.findall(pattern, html)

    if not matches:
        response.raise_for_status()

    return [
        {
            "name": file_name.split("/")[-1],
            "path": f"{config.repo_folder}/{file_name}",
            "download_url": _build_raw_url(config, f"{config.repo_folder}/{file_name}"),
        }
        for file_name in sorted(set(matches))
    ]


def _read_json(url: str, token: str | None, timeout: int) -> Any:
    if url.startswith("http://") or url.startswith("https://"):
        response = requests.get(url, headers=_build_headers(token), timeout=timeout)
        response.raise_for_status()
        return response.json()

    path = Path(url)
    if not path.exists():
        raise FileNotFoundError(f"Ficheiro JSON local não encontrado: {url}")
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _normalize_records(data: Any, source_file: str, run_id: str) -> list[dict[str, Any]]:
    ingested_at = datetime.now(timezone.utc).isoformat()

    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = [data]
    else:
        rows = [{"value": data}]

    output: list[dict[str, Any]] = []
    for row in rows:
        record = dict(row) if isinstance(row, dict) else {"value": row}
        record["_source_file"] = source_file
        record["_ingested_at"] = ingested_at
        record["_run_id"] = run_id
        output.append(record)
    return output


def _light_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    dedup_key = df.apply(lambda row: json.dumps(row.to_dict(), sort_keys=True, default=str), axis=1)
    df = df.loc[~dedup_key.duplicated()].copy()

    for col in ["timestamp", "time", "datetime", "datahora", "date"]:
        if col in df.columns:
            parsed = pd.to_datetime(df[col], errors="coerce", utc=True)
            df[col] = parsed.dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    return df


def _save_outputs(df: pd.DataFrame, output_folder: str) -> dict[str, str]:
    output_dir = Path(output_folder)
    output_dir.mkdir(parents=True, exist_ok=True)

    json_path = output_dir / "water_data_raw.json"
    csv_path = output_dir / "water_data_clean.csv"

    df.to_json(json_path, orient="records", force_ascii=False)
    df.to_csv(csv_path, index=False)

    return {
        "json_path": str(json_path),
        "csv_path": str(csv_path),
    }


def _send_email_report(config: PipelineConfig, result: dict[str, Any], logger: logging.Logger) -> None:
    if not config.email_enabled or not config.email_from or not config.email_to:
        return

    try:
        status = result.get("status", "unknown")
        run_id = result.get("run_id", "N/A")
        elapsed = result.get("elapsed_seconds", 0)
        files = result.get("files_processed", 0)
        records = result.get("records", 0)
        error = result.get("error", "")

        subject = f"[Water Pipeline] {status.upper()} - {run_id}"
        
        text_body = f"""
Pipeline de Agua - Resumo da Execução
======================================

Status: {status.upper()}
Run ID: {run_id}
Data/Hora: {datetime.now(timezone.utc).isoformat()}

Métricas:
  ✓ Ficheiros processados: {files}
  ✓ Registos inseridos: {records}
  ✓ Tempo total: {elapsed}s

"""
        if error:
            text_body += f"Erro: {error}\n"

        text_body += "\n---\nMensagem automática da pipeline de água (Aqualog)."

        html_body = f"""
<html>
  <body style="font-family: Arial, sans-serif; color: #333;">
    <h2>Pipeline de Agua - Resumo</h2>
    <p><strong>Status:</strong> <span style="color: {'green' if status == 'ok' else 'red'};">{status.upper()}</span></p>
    <p><strong>Run ID:</strong> {run_id}</p>
    <p><strong>Data:</strong> {datetime.now(timezone.utc).isoformat()}</p>
    
    <h3>Métricas:</h3>
    <ul>
      <li>Ficheiros processados: <strong>{files}</strong></li>
      <li>Registos inseridos: <strong>{records}</strong></li>
      <li>Tempo total: <strong>{elapsed}s</strong></li>
    </ul>
"""
        if error:
            html_body += f"<p style='color: red;'><strong>Erro:</strong> {error}</p>"

        html_body += "<hr><p style='font-size: 0.8em; color: #999;'>Mensagem automática</p></body></html>"

        message = MIMEMultipart("alternative")
        message["Subject"] = subject
        message["From"] = config.email_from
        message["To"] = config.email_to

        message.attach(MIMEText(text_body, "plain"))
        message.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(config.email_smtp_host, config.email_smtp_port) as server:
            server.starttls()
            server.login(config.email_username, config.email_password)
            server.sendmail(config.email_from, [config.email_to], message.as_string())

        logger.info("Email enviado com sucesso para %s", config.email_to)

    except Exception as exc:
        logger.warning("Falha ao enviar email: %s", exc)



def run_pipeline(config: PipelineConfig | None = None) -> dict[str, Any]:
    if config is None:
        config = PipelineConfig()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    logger = logging.getLogger("water_pipeline")

    started = time.perf_counter()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    token = get_github_token()

    manual_urls = [u.strip() for u in config.json_file_urls.split(",") if u.strip()]

    all_records: list[dict[str, Any]] = []
    files_processed = 0

    try:
        if manual_urls:
            logger.info("Modo URL/ficheiro direto ativo com %s entradas.", len(manual_urls))
            for url in manual_urls:
                file_name = Path(url).name if not (url.startswith("http://") or url.startswith("https://")) else (url.split("/")[-1] or "unknown.json")
                data = _read_json(url, token, config.request_timeout)
                records = _normalize_records(data, file_name, run_id)
                all_records.extend(records)
                files_processed += 1
        else:
            logger.info(
                "Listagem GitHub: %s/%s/%s (branch=%s)",
                config.repo_owner,
                config.repo_name,
                config.repo_folder,
                config.repo_branch,
            )
            files = _list_json_files(config, token)
            for file_info in files:
                file_name = file_info["name"]
                download_url = file_info.get("download_url")
                if not download_url:
                    download_url = (
                        f"https://raw.githubusercontent.com/{config.repo_owner}/"
                        f"{config.repo_name}/{config.repo_branch}/{file_info['path']}"
                    )
                data = _read_json(download_url, token, config.request_timeout)
                records = _normalize_records(data, file_name, run_id)
                all_records.extend(records)
                files_processed += 1

        df = pd.DataFrame(all_records)
        df = _light_cleaning(df)
        outputs = _save_outputs(df, config.output_folder)

        elapsed = round(time.perf_counter() - started, 2)
        result = {
            "status": "ok",
            "run_id": run_id,
            "files_processed": files_processed,
            "records": len(df),
            "elapsed_seconds": elapsed,
            "output": outputs,
            "sample": df.head(5).to_dict(orient="records"),
        }
        logger.info("Pipeline concluida: %s", result)
        _send_email_report(config, result, logger)
        return result

    except Exception as exc:
        elapsed = round(time.perf_counter() - started, 2)
        logger.exception("Falha na pipeline")
        hint = (
            "Dica: se a listagem do repo no GitHub falhar (404), preenche JSON_FILE_URLS "
            "com URLs raw diretas ou caminhos locais para ficheiros .json separados por vírgula."
        )
        result = {
            "status": "error",
            "run_id": run_id,
            "error": str(exc),
            "hint": hint,
            "elapsed_seconds": elapsed,
            "files_processed": files_processed,
        }
        _send_email_report(config, result, logger)
        return result


if __name__ == "__main__":
    print(json.dumps(run_pipeline(), indent=2, ensure_ascii=False))
