"""Environment and runtime utility helpers for the PIP Water project."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def _env_bool(name: str, default: str = 'false') -> bool:
    return os.getenv(name, default).strip().lower() in {'1', 'true', 'yes', 'on'}


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
