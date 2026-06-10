"""Configuration and runtime context for the PIP Water project."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone

from env_utils import (
	_env_bool,
	_env_or_default,
	_local_default,
	detect_runtime,
	env_or_colab_secret,
	is_colab,
	is_render,
	load_dotenv_if_available,
)

load_dotenv_if_available()

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
github_secret_name_value = env_or_colab_secret('GITHUB_SECRET_NAME', '').strip()
if not github_secret_name_value:
	user_prefix = env_or_colab_secret('USER', '').strip()
	if user_prefix:
		github_secret_name_value = f'{user_prefix}_github_json'

brevo_api_key_value = env_or_colab_secret('BREVO_API_KEY', '').strip()
brevo_sender_name_value = env_or_colab_secret('BREVO_SENDER_NAME', '').strip()
brevo_sender_email_value = env_or_colab_secret('BREVO_SENDER_EMAIL', '').strip()
email_backend_default = 'brevo' if (is_render() or (brevo_api_key_value and brevo_sender_email_value)) else 'smtp'

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
	'email_enabled': str(env_or_colab_secret('EMAIL_ENABLED', 'false')).strip().lower() in {'1', 'true', 'yes', 'on'},
	'email_smtp_host': env_or_colab_secret('EMAIL_SMTP_HOST', 'smtp.gmail.com'),
	'email_smtp_port': int(env_or_colab_secret('EMAIL_SMTP_PORT', '587')),
	'email_from': env_or_colab_secret('EMAIL_FROM', ''),
	'email_to': env_or_colab_secret('EMAIL_TO', ''),
	'email_username': env_or_colab_secret('EMAIL_USERNAME', ''),
	'email_password': env_or_colab_secret('EMAIL_PASSWORD', ''),
	'email_backend': env_or_colab_secret('EMAIL_BACKEND', _env_or_default('EMAIL_BACKEND', email_backend_default)),
	'brevo_api_key': brevo_api_key_value,
	'brevo_sender_name': brevo_sender_name_value,
	'brevo_sender_email': brevo_sender_email_value,
	'github_secret_name': github_secret_name_value,
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


__all__ = ['CONFIG', 'PipelineContext']
