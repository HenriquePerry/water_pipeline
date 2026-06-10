"""Pipeline orchestration module."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from config import CONFIG, PipelineContext
from data_parser import TOKEN, build_meter_anomaly_report, light_clean, list_candidate_files, normalize_records, read_json_source, save_outputs
from db_writer import save_to_cratedb, save_to_mongodb, save_to_tidb
from env_utils import detect_runtime
from notifier import send_email_summary


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


__all__ = ['CONFIG', 'list_candidate_files', 'run_pipeline', 'send_email_summary']
