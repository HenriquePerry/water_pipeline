"""Notification and email summary utilities."""

from __future__ import annotations

import html as ihtml
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

import requests

from config import CONFIG
from env_utils import detect_runtime, is_render


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
		'</p>'
	)
	if status != 'ok':
		reason = str(anomaly_report.get('reason') or 'anomaly report unavailable')
		return (
			"<p style='margin:12px 0 6px;color:#8a6d3b;font-weight:bold'>"
			f"Analise de anomalias indisponivel: {ihtml.escape(reason)}"
			'</p>'
		)

	if anomalous_count == 0 and missing_history_count == 0:
		lookback_days = int(anomaly_report.get('lookback_days', 2) or 2)
		day_text = f" no dia {ihtml.escape(current_day)}" if current_day else ''
		return (
			"<p style='margin:12px 0 6px;color:#2e7d32;font-weight:bold'>"
			f"Sem anomalias detetadas{day_text} (janela de {lookback_days} dia(s))."
			'</p>'
		) + comparison_days_html

	if anomalous_count == 0 and missing_history_count > 0:
		details = anomaly_report.get('without_history_details', []) or []
		preview = ', '.join(str(item.get('contador', '')) for item in details[:10] if item.get('contador'))
		suffix = '...' if missing_history_count > 10 else ''
		return (
			"<p style='margin:12px 0 6px;color:#8a6d3b;font-weight:bold'>"
			f"Sem anomalias de contagem, mas {missing_history_count} contador(es) sem historico na TiDB"
			f" ({preview}{suffix})."
			'</p>'
		) + comparison_days_html

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
		delta_water_pct = round((abs(delta_water) / expected_water) * 100, 1) if expected_water > 0 else (0.0 if current_water == 0 else 100.0)
		delta_n = current_n - expected_n
		delta_n_pct = round((abs(delta_n) / expected_n) * 100, 1) if expected_n > 0 else (0.0 if current_n == 0 else 100.0)
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
			'<tr>'
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
			'</tr>'
		)

	head = (
		'<tr>'
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
		'</tr>'
	)

	content = ''
	if anomalous_count > 0 and table_rows:
		table_html = (
			"<table style='border-collapse:collapse;font-family:Arial,Helvetica,sans-serif;font-size:12px;color:#222;margin-top:12px'>"
			+ head
			+ ''.join(table_rows)
			+ '</table>'
		)
		intro = (
			"<p style='margin:12px 0 6px;color:#d9534f;font-weight:bold'>"
			f"{anomalous_count} contador(es) fora da media dos {lookback_days} dias anteriores "
			f"(abaixo: {below_count}, acima: {above_count}):"
			'</p>'
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
			'</p>'
		)

	return content


def _safe_float(value: Any) -> float:
	try:
		return float(value)
	except Exception:
		return 0.0


def _build_profile_rows_from_result(result: dict[str, Any]) -> list[tuple[str, float, float]]:
	phase_seconds = ((result.get('performance') or {}).get('phase_seconds') or {})
	rows: list[tuple[str, float, float]] = [('Pipeline started', 0.0, 0.0)]

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
					rows.append((
						f"... [mongodb] {mongo.get('collection', CONFIG['mongo_collection'])}: inserted {mongo.get('inserted', 0)}, skipped {mongo.get('duplicates', 0)}",
						round(cumulative, 2),
						round(cumulative, 2),
					))
				elif mongo.get('status') == 'skipped':
					rows.append(('... [mongodb] skipped', round(cumulative, 2), round(cumulative, 2)))
				else:
					rows.append((f"... [mongodb] error: {mongo.get('error', 'unknown')}", round(cumulative, 2), round(cumulative, 2)))

		if key == 'save_tidb':
			tidb = result.get('tidb') or {}
			if tidb.get('enabled', False):
				if tidb.get('status') == 'ok':
					rows.append((
						f"... [tidb] {tidb.get('table', CONFIG['tidb_table'])}: written {tidb.get('written', 0)}, rows in table {tidb.get('table_count', 0)}",
						round(cumulative, 2),
						round(cumulative, 2),
					))
				elif tidb.get('status') == 'skipped':
					rows.append(('... [tidb] skipped', round(cumulative, 2), round(cumulative, 2)))
				else:
					rows.append((f"... [tidb] error: {tidb.get('error', 'unknown')}", round(cumulative, 2), round(cumulative, 2)))

		if key == 'save_cratedb':
			cratedb = result.get('cratedb') or {}
			if cratedb.get('enabled', False):
				if cratedb.get('status') == 'ok':
					rows.append((
						f"... [cratedb] {cratedb.get('table', CONFIG['cratedb_table'])}: written {cratedb.get('written', 0)}, rows in table {cratedb.get('table_count', 0)}",
						round(cumulative, 2),
						round(cumulative, 2),
					))
				elif cratedb.get('status') == 'skipped':
					rows.append(('... [cratedb] skipped', round(cumulative, 2), round(cumulative, 2)))
				else:
					rows.append((f"... [cratedb] error: {cratedb.get('error', 'unknown')}", round(cumulative, 2), round(cumulative, 2)))

	anomalies = result.get('anomalies') or {}
	history_load_seconds = _safe_float(anomalies.get('history_load_seconds', 0.0))
	if history_load_seconds > 0:
		rows.append((f"... [anomaly] TiDB history read: {history_load_seconds:.2f}s", round(cumulative, 2), round(cumulative, 2)))

	overall = round(cumulative, 2)
	rows.append(('Overall pipeline', overall, overall))
	elapsed = round(_safe_float(result.get('elapsed_seconds', overall)), 2)
	rows.append(('Overall (before email):', elapsed, elapsed))
	return rows


def _build_profile_table_html(rows: list[tuple[str, float, float]]) -> str:
	table_rows: list[str] = []
	for task, watch_secs, proc_secs in rows:
		table_rows.append(
			'<tr>'
			f"<td style='border:1px solid #666;padding:4px 6px;text-align:left'>{ihtml.escape(task)}</td>"
			f"<td style='border:1px solid #666;padding:4px 6px;text-align:right'>{watch_secs:.2f}</td>"
			f"<td style='border:1px solid #666;padding:4px 6px;text-align:right'>{proc_secs:.2f}</td>"
			'</tr>'
		)

	head = (
		'<tr>'
		"<th style='border:1px solid #666;padding:4px 6px;text-align:left;background:#f2f2f2'>Task(s)</th>"
		"<th style='border:1px solid #666;padding:4px 6px;text-align:right;background:#f2f2f2'>watch time (secs)</th>"
		"<th style='border:1px solid #666;padding:4px 6px;text-align:right;background:#f2f2f2'>proc time (secs)</th>"
		'</tr>'
	)

	return (
		"<table style='border-collapse:collapse;font-family:Arial,Helvetica,sans-serif;font-size:13px;color:#222'>"
		+ head
		+ ''.join(table_rows)
		+ '</table>'
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
		print('email enviado via brevo')
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
		f'Environment: {runtime_label}',
		f"Run ID: {result.get('run_id', 'N/A')}",
		f'Status: {status}',
		'',
	]
	text_lines.extend(f"{task} | watch: {w:.2f} | proc: {p:.2f}" for task, w, p in profile_rows)

	if anom_count > 0:
		text_lines.append(
			f"\n[ANOMALIAS] {anom_count} contador(es) fora do esperado nos ultimos 2 dias "
			f"(abaixo: {anom_below_count}, acima: {anom_above_count})"
		)

	error_text = result.get('error')
	if error_text:
		text_lines.append(f'Error: {error_text}')

	text_body = '\n'.join(text_lines) + '\nEsta é uma mensagem automática.'

	html_body = (
		"<html><body style='font-family:Montserrat,Arial,sans-serif'>"
		f"<p><b>Environment:</b> {ihtml.escape(runtime_label)}</p>"
		f"<p><b>Run ID:</b> {ihtml.escape(str(result.get('run_id', 'N/A')))}</p>"
		f"<p><b>Status:</b> {ihtml.escape(str(status).upper())}</p>"
		+ profile_table_html
	)
	if anomaly_table_html:
		html_body += '<hr color=orange>' + anomaly_table_html
	if error_text:
		html_body += f"<hr color=orange><p style='color:#b00020'><b>Error:</b> {ihtml.escape(str(error_text))}</p>"
	html_body += '<hr color=orange>'
	html_body += 'This message is an automated notification from PIP Water script/Flask pipeline'
	html_body += '</body></html>'

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


__all__ = ['send_email_summary']
