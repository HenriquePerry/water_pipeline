#!/usr/bin/env python3
"""Create local test inputs with exactly two fully-missing meters (100% loss)."""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _meter_column_index(header: list) -> int:
    columns = [str(item).strip().lower() for item in header]
    candidates = ['detalhes', 'device', 'contador']
    for name in candidates:
        if name in columns:
            return columns.index(name)
    return 0


def generate_two_full_loss_file(
    output_file: str | None = None,
    baseline_file: str | None = None,
    meter_a: str = 'C15FA534254',
    meter_b: str = 'C16SB066962U',
) -> str:
    base_path = Path(__file__).resolve().parent.parent
    source = Path(baseline_file) if baseline_file else base_path / 'output_water' / 'water_data_raw.json'
    output = Path(output_file) if output_file else base_path / 'localINPUTS' / 'test_anomaly_2x100.json'

    if not source.exists():
        raise FileNotFoundError(f'Baseline JSON not found: {source}')

    payload = json.loads(source.read_text(encoding='utf-8'))
    records = payload if isinstance(payload, list) else [payload]

    anomaly_meters = {meter_a.strip(), meter_b.strip()}
    removed_rows = 0

    for record in records:
        if not isinstance(record, dict):
            continue
        header = record.get('header')
        body = record.get('body')
        if not isinstance(header, list) or not isinstance(body, list):
            continue

        meter_idx = _meter_column_index(header)
        filtered_body = []
        for row in body:
            if not isinstance(row, list) or meter_idx >= len(row):
                filtered_body.append(row)
                continue

            meter_id = str(row[meter_idx]).strip()
            if meter_id in anomaly_meters:
                removed_rows += 1
                continue
            filtered_body.append(row)

        record['body'] = filtered_body

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(records, indent=2, ensure_ascii=False), encoding='utf-8')

    print(f'Test input generated: {output}')
    print(f'Baseline used: {source}')
    print(f'Removed rows from anomalous meters: {removed_rows}')
    print(f'Anomalous meters (100% expected): {meter_a}, {meter_b}')
    return str(output)


def generate_local_history_and_current_pair(
    meter_a: str = 'C15FA534254',
    meter_b: str = 'C16SB066962U',
    history_output: str | None = None,
    current_output: str | None = None,
) -> tuple[str, str]:
    base_path = Path(__file__).resolve().parent.parent
    local_inputs = base_path / 'localINPUTS'
    history_path = Path(history_output) if history_output else local_inputs / 'history' / 'history_anomaly_reference.json'
    current_path = Path(current_output) if current_output else local_inputs / 'current' / 'test_anomaly_2x100.json'

    meters = [
        ('00PC503015', '1001', 100000),
        ('10045021', '1002', 200000),
        ('2124881J', '1003', 300000),
        ('94CWK04576', '1004', 400000),
        ('97WWK12134', '1005', 500000),
        ('98WWKI12645', '1006', 600000),
        ('C15SC004730', '1007', 700000),
        ('C16SC000825R', '1008', 800000),
        (meter_a, '9001', 900000),
        (meter_b, '9002', 950000),
    ]

    header = ['Detalhes', 'Nome', 'Data e hora', 'Valor (l)', 'Leitura (l)']
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    history_body: list[list[str]] = []
    for meter_id, alias, base in meters:
        for i in range(16):
            ts = now - timedelta(hours=32 - i)
            value = 40 + ((i * 7) % 65)
            reading = base + (i * value)
            history_body.append([
                meter_id,
                alias,
                ts.strftime('%d/%m/%Y %H:%M'),
                str(value),
                str(reading),
            ])

    anomaly_targets = {meter_a, meter_b}
    current_body = [row for row in history_body if str(row[0]).strip() not in anomaly_targets]

    history_record = {'header': header, 'body': history_body, 'footer': None}
    current_record = {'header': header, 'body': current_body, 'footer': None}

    history_path.parent.mkdir(parents=True, exist_ok=True)
    current_path.parent.mkdir(parents=True, exist_ok=True)
    history_path.write_text(json.dumps([history_record], indent=2, ensure_ascii=False), encoding='utf-8')
    current_path.write_text(json.dumps([current_record], indent=2, ensure_ascii=False), encoding='utf-8')

    print(f'History file created: {history_path}')
    print(f'Current file created: {current_path}')
    print(f'History rows: {len(history_body)} | Current rows: {len(current_body)}')
    print(f'Configured 100% missing meters: {meter_a}, {meter_b}')
    print('Use ANOMALY_HISTORY_LOCAL_JSON=<history file> and LOCAL_JSON_DIR=<folder with current file>.')
    return str(history_path), str(current_path)


if __name__ == '__main__':
    args = [arg for arg in sys.argv[1:] if arg]
    arg_a = args[0] if len(args) > 0 else 'C15FA534254'
    arg_b = args[1] if len(args) > 1 else 'C16SB066962U'

    # Default mode creates deterministic local test pair (history + current).
    if '--from-baseline' in args:
        clean_args = [arg for arg in args if arg != '--from-baseline']
        arg_a = clean_args[0] if len(clean_args) > 0 else 'C15FA534254'
        arg_b = clean_args[1] if len(clean_args) > 1 else 'C16SB066962U'
        arg_output = clean_args[2] if len(clean_args) > 2 else None
        arg_baseline = clean_args[3] if len(clean_args) > 3 else None
        generate_two_full_loss_file(arg_output, arg_baseline, arg_a, arg_b)
    else:
        generate_local_history_and_current_pair(arg_a, arg_b)
