#!/usr/bin/env python3
"""
Generate a test JSON file with one meter showing anomalous readings (fewer than expected).
This simulates a meter failure or communication issue.
"""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

def generate_test_anomaly_json(output_file: str = None, anomaly_meter: str = "D13XF126371P"):
    """
    Generate a test JSON with normal data plus one meter with very few readings.
    
    Args:
        output_file: Where to save the JSON (default: output_water/test_anomaly_sensors.json)
        anomaly_meter: Which meter to simulate failure for
    """
    if output_file is None:
        output_file = Path(__file__).parent.parent / "output_water" / "test_anomaly_sensors.json"
    else:
        output_file = Path(output_file)
    
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Standard header
    header = ["Device", "Alias", "Date/Time", "Value (l)", "Reading (l)"]
    
    # Generate 10 normal meters with ~20 readings each (normal behavior)
    normal_meters = {
        "D13XF126371P": ("2081", 19642880),
        "D14XF111533Z": ("148391", 2500000),
        "D15XF224455A": ("2089", 1800000),
        "D16XF333666B": ("2090", 3200000),
        "D17XF444777C": ("2091", 2100000),
        "D18XF555888D": ("2092", 1500000),
        "D19XF666999E": ("2093", 2800000),
        "D20XF777000F": ("2094", 1900000),
        "D21XF888111G": ("2095", 2400000),
        "D22XF999222H": ("2096", 1700000),
    }
    
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    body = []
    reading_id = 0
    
    # Generate readings for normal meters
    for meter_id, (alias, base_reading) in normal_meters.items():
        # 20 readings per meter over ~30 hours (normal)
        for i in range(20):
            timestamp = now - timedelta(hours=30-i)
            value_l = 50 * (i + 1)  # incrementing values
            reading = base_reading + value_l
            body.append([
                meter_id,
                alias,
                timestamp.strftime("%d/%m/%Y %H:%M"),
                f"{value_l:,}",
                f"{reading:,}"
            ])
            reading_id += 1
    
    # Add the ANOMALOUS meter with very few readings (only 3-4 readings instead of ~20)
    if anomaly_meter in normal_meters:
        alias, base_reading = normal_meters[anomaly_meter]
        print(f"⚠️  Marking meter {anomaly_meter} (alias {alias}) as anomalous (only 3 readings)")
        
        # Remove the normal readings for this meter
        body = [row for row in body if row[0] != anomaly_meter]
        
        # Add only 3-4 readings for this meter (will fail threshold)
        for i in range(3):
            timestamp = now - timedelta(hours=25-i)
            value_l = 50 * (i + 1)
            reading = base_reading + value_l
            body.append([
                anomaly_meter,
                alias,
                timestamp.strftime("%d/%m/%Y %H:%M"),
                f"{value_l:,}",
                f"{reading:,}"
            ])
    
    data = {
        "header": header,
        "body": body,
        "footer": None
    }
    
    # Write to file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Test JSON created: {output_file}")
    print(f"   - Total readings: {len(body)}")
    print(f"   - Normal meters: {len(normal_meters)-1} with ~20 readings each")
    print(f"   - Anomalous meter: {anomaly_meter} with only 3-4 readings")
    print(f"\n📝 To use this file:")
    print(f"   1. Set JSON_FILE_URLS to point to this file, or")
    print(f"   2. Place it in LOCAL_JSON_DIR, or")
    print(f"   3. Manually add it to REPO_JSON_FILES list")
    print(f"\n🚀 Then run the pipeline cell to see the anomaly in the email!")
    
    return str(output_file)


if __name__ == "__main__":
    anomaly_meter = sys.argv[1] if len(sys.argv) > 1 else "D13XF126371P"
    output = sys.argv[2] if len(sys.argv) > 2 else None
    generate_test_anomaly_json(output, anomaly_meter)
