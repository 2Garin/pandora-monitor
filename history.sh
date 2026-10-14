#!/bin/sh

docker exec pandora-monitor python -c '
import sqlite3

db = sqlite3.connect(
    "file:/data/pandora.sqlite3?mode=ro",
    uri=True
)

print("{:<22} {:<12} {}".format("Дата (МСК)", "ID устройства", "Напряжение"))
print("-" * 50)

for timestamp, device_id, voltage in db.execute("""
    SELECT
        datetime(session_timestamp, "unixepoch", "+3 hours"),
        device_id,
        voltage
    FROM battery_history
    ORDER BY session_timestamp DESC
    LIMIT 30
"""):
    print(f"{timestamp:22} {device_id:<12} {voltage:.2f} В")

db.close()
'
