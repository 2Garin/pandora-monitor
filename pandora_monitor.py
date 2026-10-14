import asyncio
import logging
import math
import os
import signal
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
from pandora_cas.account import PandoraOnlineAccount

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('pandora-monitor')


def required(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f'Missing environment variable: {name}')
    return value


async def push(session, base, token, status, message):
    async with session.get(f'{base}/api/push/{token}', params={'status': status, 'msg': message}) as response:
        response.raise_for_status()


DB_PATH = Path(os.getenv('SQLITE_PATH', '/data/pandora.sqlite3'))


def save_measurement(device_id: int, last_seen: int, voltage: float) -> bool:
    # One measurement per device and last contact timestamp.
    if not math.isfinite(voltage) or voltage < 0:
        raise ValueError('Invalid battery voltage')
    if last_seen <= 0:
        raise ValueError('Invalid last contact timestamp')
    if not DB_PATH.parent.is_dir():
        raise RuntimeError(f'SQLite directory missing: {DB_PATH.parent} (create it before starting Compose)')
    with sqlite3.connect(DB_PATH, timeout=15) as db:
        db.execute('PRAGMA busy_timeout=15000')
        db.execute('''
            CREATE TABLE IF NOT EXISTS battery_history (
                device_id INTEGER NOT NULL,
                session_timestamp INTEGER NOT NULL,
                voltage REAL NOT NULL,
                recorded_at INTEGER NOT NULL,
                PRIMARY KEY (device_id, session_timestamp)
            )
        ''')
        cursor = db.execute('''
            INSERT OR IGNORE INTO battery_history
                (device_id, session_timestamp, voltage, recorded_at)
            VALUES (?, ?, ?, ?)
        ''', (device_id, last_seen, voltage, int(datetime.now(timezone.utc).timestamp())))
        return cursor.rowcount == 1


async def run_once(session):
    base = required('KUMA_URL').rstrip('/')
    api_token = required('KUMA_PUSH_API')
    stage = 'reading Pandora state'
    try:
        account = PandoraOnlineAccount(session=session, username=required('PANDORA_LOGIN'), password=required('PANDORA_PASSWORD'))
        await account.async_authenticate()
        await account.async_refresh_devices()
        await account.async_request_updates()
        device_id = int(required('PANDORA_DEVICE_ID'))
        device = account.devices.get(device_id)
        if device is None or device.state is None:
            raise RuntimeError('Device or state unavailable')
        state = device.state
        now = datetime.now(timezone.utc).timestamp()
        last_seen = state.online_timestamp_utc
        if last_seen is None or not math.isfinite(last_seen) or last_seen <= 0 or last_seen > now + 300:
            raise RuntimeError('Invalid last-online timestamp')
        age_hours = (now - last_seen) / 3600
        max_age = float(os.getenv('MAX_OFFLINE_HOURS', '48'))
        if not math.isfinite(max_age) or max_age <= 0:
            raise ValueError('MAX_OFFLINE_HOURS must be a positive number')
        connection_ok = age_hours < max_age
        voltage = state.voltage
        # Persist before Push: a Kuma outage should not lose a new sample.
        if voltage is not None:
            voltage = float(voltage)
            stage = 'saving battery history'
            inserted = save_measurement(device_id, int(last_seen), voltage)
            if inserted:
                log.info('SQLite: new measurement saved: %.2f V at %s', voltage,
                         datetime.fromtimestamp(last_seen, timezone.utc).isoformat())
        stage = 'pushing connection status to Kuma'
        await push(session, base, required('KUMA_PUSH_CONNECTION'), 'up' if connection_ok else 'down', f'Last contact {age_hours:.1f}h ago')
        if not connection_ok:
            battery_status, battery_message = 'down', f'Stale battery reading: last contact {age_hours:.1f}h ago'
        elif voltage is None:
            battery_status, battery_message = 'down', 'Battery voltage unavailable'
        else:
            threshold = float(os.getenv('MIN_BATTERY_VOLTAGE', '4.0'))
            if not math.isfinite(threshold) or threshold < 0:
                raise ValueError('MIN_BATTERY_VOLTAGE must be a non-negative number')
            battery_status = 'up' if voltage >= threshold else 'down'
            battery_message = f'Battery {voltage:.2f}V (threshold {threshold:.2f}V); last contact {age_hours:.1f}h ago'
        stage = 'pushing battery status to Kuma'
        await push(session, base, required('KUMA_PUSH_BATTERY'), battery_status, battery_message)
        stage = 'pushing API status to Kuma'
        await push(session, base, api_token, 'up', 'Pandora API and Kuma push OK')
        log.info('Voltage=%s V; last contact %.1fh ago; connection=%s; battery=%s', voltage, age_hours, 'up' if connection_ok else 'down', battery_status)
    except Exception as exc:
        # HTTP exceptions can contain the push token in their URL; do not log them verbatim.
        detail = (str(exc) if stage == 'saving battery history' and isinstance(exc, (sqlite3.Error, RuntimeError))
                  else type(exc).__name__)
        log.error('Monitoring cycle failed while %s: %s', stage, detail)
        try:
            await push(session, base, api_token, 'down', f'Monitor error: {type(exc).__name__}')
        except Exception:
            log.error('Unable to report API monitor failure to Kuma')


async def main():
    interval = int(os.getenv('POLL_INTERVAL_SECONDS', '1800'))
    if interval < 300:
        raise ValueError('POLL_INTERVAL_SECONDS must be >= 300')
    for name in ('PANDORA_LOGIN', 'PANDORA_PASSWORD', 'PANDORA_DEVICE_ID', 'KUMA_URL', 'KUMA_PUSH_CONNECTION', 'KUMA_PUSH_BATTERY', 'KUMA_PUSH_API'):
        required(name)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while not stop.is_set():
            await run_once(session)
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass


if __name__ == '__main__':
    asyncio.run(main())
