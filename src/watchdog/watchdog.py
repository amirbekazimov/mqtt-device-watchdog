"""Heartbeat-based watchdog: monitors devices, detects dead ones, Rich dashboard."""
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import paho.mqtt.client as mqtt
from rich.console import Console
from rich.live import Live
from rich.table import Table

from storage.database import (
    init_db,
    get_all_devices,
    get_device_status,
    mark_online,
    mark_offline,
)
from storage.heartbeat_store import record_heartbeat, get_all_heartbeats
from alerts.alert_manager import alert_device_offline

console = Console()
_client = None
_live = None


def _request_stop(signum=None, frame=None):
    global _client, _live
    if _live:
        _live.stop()
    if _client:
        _client.disconnect()
    sys.exit(0)


def _env_int(key: str, default: int) -> int:
    return int(os.getenv(key, str(default)))


def _build_status_table():
    table = Table(title="Device Watchdog — Real-time Status", show_header=True, header_style="bold cyan")
    table.add_column("Device ID", style="dim")
    table.add_column("Status", style="bold")
    table.add_column("Last Seen", style="dim")
    table.add_column("Offline Count", justify="right")
    rows = get_all_devices()
    for device_id, status, last_seen, total_offline_count in rows:
        status_style = "green" if status == "online" else "red"
        last_str = last_seen.strftime("%Y-%m-%d %H:%M:%S") if last_seen else "—"
        table.add_row(device_id, f"[{status_style}]{status}[/{status_style}]", last_str, str(total_offline_count))
    if not rows:
        table.add_row("—", "—", "—", "—")
    return table


def on_connect(client, userdata, flags, reason_code, properties):
    client.subscribe("devices/+/heartbeat", qos=1)


def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        if not topic.startswith("devices/") or not topic.endswith("/heartbeat"):
            return
        parts = topic.split("/")
        if len(parts) != 3:
            return
        device_id = parts[1]
        payload = json.loads(msg.payload.decode()) if msg.payload else {}
        ts = payload.get("ts") or time.time()
        record_heartbeat(device_id, float(ts))
        last_seen = datetime.fromtimestamp(ts, tz=timezone.utc)
        mark_online(device_id, last_seen)
    except Exception as e:
        console.print(f"[red]Watchdog heartbeat error: {e}[/red]")


def run_check():
    """Check all heartbeats; mark stale devices offline and alert."""
    timeout = _env_int("HEARTBEAT_TIMEOUT", 90)
    now = time.time()
    heartbeats = get_all_heartbeats()
    for device_id, last_ts in heartbeats.items():
        if now - last_ts >= timeout:
            row = get_device_status(device_id)
            was_online = row is None or row[0] == "online"
            if was_online:
                last_seen = datetime.fromtimestamp(last_ts, tz=timezone.utc) if last_ts else None
                mark_offline(device_id, last_seen=last_seen)
                row_after = get_device_status(device_id)
                total = row_after[2] if row_after else 1
                alert_device_offline(device_id, now - last_ts, total)


def main():
    global _client, _live
    init_db()

    mqtt_host = os.getenv("MQTT_HOST", "localhost")
    mqtt_port = _env_int("MQTT_PORT", 1883)
    check_interval = _env_int("WATCHDOG_CHECK_INTERVAL", 10)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="watchdog")
    _client = client
    client.on_connect = on_connect
    client.on_message = on_message

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    client.connect(mqtt_host, mqtt_port, keepalive=60)
    client.loop_start()

    try:
        with Live(_build_status_table(), console=console, refresh_per_second=2) as live:
            _live = live
            while True:
                time.sleep(check_interval)
                run_check()
                live.update(_build_status_table())
    except KeyboardInterrupt:
        pass
    finally:
        _live = None
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
