"""Simulates IoT devices: send heartbeats every 30s; some randomly freeze to demonstrate LWT failure."""
import json
import os
import random
import signal
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import paho.mqtt.client as mqtt

_client = None


def _env_int(key: str, default: int) -> int:
    return int(os.getenv(key, str(default)))


def _env_float(key: str, default: float) -> float:
    return float(os.getenv(key, str(default)))


def _request_stop(signum=None, frame=None):
    global _client
    if _client:
        _client.disconnect()
    sys.exit(0)


def main():
    global _client
    device_count = _env_int("SIMULATOR_DEVICE_COUNT", 10)
    heartbeat_interval = _env_int("HEARTBEAT_INTERVAL", 30)
    freeze_probability = _env_float("SIMULATOR_FREEZE_PROBABILITY", 0.02)

    device_ids = [f"device_{i:03d}" for i in range(1, device_count + 1)]
    frozen = set()
    freeze_times = {}
    REBOOT_AFTER_SECONDS = 180

    mqtt_host = os.getenv("MQTT_HOST", "localhost")
    mqtt_port = _env_int("MQTT_PORT", 1883)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="device_simulator")
    _client = client
    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)
    client.connect(mqtt_host, mqtt_port, keepalive=60)
    client.loop_start()

    print(f"Simulator: {device_count} devices, heartbeat every {heartbeat_interval}s, freeze prob={freeze_probability}")
    print("Devices:", ", ".join(device_ids))

    try:
        while True:
            # Auto-reboot: unfreeze devices that have been frozen for 180+ seconds
            now = time.time()
            for device_id in list(frozen):
                if now - freeze_times.get(device_id, now) >= REBOOT_AFTER_SECONDS:
                    frozen.discard(device_id)
                    freeze_times.pop(device_id, None)
                    print(f"[REBOOT] {device_id} restarted and sending heartbeats again!")

            # Randomly freeze one device (if any still running)
            running = [d for d in device_ids if d not in frozen]
            if running and random.random() < freeze_probability:
                victim = random.choice(running)
                frozen.add(victim)
                freeze_times[victim] = time.time()
                print(f"[FREEZE] {victim} stopped sending heartbeats (simulated crash/WiFi drop)")

            ts = time.time()
            payload = json.dumps({"ts": ts})
            for device_id in device_ids:
                if device_id in frozen:
                    continue
                topic = f"devices/{device_id}/heartbeat"
                client.publish(topic, payload, qos=1)

            time.sleep(heartbeat_interval)
    except KeyboardInterrupt:
        pass
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
