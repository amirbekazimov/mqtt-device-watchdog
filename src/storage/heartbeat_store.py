"""Redis-backed fast heartbeat tracking: last heartbeat timestamp per device."""
import os
import time
from typing import Optional

import redis
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

HEARTBEAT_KEY_PREFIX = os.getenv("REDIS_HEARTBEAT_PREFIX", "watchdog:heartbeat:")


def get_redis():
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=0,
        decode_responses=True,
    )


def record_heartbeat(device_id: str, timestamp: float = None) -> None:
    """Store last heartbeat timestamp for device (Unix float)."""
    if timestamp is None:
        timestamp = time.time()
    r = get_redis()
    key = f"{HEARTBEAT_KEY_PREFIX}{device_id}"
    r.set(key, str(timestamp))


def get_last_heartbeat(device_id: str) -> Optional[float]:
    """Return last heartbeat timestamp (Unix float) or None."""
    r = get_redis()
    key = f"{HEARTBEAT_KEY_PREFIX}{device_id}"
    raw = r.get(key)
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def get_all_heartbeat_keys():
    """Return list of device_ids that have a heartbeat key (scan prefix)."""
    r = get_redis()
    prefix = f"{HEARTBEAT_KEY_PREFIX}*"
    keys = []
    for key in r.scan_iter(match=prefix):
        device_id = key.removeprefix(HEARTBEAT_KEY_PREFIX)
        keys.append(device_id)
    return keys


def get_all_heartbeats():
    """Return dict device_id -> last_heartbeat_timestamp (float)."""
    r = get_redis()
    prefix = f"{HEARTBEAT_KEY_PREFIX}*"
    out = {}
    for key in r.scan_iter(match=prefix):
        device_id = key.removeprefix(HEARTBEAT_KEY_PREFIX)
        raw = r.get(key)
        if raw is not None:
            try:
                out[device_id] = float(raw)
            except (TypeError, ValueError):
                pass
    return out
