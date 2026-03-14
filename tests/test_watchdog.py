"""Tests for storage and watchdog logic."""
import os
import sys
import time

# Local run: point to localhost + mapped ports and DB name (docker compose postgres:5434, redis:6379)
# setdefault so existing env is kept; load_dotenv() in storage won't override these
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5434")
os.environ.setdefault("POSTGRES_DB", "watchdog")
os.environ.setdefault("REDIS_HOST", "localhost")
os.environ.setdefault("REDIS_PORT", "6379")
os.environ.setdefault("REDIS_HEARTBEAT_PREFIX", "test_watchdog:heartbeat:")

sys_paths = [
    os.path.join(os.path.dirname(__file__), "..", "src"),
    os.path.join(os.path.dirname(__file__), "..", "src", "storage"),
]
for p in sys_paths:
    if p not in sys.path:
        sys.path.insert(0, p)

from storage.database import (
    init_db,
    upsert_device,
    mark_online,
    mark_offline,
    get_all_devices,
    get_device_status,
)
from storage.heartbeat_store import (
    record_heartbeat,
    get_last_heartbeat,
    get_all_heartbeats,
)


# ─────────────────────────────────────────
# DATABASE TESTS
# ─────────────────────────────────────────


def test_init_db():
    """Database should initialize without errors."""
    init_db()


def test_upsert_device():
    """Should insert new device and update existing."""
    init_db()
    upsert_device("test_device_1", "online", last_seen=None)
    row = get_device_status("test_device_1")
    assert row is not None
    assert row[0] == "online"


def test_mark_online():
    """Should set device online and update last_seen."""
    from datetime import datetime, timezone
    init_db()
    t = datetime(2025, 3, 13, 12, 0, 0, tzinfo=timezone.utc)
    mark_online("test_online_device", t)
    row = get_device_status("test_online_device")
    assert row is not None
    assert row[0] == "online"
    assert row[1] is not None


def test_mark_offline():
    """Should set device offline and increment total_offline_count."""
    from datetime import datetime, timezone
    init_db()
    upsert_device("test_offline_device", "online")
    mark_offline("test_offline_device", last_seen=datetime.now(timezone.utc))
    row = get_device_status("test_offline_device")
    assert row is not None
    assert row[0] == "offline"
    assert row[2] >= 1
    mark_offline("test_offline_device")  # already offline: should increment again
    row2 = get_device_status("test_offline_device")
    assert row2[2] >= 2


def test_get_all_devices():
    """Should return all devices ordered by device_id."""
    init_db()
    upsert_device("device_a", "online")
    upsert_device("device_b", "offline")
    rows = get_all_devices()
    assert isinstance(rows, list)
    ids = [r[0] for r in rows]
    assert "device_a" in ids
    assert "device_b" in ids


# ─────────────────────────────────────────
# REDIS HEARTBEAT TESTS
# ─────────────────────────────────────────


def test_record_and_get_heartbeat():
    """Should store and retrieve last heartbeat timestamp."""
    record_heartbeat("test_device_hb", 12345.67)
    ts = get_last_heartbeat("test_device_hb")
    assert ts == 12345.67


def test_get_last_heartbeat_missing():
    """Missing device should return None."""
    ts = get_last_heartbeat("nonexistent_device_xyz_123")
    assert ts is None


def test_get_all_heartbeats():
    """Should return dict of device_id -> timestamp."""
    t = time.time()
    record_heartbeat("all_hb_1", t)
    record_heartbeat("all_hb_2", t + 1)
    out = get_all_heartbeats()
    assert "all_hb_1" in out
    assert "all_hb_2" in out
    assert out["all_hb_1"] == t
    assert out["all_hb_2"] == t + 1
