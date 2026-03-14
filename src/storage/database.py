"""PostgreSQL persistence for device status and offline history."""
import os

import psycopg2
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

STATUS_ONLINE = "online"
STATUS_OFFLINE = "offline"


def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "watchdog"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "root123"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
    )


def init_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS devices (
            device_id VARCHAR(100) PRIMARY KEY,
            status VARCHAR(20) NOT NULL DEFAULT 'offline',
            last_seen TIMESTAMP,
            total_offline_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS device_history (
            id SERIAL PRIMARY KEY,
            device_id VARCHAR(100) NOT NULL,
            status VARCHAR(20) NOT NULL,
            changed_at TIMESTAMP DEFAULT NOW(),
            FOREIGN KEY (device_id) REFERENCES devices(device_id)
        )
    """)

    conn.commit()
    cur.close()
    conn.close()


def upsert_device(device_id: str, status: str, last_seen=None):
    """Insert or update device. last_seen can be a datetime or None."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO devices (device_id, status, last_seen, updated_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (device_id)
        DO UPDATE SET
            status = EXCLUDED.status,
            last_seen = EXCLUDED.last_seen,
            updated_at = NOW()
    """, (device_id, status, last_seen))
    conn.commit()
    cur.close()
    conn.close()


def mark_offline(device_id: str, last_seen=None):
    """Mark device offline and increment total_offline_count."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO devices (device_id, status, last_seen, total_offline_count, updated_at)
        VALUES (%s, %s, %s, 1, NOW())
        ON CONFLICT (device_id)
        DO UPDATE SET
            status = 'offline',
            last_seen = COALESCE(EXCLUDED.last_seen, devices.last_seen),
            total_offline_count = devices.total_offline_count + 1,
            updated_at = NOW()
    """, (device_id, STATUS_OFFLINE, last_seen))
    cur.execute("""
        INSERT INTO device_history (device_id, status) VALUES (%s, 'offline')
    """, (device_id,))
    conn.commit()
    cur.close()
    conn.close()


def mark_online(device_id: str, last_seen):
    """Mark device online and update last_seen."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO devices (device_id, status, last_seen, updated_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (device_id)
        DO UPDATE SET
            status = 'online',
            last_seen = EXCLUDED.last_seen,
            updated_at = NOW()
    """, (device_id, STATUS_ONLINE, last_seen))
    cur.execute("""
        INSERT INTO device_history (device_id, status) VALUES (%s, 'online')
    """, (device_id,))
    conn.commit()
    cur.close()
    conn.close()


def get_all_devices():
    """Return list of (device_id, status, last_seen, total_offline_count)."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT device_id, status, last_seen, total_offline_count
        FROM devices
        ORDER BY device_id
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def get_device_status(device_id: str):
    """Return (status, last_seen, total_offline_count) or None."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT status, last_seen, total_offline_count
        FROM devices
        WHERE device_id = %s
    """, (device_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row
