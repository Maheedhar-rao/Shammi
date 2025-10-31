# Statements/decisions/db.py
from __future__ import annotations
import sqlite3, os, json, time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DECISIONS_DB_PATH = BASE_DIR / "decisions.db"

def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DECISIONS_DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn

def _init():
    conn = _conn()
    cur = conn.cursor()

    # Latest snapshot per (tenant_id, deal_id, lender)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS decision_state (
      tenant_id   TEXT NOT NULL,
      deal_id     INTEGER NOT NULL,
      lender      TEXT NOT NULL,
      status      TEXT NOT NULL,         -- APPROVED | DECLINED | STIPS_REQUIRED | PENDING
      reason      TEXT,
      offer_json  TEXT,                   -- json string
      stips_json  TEXT,                   -- json string
      last_email_msg_id TEXT,
      updated_at  INTEGER NOT NULL,       -- epoch seconds
      PRIMARY KEY (tenant_id, deal_id, lender)
    );
    """)

    # Raw event log (optional, handy for drilldown)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS decision_events (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      tenant_id  TEXT NOT NULL,
      deal_id    INTEGER NOT NULL,
      lender     TEXT NOT NULL,
      event_type TEXT NOT NULL,       -- APPROVED | DECLINED | STIPS_REQUIRED | NOTE ...
      payload    TEXT,                -- json string
      occurred_at INTEGER NOT NULL    -- epoch seconds
    );
    """)

    # Lightweight tickets (optional/future)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tickets (
      ticket_id  INTEGER PRIMARY KEY AUTOINCREMENT,
      tenant_id  TEXT NOT NULL,
      deal_id    INTEGER NOT NULL,
      lender     TEXT,
      kind       TEXT NOT NULL,       -- STIPS_OVERDUE | APPROVAL_REVIEW | DECLINE_REVIEW ...
      status     TEXT NOT NULL DEFAULT 'OPEN',
      summary    TEXT,
      details    TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
    """)

    conn.commit()
    conn.close()

_init()

def upsert_state(tenant_id: str, deal_id: int, lender: str, status: str,
                 reason: str | None = None,
                 offer: dict | None = None,
                 stips: dict | None = None,
                 last_email_msg_id: str | None = None) -> None:
    now = int(time.time())
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO decision_state (tenant_id, deal_id, lender, status, reason, offer_json, stips_json, last_email_msg_id, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(tenant_id, deal_id, lender) DO UPDATE SET
      status=excluded.status,
      reason=excluded.reason,
      offer_json=excluded.offer_json,
      stips_json=excluded.stips_json,
      last_email_msg_id=excluded.last_email_msg_id,
      updated_at=excluded.updated_at
    """, (
        tenant_id, deal_id, lender, status, reason or "",
        json.dumps(offer or {}), json.dumps(stips or {}),
        last_email_msg_id or "", now
    ))
    conn.commit()
    conn.close()

def insert_event(tenant_id: str, deal_id: int, lender: str, event_type: str, payload: dict | None):
    now = int(time.time())
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO decision_events (tenant_id, deal_id, lender, event_type, payload, occurred_at)
    VALUES (?, ?, ?, ?, ?, ?)
    """, (tenant_id, deal_id, lender, event_type, json.dumps(payload or {}), now))
    conn.commit()
    conn.close()

def get_states_for_deal(tenant_id: str, deal_id: int) -> list[dict]:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
    SELECT lender, status, reason, offer_json, stips_json, last_email_msg_id, updated_at
    FROM decision_state
    WHERE tenant_id = ? AND deal_id = ?
    ORDER BY lender ASC
    """, (tenant_id, deal_id))
    rows = cur.fetchall()
    conn.close()
    out = []
    import json as _json
    for r in rows:
        out.append({
            "lender": r["lender"],
            "status": r["status"],
            "reason": r["reason"],
            "offer": (_json.loads(r["offer_json"]) if r["offer_json"] else {}),
            "stips": (_json.loads(r["stips_json"]) if r["stips_json"] else {}),
            "last_email_msg_id": r["last_email_msg_id"],
            "updated_at": r["updated_at"],
        })
    return out

def get_summary_for_deals(tenant_id: str, deal_ids: list[int]) -> dict:
    if not deal_ids:
        return {}
    ph = ",".join("?" for _ in deal_ids)
    conn = _conn()
    cur = conn.cursor()
    cur.execute(f"""
      SELECT deal_id,
             SUM(CASE WHEN status='APPROVED' THEN 1 ELSE 0 END) AS approved,
             SUM(CASE WHEN status='DECLINED' THEN 1 ELSE 0 END) AS declined,
             SUM(CASE WHEN status='STIPS_REQUIRED' THEN 1 ELSE 0 END) AS stips
      FROM decision_state
      WHERE tenant_id = ? AND deal_id IN ({ph})
      GROUP BY deal_id
    """, [tenant_id, *deal_ids])
    rows = cur.fetchall()
    conn.close()
    out = {int(r["deal_id"]): {"approved": r["approved"], "declined": r["declined"], "stips": r["stips"]} for r in rows}
    return out
