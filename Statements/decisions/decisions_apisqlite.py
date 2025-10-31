# decisions/decisions_api.py
from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Optional

from flask import Blueprint, jsonify, request, session

# --- Reuse underwrite's DB + helpers (so we see the same deals/deliveries) ---
from underwrite import (
    _db as uw_db,
    _init_db as uw_init_db,
    DEALS_DB_PATH as UW_DB_PATH,
)

bp = Blueprint("decisions", __name__)
# Optional: allow a separate decisions DB. By default we store decisions
# in the SAME deals.db to keep things simple and avoid table-not-found.
DEFAULT_DECISIONS_DB = str(UW_DB_PATH)
DECISIONS_DB_PATH = Path(os.environ.get("DECISIONS_DB_PATH", DEFAULT_DECISIONS_DB))



# --- Path & imports ----------------------------------------------------------
PKG_ROOT = Path(__file__).resolve().parents[1]  # -> Statements/
if str(PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(PKG_ROOT))

# Optional: Gmail watcher (worker). If missing, /ingest-now returns 501.
try:
    from worker.gmail_worker import GmailWatcher  # type: ignore
    _GMAIL_IMPORT_ERROR = None
except Exception as _imp_err:
    GmailWatcher = None  # type: ignore
    _GMAIL_IMPORT_ERROR = _imp_err


# -------------------------- DB helpers & init --------------------------

def _decisions_db() -> sqlite3.Connection:
    """Open the decisions DB. If same as deals DB, reuse underwrite's _db()."""
    if str(DECISIONS_DB_PATH) == str(UW_DB_PATH):
        return uw_db()
    conn = sqlite3.connect(str(DECISIONS_DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_decisions_schema() -> None:
    """Create decisions table if missing; add optional columns if needed."""
    con = _decisions_db()
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS decisions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          deal_id INTEGER NOT NULL,
          lender TEXT,
          status TEXT,
          reason TEXT,
          offer_json TEXT,
          stips_json TEXT,
          last_email_msg_id TEXT,
          user_id TEXT,            -- multi-tenant isolation
          provider TEXT,           -- optional metadata
          message_id TEXT,         -- optional metadata
          updated_at INTEGER,
          created_at INTEGER
        )
        """
    )
    # Backfill optional columns when running against an older table
    cur.execute("PRAGMA table_info(decisions)")
    have_cols = {r[1] for r in cur.fetchall()}
    def _add(col: str, ddl: str):
        if col not in have_cols:
            try:
                cur.execute(f"ALTER TABLE decisions ADD COLUMN {ddl}")
            except Exception:
                pass

    _add("provider", "provider TEXT")
    _add("message_id", "message_id TEXT")
    _add("created_at", "created_at INTEGER")
    con.commit()
    con.close()


# Initialize both schemas at import time (idempotent/no-op if they exist)
uw_init_db()               # deals + deliveries
_ensure_decisions_schema() # decisions


# -------------------------- Local helpers --------------------------

def _now() -> int:
    return int(time.time())


def _deal_owner(deal_id: int) -> Optional[str]:
    """Return the owner (user_id or sender_email) of a deal from the deals DB."""
    con = uw_db()
    try:
        row = con.execute(
            "SELECT user_id, sender_email FROM deals WHERE id=?",
            (deal_id,),
        ).fetchone()
        if row:
            return (row["user_id"] or row["sender_email"] or "").strip() or None
        return None
    finally:
        try:
            con.close()
        except Exception:
            pass


def _json_or_empty(v) -> str:
    try:
        return json.dumps(v if v is not None else {})
    except Exception:
        return "{}"


# -------------------------- API: create manual event --------------------------

@bp.post("/api/decisions/event")
def decisions_event_create():
    """
    Ingest a decision event (manual, webhooks, or downstream workers).

    Payload:
    {
      "deal_id": 11,
      "lender": "Spartan",
      "event_type": "APPROVED" | "DECLINED" | "STIPS_REQUIRED",   # alias of status
      "status": "APPROVED",
      "reason": "Approved at 1.35",
      "offer": {...},
      "stips": {...},
      "last_email_msg_id": "abc",
      "provider": "gmail",                  # optional
      "message_id": "abcdef123"             # optional
    }
    """
    data = request.get_json(force=True) or {}
    try:
        deal_id = int(data.get("deal_id") or 0)
    except Exception:
        deal_id = 0
    if deal_id <= 0:
        return jsonify({"error": "deal_id required"}), 400

    # bind user: session -> header -> actual deal owner (so curl inserts still show)
    uid = (session.get("google_email") or request.headers.get("X-User-Email") or "").strip()
    if not uid:
        owner = _deal_owner(deal_id)
        if owner:
            uid = owner

    status = (data.get("status") or data.get("event_type") or "").strip().upper()
    payload = {
        "deal_id": deal_id,
        "lender": (data.get("lender") or "").strip(),
        "status": status,
        "reason": data.get("reason") or "",
        "offer_json": _json_or_empty(data.get("offer")),
        "stips_json": _json_or_empty(data.get("stips")),
        "last_email_msg_id": (data.get("last_email_msg_id") or "").strip(),
        "user_id": uid or None,
        "provider": (data.get("provider") or "").strip() or None,
        "message_id": (data.get("message_id") or "").strip() or None,
        "updated_at": _now(),
        "created_at": _now(),
    }

    con = _decisions_db()
    try:
        con.execute(
            """INSERT INTO decisions
               (deal_id,lender,status,reason,offer_json,stips_json,last_email_msg_id,user_id,provider,message_id,updated_at,created_at)
               VALUES (:deal_id,:lender,:status,:reason,:offer_json,:stips_json,:last_email_msg_id,:user_id,:provider,:message_id,:updated_at,:created_at)
            """,
            payload,
        )
        con.commit()
    finally:
        try:
            con.close()
        except Exception:
            pass

    return jsonify({"ok": True})


# -------------------------- API: fetch decisions for a deal --------------------------

@bp.get("/api/decisions/deal/<int:deal_id>")
def decisions_by_deal(deal_id: int):
    """
    Return decision states for a given deal, filtered by user ownership.
    """
    caller = (session.get("google_email") or request.headers.get("X-User-Email") or "").strip()
    owner = _deal_owner(deal_id)

    # Optional isolation guard: if both known and mismatch, hide
    if owner and caller and (owner != caller):
        return jsonify({"states": []})

    con = _decisions_db()
    try:
        rows = con.execute(
            """
            SELECT lender,status,reason,offer_json,stips_json,last_email_msg_id,updated_at
              FROM decisions
             WHERE deal_id=?
               AND (user_id IS NULL OR user_id = ?)
             ORDER BY updated_at DESC, id DESC
            """,
            (deal_id, caller or owner or ""),
        ).fetchall()
    finally:
        try:
            con.close()
        except Exception:
            pass

    def _parse(js: str):
        try:
            return json.loads(js or "{}")
        except Exception:
            return {}

    states = [
        {
            "lender": r["lender"],
            "status": r["status"],
            "reason": r["reason"],
            "offer": _parse(r["offer_json"]),
            "stips": _parse(r["stips_json"]),
            "updated_at": r["updated_at"],
        }
        for r in rows
    ]
    return jsonify({"states": states})


# -------------------------- API: one-off Gmail ingest for a deal --------------------------

@bp.post("/api/decisions/ingest-now/<int:deal_id>")
def decisions_ingest_now(deal_id: int):
    """
    Trigger a one-off Gmail ingest for this deal:
      - looks up the deal sender_email (from deals DB),
      - uses its Gmail token (your watcher should know how to load it),
      - processes any replies and writes into decisions table,
      - returns the refreshed states.
    """
    if GmailWatcher is None:
        return jsonify({"ok": False, "error": "gmail_watcher_unavailable", "detail": str(GMAIL_WATCHER_ERR)}), 501

    # 1) Verify you’re logged in (or return 401)
    if not (session.get("user_email") or session.get("google_email")):
        return jsonify({"ok": False, "error": "not_authenticated"}), 401

    # 2) Pull sender_email for the deal from the SAME DB the watcher will use
    from underwrite import _db as uw_db, DEALS_DB_PATH as UW_DB_PATH
    con = uw_db()
    row = con.execute("SELECT id, sender_email FROM deals WHERE id=?", (deal_id,)).fetchone()
    con.close()
    if not row:
        return jsonify({"ok": False, "error": "deal_not_found"}), 404

    sender = (row["sender_email"] or "").strip()

    # 3) Run the ingest
    try:
        watcher = GmailWatcher(email=sender, db_path=str(UW_DB_PATH))
        result  = watcher.ingest_deal(deal_id)
    except Exception as e:
        # Common issues: no tokens/<sender>.json, expired token, wrong scopes
        return jsonify({"ok": False, "error": str(e)}), 409

    # 4) Return current decisions for this deal from that same DB
    con = uw_db()
    rows = con.execute("""
        SELECT lender, status, reason, offer_json, stips_json, provider, message_id, updated_at
          FROM decisions
         WHERE deal_id=?
         ORDER BY updated_at DESC
    """, (deal_id,)).fetchall()
    con.close()

    states = []
    for r in rows:
        import json as _json
        try: offer = _json.loads(r["offer_json"] or "{}")
        except: offer = {}
        try: stips = _json.loads(r["stips_json"] or "{}")
        except: stips = {}
        states.append({
            "lender": r["lender"],
            "status": r["status"],
            "reason": r["reason"],
            "offer": offer,
            "stips": stips,
            "provider": r["provider"],
            "message_id": r["message_id"],
            "updated_at": r["updated_at"],
        })

    return jsonify({"ok": True, **result, "states": states})