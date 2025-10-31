# Statements/decisions/decisions_api.py
# Flask blueprint for lender decision tracking (Supabase-backed).

from __future__ import annotations

import os
import logging as log
from typing import Any, Dict, List, Optional

from flask import Blueprint, jsonify, request

# --- load .env early so imports below see env vars when run directly ---
try:
    from dotenv import load_dotenv, find_dotenv  # type: ignore
    load_dotenv(find_dotenv())
except Exception:
    pass

# We reuse the Supabase client from underwrite.py
# (that module already handles loading .env and raising if missing)
from underwrite import supabase  # noqa: E402

bp = Blueprint("decisions", __name__, url_prefix="/api/decisions")


# ------------------------------- Helpers ------------------------------------ #

def _n0(v: Any) -> int:
    try:
        return int(v or 0)
    except Exception:
        return 0


def _csv_to_list(s: Optional[str]) -> List[str]:
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


def _list_to_csv(items: Optional[List[str]]) -> str:
    if not items:
        return ""
    return ",".join([x for x in items if x])


# --------------------------- Supabase Operations ---------------------------- #

def decisions_by_deal(deal_id: str) -> List[Dict[str, Any]]:
    """Return all decisions for a deal, newest first."""
    r = (
        supabase.table("decisions")
        .select("*")
        .eq("deal_id", deal_id)
        .order("created_at", desc=True)
        .execute()
    )
    return r.data or []


def decision_get(decision_id: str) -> Optional[Dict[str, Any]]:
    r = (
        supabase.table("decisions")
        .select("*")
        .eq("id", decision_id)
        .limit(1)
        .execute()
    )
    rows = r.data or []
    return rows[0] if rows else None


def decision_insert(
    deal_id: str,
    lender_name: Optional[str] = None,
    subject: Optional[str] = None,
    status: Optional[str] = None,
    message_id: Optional[str] = None,
    thread_id: Optional[str] = None,
    reply_body: Optional[str] = None,
    reply_date: Optional[str] = None,  # ISO 8601 string or None
) -> str:
    payload = {
        "deal_id": deal_id,
        "lender_name": lender_name or "",
        "subject": subject or "",
        "status": status or "",
        "message_id": message_id or "",
        "thread_id": thread_id or "",
        "reply_body": reply_body or "",
        "reply_date": reply_date,  # pass-through; DB column is timestamptz
    }
    r = supabase.table("decisions").insert(payload).select("id").execute()
    if not r.data:
        raise RuntimeError("Failed to insert decision")
    return r.data[0]["id"]


def decision_update(decision_id: str, **fields) -> None:
    """Partial update. Only non-None keys will be written."""
    patch = {k: v for k, v in fields.items() if v is not None}
    if not patch:
        return
    supabase.table("decisions").update(patch).eq("id", decision_id).execute()


def decision_set_status(
    decision_id: str,
    status: str,
    reply_body: Optional[str] = None,
    reply_date: Optional[str] = None,
) -> None:
    decision_update(
        decision_id,
        status=status,
        reply_body=reply_body,
        reply_date=reply_date,
    )


def decision_set_thread_and_msg(
    decision_id: str,
    thread_id: Optional[str] = None,
    message_id: Optional[str] = None,
) -> None:
    decision_update(
        decision_id,
        thread_id=thread_id,
        message_id=message_id,
    )


def decisions_search_by_subject(subject_substring: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Case-insensitive subject search."""
    pattern = f"%{subject_substring}%"
    r = (
        supabase.table("decisions")
        .select("*")
        .ilike("subject", pattern)
        .order("created_at", desc=True)
        .limit(max(1, min(500, limit)))
        .execute()
    )
    return r.data or []


# --------------------------------- Routes ----------------------------------- #

@bp.get("/healthz")
def healthz():
    # no DB query here on purpose; this is a lightweight probe
    return jsonify({"ok": True, "service": "decisions"})


@bp.get("/by-deal/<deal_id>")
def api_list_by_deal(deal_id: str):
    try:
        rows = decisions_by_deal(deal_id)
        return jsonify({"decisions": rows})
    except Exception as e:
        log.exception("decisions.by_deal failed: %s", e)
        return jsonify({"error": "Failed to list decisions"}), 500


@bp.get("/<decision_id>")
def api_get(decision_id: str):
    try:
        row = decision_get(decision_id)
        if not row:
            return jsonify({"error": "not found"}), 404
        return jsonify({"decision": row})
    except Exception as e:
        log.exception("decisions.get failed: %s", e)
        return jsonify({"error": "Failed to get decision"}), 500


@bp.post("")
def api_insert():
    try:
        data = request.get_json(silent=True) or {}
        decision_id = decision_insert(
            deal_id=data.get("deal_id"),
            lender_name=data.get("lender_name"),
            subject=data.get("subject"),
            status=data.get("status"),
            message_id=data.get("message_id"),
            thread_id=data.get("thread_id"),
            reply_body=data.get("reply_body"),
            reply_date=data.get("reply_date"),
        )
        return jsonify({"ok": True, "id": decision_id}), 201
    except Exception as e:
        log.exception("decisions.insert failed: %s", e)
        return jsonify({"ok": False, "error": "Failed to insert decision"}), 500


@bp.patch("/<decision_id>")
def api_update(decision_id: str):
    try:
        data = request.get_json(silent=True) or {}
        # Allow partial updates of any known field
        allowed = {
            "deal_id",
            "lender_name",
            "subject",
            "status",
            "message_id",
            "thread_id",
            "reply_body",
            "reply_date",
        }
        patch = {k: v for k, v in data.items() if k in allowed}
        decision_update(decision_id, **patch)
        return jsonify({"ok": True})
    except Exception as e:
        log.exception("decisions.update failed: %s", e)
        return jsonify({"ok": False, "error": "Failed to update decision"}), 500


@bp.post("/<decision_id>/status")
def api_set_status(decision_id: str):
    try:
        data = request.get_json(silent=True) or {}
        status = data.get("status")
        if not status:
            return jsonify({"ok": False, "error": "status is required"}), 400
        decision_set_status(
            decision_id,
            status=status,
            reply_body=data.get("reply_body"),
            reply_date=data.get("reply_date"),
        )
        return jsonify({"ok": True})
    except Exception as e:
        log.exception("decisions.set_status failed: %s", e)
        return jsonify({"ok": False, "error": "Failed to set status"}), 500


@bp.post("/<decision_id>/thread")
def api_set_thread_and_message(decision_id: str):
    try:
        data = request.get_json(silent=True) or {}
        decision_set_thread_and_msg(
            decision_id,
            thread_id=data.get("thread_id"),
            message_id=data.get("message_id"),
        )
        return jsonify({"ok": True})
    except Exception as e:
        log.exception("decisions.set_thread failed: %s", e)
        return jsonify({"ok": False, "error": "Failed to update thread/message"}), 500


@bp.get("/search")
def api_search_subject():
    try:
        subject = request.args.get("subject") or request.args.get("q") or ""
        limit = _n0(request.args.get("limit") or 50)
        if not subject:
            return jsonify({"decisions": []})
        rows = decisions_search_by_subject(subject, limit=limit)
        return jsonify({"decisions": rows})
    except Exception as e:
        log.exception("decisions.search failed: %s", e)
        return jsonify({"error": "Failed to search decisions"}), 500
