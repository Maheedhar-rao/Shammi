# probe_auth.py
import os, requests
from flask import Blueprint, request, jsonify

bp = Blueprint("probe_auth", __name__, url_prefix="/api/auth/probe")

SUPABASE_URL  = os.environ["SUPABASE_URL"]
SUPABASE_ANON = os.environ["SUPABASE_ANON"]
AUTH_BASE     = f"{SUPABASE_URL.rstrip('/')}/auth/v1"

def _headers(token=None):
    bearer = token or SUPABASE_ANON
    return {"apikey": SUPABASE_ANON, "Authorization": f"Bearer {bearer}"}

@bp.get("/cookies")
def probe_cookies():
    at = request.cookies.get("sb-access-token")
    rt = request.cookies.get("sb-refresh-token")
    return jsonify({
        "saw_cookie_header": ("Cookie" in request.headers),
        "has_access_cookie": bool(at),
        "has_refresh_cookie": bool(rt),
        "host": request.host,
    }), 200

@bp.get("/supabase")
def probe_supabase():
    at = request.cookies.get("sb-access-token")
    if not at:
        return jsonify({"ok": False, "where": "probe_supabase", "error": "no_access_cookie"}), 401
    r = requests.get(f"{AUTH_BASE}/user", headers=_headers(at), timeout=10)
    try:
        body = r.json()
    except Exception:
        body = {"text": r.text}
    return jsonify({
        "ok": r.status_code < 400,
        "status": r.status_code,
        "body": body
    }), 200 if r.status_code < 400 else 401
