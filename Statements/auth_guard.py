# auth_guard.py
import os, requests, functools
from flask import request, jsonify, g

SUPABASE_URL  = os.environ["SUPABASE_URL"]
SUPABASE_ANON = os.environ["SUPABASE_ANON"]
AUTH_BASE     = f"{SUPABASE_URL.rstrip('/')}/auth/v1"

PUBLIC_PATHS = (
    "/",
    "/static/",

    # Auth flows that must remain public:
    "/api/auth/user/login",
    "/api/auth/user/signup",
    "/api/auth/user/reset",
    "/api/auth/user/status",
    "/api/auth/user/google/status",
    "/api/auth/user/refresh",

    # Probes you’re using to debug:
    "/api/auth/probe/cookies",
    "/api/auth/probe/supabase",
)

def _sb_headers(token=None):
    bearer = token or SUPABASE_ANON
    return {"apikey": SUPABASE_ANON, "Authorization": f"Bearer {bearer}"}

def _get_access_token():
    # Prefer Authorization header, else fall back to cookie (browser flow)
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:].strip()
    return request.cookies.get("sb-access-token")

def global_auth_before_request():
    # Allow public paths (and preflight)
    if request.method == "OPTIONS":
        return
    path = request.path
    if any(path.startswith(p) for p in PUBLIC_PATHS):
        return

    # Extract token from header or cookie
    at = _get_access_token()
    if not at:
        return jsonify({"error": "not_authenticated"}), 401

    # Validate with Supabase; stash user on g
    r = requests.get(f"{AUTH_BASE}/user", headers=_sb_headers(at), timeout=10)
    if r.status_code >= 400:
        try:
            body = r.json()
        except Exception:
            body = {"text": r.text}
        # Optional: print(body) for visibility
        return jsonify({"error": "invalid_token"}), 401

    g.user = r.json()
