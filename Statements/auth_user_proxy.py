# Statements/auth_user_proxy.py
from __future__ import annotations
import os, requests
from flask import Blueprint, request, jsonify, make_response
import json, base64


bp = Blueprint("auth_user_proxy", __name__, url_prefix="/api/auth/user")

SUPABASE_URL  = os.environ["SUPABASE_URL"]
SUPABASE_ANON = os.environ["SUPABASE_ANON"]  # keep server-side; do NOT expose to browser
AUTH_BASE = f"{SUPABASE_URL.rstrip('/')}/auth/v1"

# Cookie settings (httpOnly so JS cannot read tokens)
COOKIE_SECURE = os.getenv("COOKIE_SECURE", "false").lower() in ("1","true","yes")
COOKIE_DOMAIN = os.getenv("COOKIE_DOMAIN") or None
COOKIE_MAX_AGE = 60 * 60 * 24 * 7  # 7 days

def _headers(token: str | None = None):
    # For GoTrue: send apikey and a Bearer token.
    # When token is None, we use the anon key for both Authorization and apikey.
    bearer = token or SUPABASE_ANON
    return {
        "apikey": SUPABASE_ANON,
        "Authorization": f"Bearer {bearer}",
        "Content-Type": "application/json"
    }

def _set_auth_cookies(resp, access_token: str, refresh_token: str):
    resp.set_cookie("sb-access-token", access_token,
                    max_age=COOKIE_MAX_AGE, httponly=True, secure=False,
                    samesite="Lax", path="/", domain=COOKIE_DOMAIN)
    resp.set_cookie("sb-refresh-token", refresh_token,
                    max_age=COOKIE_MAX_AGE, httponly=True, secure=False,
                    samesite="Lax", path="/", domain=COOKIE_DOMAIN)

def _clear_auth_cookies(resp):
    for name in ("sb-access-token","sb-refresh-token"):
        resp.delete_cookie(name, path="/", domain=COOKIE_DOMAIN, samesite="None")

@bp.post("/signup")
def signup():
    body = request.get_json(silent=True) or {}
    email = (body.get("email") or "").strip()
    password = body.get("password") or ""
    profile = body.get("profile") or {}
    if not email or not password:
        return jsonify({"ok": False, "error": "email and password required"}), 400

    r = requests.post(f"{AUTH_BASE}/signup", headers=_headers(),
                      json={"email": email, "password": password, "data": profile}, timeout=15)
    if r.status_code >= 400:
        try:
            msg = r.json().get("msg") or r.json().get("error_description") or r.text
        except Exception:
            msg = r.text
        return jsonify({"ok": False, "error": msg}), 400

    # Most setups have confirm-email=ON, so no session here. That’s fine.
    return jsonify({"ok": True, "confirmation_sent": True, "user": r.json().get("user")}), 200

@bp.post("/login")
def login():
    body = request.get_json(silent=True) or {}
    email = (body.get("email") or "").strip()
    password = body.get("password") or ""
    if not email or not password:
        return jsonify({"ok": False, "error": "email and password required"}), 400

    r = requests.post(f"{AUTH_BASE}/token?grant_type=password", headers=_headers(),
                      json={"email": email, "password": password}, timeout=15)
    if r.status_code >= 400:
        try:
            msg = r.json().get("msg") or r.json().get("error_description") or r.text
        except Exception:
            msg = r.text
        return jsonify({"ok": False, "error": msg}), 401

    j = r.json()
    at = j.get("access_token"); rt = j.get("refresh_token")
    if not at or not rt:
        return jsonify({"ok": False, "error": "missing tokens"}), 500

    resp = make_response(jsonify({"ok": True}))
    _set_auth_cookies(resp, at, rt)
    return resp

@bp.post("/reset")
def reset():
    body = request.get_json(silent=True) or {}
    email = (body.get("email") or "").strip()
    if not email:
        return jsonify({"ok": False, "error": "email required"}), 400
    r = requests.post(f"{AUTH_BASE}/recover", headers=_headers(), json={"email": email}, timeout=15)
    ok = r.status_code < 400
    return (jsonify({"ok": ok}), 200 if ok else 400)

@bp.post("/change-password")
def change_password():
    body = request.get_json(silent=True) or {}
    email = (body.get("email") or "").strip()
    old_password = body.get("old_password") or ""
    new_password = body.get("new_password") or ""
    if not email or not new_password:
        return jsonify({"ok": False, "error": "email and new_password required"}), 400

    # If there is an access token cookie, use it; else sign in to get a short session
    at = request.cookies.get("sb-access-token")
    if not at:
        if not old_password:
            return jsonify({"ok": False, "error": "old_password required without active session"}), 400
        r = requests.post(f"{AUTH_BASE}/token?grant_type=password", headers=_headers(),
                          json={"email": email, "password": old_password}, timeout=15)
        if r.status_code >= 400:
            return jsonify({"ok": False, "error": "sign-in failed"}), 401
        at = r.json().get("access_token")

    r2 = requests.put(f"{AUTH_BASE}/user", headers=_headers(at), json={"password": new_password}, timeout=15)
    if r2.status_code >= 400:
        try:
            msg = r2.json().get("msg") or r2.json().get("error_description") or r2.text
        except Exception:
            msg = r2.text
        return jsonify({"ok": False, "error": msg}), 400

    return jsonify({"ok": True}), 200

@bp.post("/logout")
def logout():
    at = request.cookies.get("sb-access-token")
    if at:
        try:
            requests.post(f"{AUTH_BASE}/logout", headers=_headers(at), timeout=10)
        except Exception:
            pass
    resp = make_response(jsonify({"ok": True}))
    _clear_auth_cookies(resp)
    return resp

@bp.get("/status")
def status():
    """
    Reports if a Supabase session is present.
    Returns multiple "truthy" flags so legacy guards won't bounce the user.
    """
    at = request.cookies.get("sb-access-token")
    if not at:
        return jsonify({
            "ok": True, "success": True,
            "authenticated": False, "authorized": False, "logged_in": False, "loggedIn": False,
            "user": None
        }), 200

    # Validate token with Supabase (authoritative)
    r = requests.get(f"{AUTH_BASE}/user", headers=_headers(at), timeout=10)
    claims = _jwt_claims(at)
    if r.status_code >= 400:
        return jsonify({
            "ok": True, "success": True,
            "authenticated": False, "authorized": False, "logged_in": False, "loggedIn": False,
            "user": None
        }), 200

    u = r.json()  # Supabase user object
    # Normalize & include lots of aliases for maximum compatibility
    user_basic = {
        "id": u.get("id") or claims.get("sub"),
        "email": u.get("email") or claims.get("email"),
        "role": u.get("role") or claims.get("role"),
    }
    return jsonify({
        "ok": True, "success": True,
        "authenticated": True, "authorized": True, "logged_in": True, "loggedIn": True,
        "user": user_basic,
        # Extras some guards read:
        "email": user_basic["email"],
        "uid": user_basic["id"], "user_id": user_basic["id"], "sub": user_basic["id"],
        "exp": claims.get("exp"), "iat": claims.get("iat"),
    }), 200

@bp.get("/google/status")
def google_status():
    # If you aren't using Google, return a stable "not linked" shape.
    return jsonify({"ok": True, "connected": False, "authorized": True}), 200

@bp.get("/me")
def me():
    at = request.cookies.get("sb-access-token")
    if not at:
        return jsonify({"user": None})
    r = requests.get(f"{AUTH_BASE}/user", headers=_headers(at), timeout=10)
    if r.status_code >= 400:
        return jsonify({"user": None})
    return jsonify(r.json())

@bp.post("/refresh")
def refresh():
    rt = request.cookies.get("sb-refresh-token")
    if not rt:
        return jsonify({"ok": False, "error": "no refresh token"}), 401

    r = requests.post(f"{AUTH_BASE}/token?grant_type=refresh_token",
                      headers=_headers(),
                      json={"refresh_token": rt}, timeout=15)

    if r.status_code >= 400:
        return jsonify({"ok": False, "error": "refresh failed"}), 401

    j = r.json()
    at = j.get("access_token"); new_rt = j.get("refresh_token") or rt
    resp = make_response(jsonify({"ok": True}))
    _set_auth_cookies(resp, at, new_rt)
    return resp


def _jwt_claims(token: str):
    try:
        body = token.split(".")[1]
        body += "=" * (-len(body) % 4)  # pad
        return json.loads(base64.urlsafe_b64decode(body))
    except Exception:
        return {}
