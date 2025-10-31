#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Google OAuth (Gmail Send) — minimal, safe, and verifiable.

Routes:
  GET  /api/auth/ping
  GET  /api/auth/google/status
  GET  /api/auth/google/login
  GET  /api/auth/google/callback
  POST /api/auth/google/logout

Env (dev):
  export APP_ORIGIN="http://127.0.0.1:5055"
  export GOOGLE_OAUTH_CLIENT_SECRETS="/absolute/path/to/creds.json"
  export OAUTHLIB_INSECURE_TRANSPORT=1
  export ALLOWED_EMAIL="team@pathwaycatalyst.com"
"""

from __future__ import annotations

import json
import os
import pathlib
import time
from pathlib import Path
from typing import Optional, Tuple

import requests
from flask import Blueprint, jsonify, redirect, request, session
from google.auth.transport.requests import Request as GRequest
from google.oauth2 import id_token
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow

bp = Blueprint("auth_google", __name__)

# --- allow http:// for local development explicitly (oauthlib reads this at parse time) ---
if os.environ.get("OAUTHLIB_INSECURE_TRANSPORT") != "1":
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

BASE_DIR = Path(__file__).resolve().parent
TOKENS_DIR = BASE_DIR / "tokens"
TOKENS_DIR.mkdir(exist_ok=True)

SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.readonly",
]

ALLOWED_EMAIL = (os.environ.get("ALLOWED_EMAIL") or "team@pathwaycatalyst.com").lower()
ALLOWED_DOMAIN = ALLOWED_EMAIL.split("@", 1)[-1]


def _find_creds() -> Optional[Path]:
    # 1) explicit env override
    env = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRETS") or os.environ.get("GOOGLE_CLIENT_CONFIG")
    if env:
        p = Path(env)
        if p.exists():
            return p

    # 2) local filenames next to this file
    for name in ("creds.json", "credentials.json"):
        p = BASE_DIR / name
        if p.exists():
            return p
    return None


CREDS_PATH: Optional[Path] = _find_creds()


def _origin() -> str:
    o = os.environ.get("APP_ORIGIN")
    return (o or request.host_url.rstrip("/")).rstrip("/")


def _redirect_uri() -> str:
    return f"{_origin()}/api/auth/google/callback"


def _token_path(email: str) -> Path:
    safe = email.replace("/", "_")
    return TOKENS_DIR / f"{safe}.json"


def _save_creds(creds: Credentials, email: str) -> None:
    data = json.loads(creds.to_json())
    data["email"] = email
    _token_path(email).write_text(json.dumps(data, indent=2))


def _load_creds(email: str) -> Optional[Credentials]:
    p = _token_path(email)
    if not p.exists():
        return None
    data = json.loads(p.read_text() or "{}")
    try:
        return Credentials.from_authorized_user_info(data, scopes=SCOPES)
    except Exception:
        return None


def _refresh_if_needed(creds: Credentials) -> Credentials:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(GRequest())
    return creds


def _current_connection() -> Tuple[bool, Optional[str], Optional[Credentials]]:
    email = (session.get("google_email") or ALLOWED_EMAIL).lower()
    creds = _load_creds(email)
    if not creds:
        return False, None, None
    creds = _refresh_if_needed(creds)
    if not creds or not creds.valid:
        return False, None, None
    _save_creds(creds, email)
    session["google_email"] = email
    session["google_connected_at"] = int(time.time())
    return True, email, creds


@bp.get("/api/auth/ping")
def ping():
    return jsonify({"ok": True, "module": "auth_google"})


@bp.get("/api/auth/google/status")
def google_status():
    if not CREDS_PATH or not CREDS_PATH.exists():
        return jsonify({"connected": False, "email": None, "hint": "Missing creds.json"}), 200
    ok, email, _ = _current_connection()
    return jsonify({"connected": bool(ok), "email": email if ok else None}), 200


@bp.get("/api/auth/google/login")
def google_login():
    # force allow HTTP for local dev (oauthlib checks this before building auth URL sometimes)
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    if not CREDS_PATH or not CREDS_PATH.exists():
        return jsonify({"error": "creds.json not found; set GOOGLE_OAUTH_CLIENT_SECRETS"}), 500

    flow = Flow.from_client_secrets_file(str(CREDS_PATH), scopes=SCOPES, redirect_uri=_redirect_uri())
    auth_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent select_account",
        login_hint=ALLOWED_EMAIL,
        hd=ALLOWED_DOMAIN,
    )
    session["oauth_state"] = state
    session["allowed_email"] = ALLOWED_EMAIL
    return redirect(auth_url)


@bp.get("/api/auth/google/callback")
def google_callback():
    # force allow HTTP for local dev at the exact time oauthlib parses the callback URI
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    state = session.get("oauth_state")
    if not state:
        return redirect(f"{_origin()}/underwrite.html?connected=0&reason=missing_state")

    if not CREDS_PATH or not CREDS_PATH.exists():
        return redirect(f"{_origin()}/underwrite.html?connected=0&reason=missing_creds")

    flow = Flow.from_client_secrets_file(str(CREDS_PATH), scopes=SCOPES, state=state, redirect_uri=_redirect_uri())

    # This is where oauthlib enforces HTTPS unless OAUTHLIB_INSECURE_TRANSPORT=1 is set
    flow.fetch_token(authorization_response=request.url)

    creds = flow.credentials
    try:
        idinfo = id_token.verify_oauth2_token(creds.id_token, GRequest(), flow.client_config["client_id"])
        email = (idinfo.get("email") or "").lower()
    except Exception:
        email = ""

    allowed = (session.get("allowed_email") or ALLOWED_EMAIL).lower()
    if email != allowed:
        # Revoke the token we just got if the wrong account was used
        try:
            requests.post(
                "https://oauth2.googleapis.com/revoke",
                params={"token": creds.token},
                headers={"content-type": "application/x-www-form-urlencoded"},
                timeout=5,
            )
        except Exception:
            pass
        return redirect(f"{_origin()}/underwrite.html?connected=0&reason=wrong_account")

    _save_creds(creds, email)
    session["google_email"] = email
    session["google_connected_at"] = int(time.time())
    return redirect(f"{_origin()}/underwrite.html?connected=1")


@bp.post("/api/auth/google/logout")
def google_logout():
    ok, email, creds = _current_connection()
    if ok and creds:
        try:
            requests.post(
                "https://oauth2.googleapis.com/revoke",
                params={"token": creds.token},
                headers={"content-type": "application/x-www-form-urlencoded"},
                timeout=5,
            )
        except Exception:
            pass

    try:
        if email:
            p = _token_path(email)
            if p.exists():
                p.unlink()
    except Exception:
        pass

    session.pop("google_email", None)
    session.pop("google_connected_at", None)
    session.pop("oauth_state", None)
    session.pop("allowed_email", None)
    return jsonify({"ok": True})


# --- add to Statements/auth_google.py ---------------------------------------
# Public helpers so other modules can use the stored Gmail token.

def get_gmail_access_token(email: str) -> str:
    """
    Returns a valid OAuth access token for the given Gmail account.
    Relies on the token JSON saved by this blueprint in TOKENS_DIR.
    Raises RuntimeError if no valid token is available.
    """
    if not email:
        raise RuntimeError("get_gmail_access_token: email required")
    e = (email or "").strip().lower()
    creds = _load_creds(e)
    if not creds:
        raise RuntimeError(f"No stored token for {e}. Connect Gmail first.")
    creds = _refresh_if_needed(creds)
    if not creds or not creds.valid:
        raise RuntimeError(f"Token for {e} is invalid and could not be refreshed.")
    # persist any refresh to disk
    _save_creds(creds, e)
    return creds.token


def token_file_path(email: str) -> pathlib.Path:
    """Returns the exact token file path used for this email (for debugging)."""
    return _token_path((email or "").strip().lower())


# Nice-to-have explicit export list
__all__ = ["bp", "get_gmail_access_token", "token_file_path", "TOKENS_DIR"]
# ---------------------------------------------------------------------------
