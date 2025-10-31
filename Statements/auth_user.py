#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Email/password user auth (session-based).

Endpoints:
  POST /api/auth/user/signup         {email, password, first_name, last_name, mobile}
  POST /api/auth/user/login          {email, password}
  POST /api/auth/user/logout
  GET  /api/auth/user/status
  POST /api/auth/user/change-password {email, old_password, new_password}
"""

from __future__ import annotations
from flask import Blueprint, request, jsonify, session
from werkzeug.security import generate_password_hash, check_password_hash
import re, time, sqlite3

# Use the app's DB helper if present
try:
    from underwrite import _db  # type: ignore
except Exception:
    # Fallback: simple local sqlite (same shape as underwrite._db)
    import os
    from pathlib import Path
    _DB_PATH = Path(__file__).resolve().parent / "deals.db"
    def _db():
        con = sqlite3.connect(str(_DB_PATH))
        con.row_factory = sqlite3.Row
        return con

bp = Blueprint("auth_user", __name__)

EMAIL_RE = re.compile(r"^[^@]+@[^@]+\.[^@]+$")

FREE_DOMAINS = {
    "gmail.com","yahoo.com","outlook.com","hotmail.com","icloud.com","aol.com",
    "proton.me","protonmail.com","gmx.com","yandex.com","zoho.com","mail.com"
}

def _now() -> int:
    return int(time.time())

def _normalize_email(s: str) -> str:
    return (s or "").strip().lower()

def _domain(email: str) -> str:
    try:
        return _normalize_email(email).split("@", 1)[1]
    except Exception:
        return ""

def _company_domain_allowed(email: str) -> bool:
    d = _domain(email)
    if not d: return False
    # Allow Gmail explicitly, OR any domain that's not a common free provider
    return d == "gmail.com" or d not in FREE_DOMAINS

# -- DB bootstrap/migration ----------------------------------------------------
def _init_users():
    con = _db()
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS users (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              email TEXT UNIQUE NOT NULL,
              pw_hash TEXT NOT NULL,
              first_name TEXT,
              last_name TEXT,
              mobile TEXT,
              created_at INTEGER,
              last_login_at INTEGER
            )
        """)
        con.commit()

        # Migrations (add cols if missing)
        def ensure_col(name: str, sql: str):
            try:
                con.execute(f"SELECT {name} FROM users LIMIT 1")
            except Exception:
                con.execute(f"ALTER TABLE users ADD COLUMN {sql}")
                con.commit()

        ensure_col("first_name", "first_name TEXT")
        ensure_col("last_name",  "last_name  TEXT")
        ensure_col("mobile",     "mobile     TEXT")
        ensure_col("last_login_at", "last_login_at INTEGER")
    finally:
        con.close()

_init_users()

# -- API ----------------------------------------------------------------------

@bp.post("/api/auth/user/signup")
def user_signup():
    data = request.get_json(silent=True) or {}
    email = _normalize_email(data.get("email"))
    password = data.get("password") or ""
    first_name = (data.get("first_name") or "").strip()
    last_name  = (data.get("last_name")  or "").strip()
    mobile     = (data.get("mobile")     or "").strip()

    if not EMAIL_RE.match(email):
        return jsonify({"error": "invalid_email"}), 400
    if not _company_domain_allowed(email):
        return jsonify({"error": "domain_not_allowed", "hint": "Use company email or gmail.com"}), 400
    if len(password) < 8:
        return jsonify({"error": "weak_password", "hint": "Use at least 8 characters"}), 400
    if not first_name or not last_name:
        return jsonify({"error": "missing_fields", "hint": "First and last name are required"}), 400

    pw_hash = generate_password_hash(password, method="pbkdf2:sha256", salt_length=16)

    con = _db()
    try:
        con.execute(
            "INSERT INTO users (email, pw_hash, first_name, last_name, mobile, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (email, pw_hash, first_name, last_name, mobile, _now())
        )
        con.commit()
    except sqlite3.IntegrityError:
        con.close()
        return jsonify({"error": "email_exists"}), 409
    finally:
        try: con.close()
        except: pass

    session.clear()
    session["user_email"] = email
    session["google_email"] = email  # keeps the rest of your app happy

    return jsonify({"ok": True, "email": email})

@bp.post("/api/auth/user/login")
def user_login():
    data = request.get_json(force=True) or {}
    email = _normalize_email(data.get("email"))
    password = data.get("password") or ""

    con = _db()
    try:
        row = con.execute("SELECT pw_hash FROM users WHERE email=?", (email,)).fetchone()
        if not row:
            return jsonify({"error": "Invalid credentials."}), 401
        if not check_password_hash(row["pw_hash"], password):
            return jsonify({"error": "Invalid credentials."}), 401

        con.execute("UPDATE users SET last_login_at=? WHERE email=?", (_now(), email))
        con.commit()
    finally:
        con.close()

    session.clear()
    user = supabase.table("users").upsert({"email": email}, on_conflict="email").select("id").single().execute().data
    session["user_id"] = user["id"] 
    session["user_email"] = email
    session["google_email"] = email
    return jsonify({"ok": True, "email": email})

@bp.post("/api/auth/user/logout")
def user_logout():
    session.clear()
    return jsonify({"ok": True})

@bp.get("/api/auth/user/status")
def user_status():
    email = session.get("user_email")
    return jsonify({"logged_in": bool(email), "email": email})

@bp.post("/api/auth/user/change-password")
def change_password():
    """
    Body: {email, old_password, new_password}
    """
    data = request.get_json(force=True) or {}
    email = _normalize_email(data.get("email"))
    old_pw = data.get("old_password") or ""
    new_pw = data.get("new_password") or ""

    if not EMAIL_RE.match(email):
        return jsonify({"error": "invalid_email"}), 400
    if not old_pw or not new_pw:
        return jsonify({"error": "missing_fields"}), 400
    if len(new_pw) < 8:
        return jsonify({"error": "weak_password"}), 400

    con = _db()
    try:
        row = con.execute("SELECT pw_hash FROM users WHERE email=?", (email,)).fetchone()
        if not row:
            return jsonify({"error": "account_not_found"}), 404
        if not check_password_hash(row["pw_hash"], old_pw):
            return jsonify({"error": "old_password_incorrect"}), 403

        new_hash = generate_password_hash(new_pw, method="pbkdf2:sha256", salt_length=16)
        con.execute("UPDATE users SET pw_hash=? WHERE email=?", (new_hash, email))
        con.commit()
    finally:
        con.close()

    # keep the user signed in after change if desired
    session["user_email"] = email
    session["google_email"] = email
    return jsonify({"ok": True})
