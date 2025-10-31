#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
from flask import Flask, send_from_directory, Response, request, jsonify, redirect, session
from flask_cors import CORS
from flask import request, make_response, session
import os, requests
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

BASE_DIR = Path(__file__).resolve().parent
PUBLIC_DIR = BASE_DIR / "public"   

# Read Supabase credentials from environment
SUPABASE_URL  = os.environ.get("SUPABASE_URL")
SUPABASE_ANON = os.environ.get("SUPABASE_ANON")
AUTH_BASE     = f"{SUPABASE_URL.rstrip('/')}/auth/v1" if SUPABASE_URL else None

app = Flask(
    __name__,
    static_folder=str(PUBLIC_DIR),
    static_url_path=""  
)
CORS(appsupports_credentials=True,
     resources={r"/api/*": {"origins": ["http://127.0.0.1:5055", "http://localhost:5055",]}})

app.secret_key = os.environ.get("APP_SECRET", "dev-secret")
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB


from underwrite import bp as underwrite_bp          
from auth_google import bp as auth_google_bp        
from auth_user import bp as auth_user_bp            
from decisions.decisions_api import bp as decisions_bp  
from auth_user_proxy import bp as auth_user_bp   
from api_wrap import bp as wrap_bp
from probe_auth import bp as probe_bp
from auth_guard import global_auth_before_request
from flask_login import LoginManager





print("\n-- ROUTES --")
for r in app.url_map.iter_rules():
    if "/api/auth/user" in str(r):
        print(f"{r}  ->  {app.view_functions[r.endpoint].__module__}.{app.view_functions[r.endpoint].__name__}")
print("-- END ROUTES --\n")


app.register_blueprint(underwrite_bp, url_prefix="/api/underwrite")
app.register_blueprint(auth_google_bp)          
app.register_blueprint(auth_user_bp)            
app.register_blueprint(decisions_bp)   
app.register_blueprint(wrap_bp)         
app.register_blueprint(probe_bp)
app.before_request(global_auth_before_request)
login_manager = LoginManager(app)


@app.after_request
def add_no_store_headers(resp):
    ct = (resp.headers.get("Content-Type") or "").lower()
    if "text/html" in ct:
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0, private"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
    return resp

ALLOW_PREFIXES = (
    "/login",
     "/signup.html",                     
    "/api/auth/user/",          
    "/api/auth/google/",        
    "/healthz",
    "/favicon",                 
    "/debug/routes",
    "/api/leads",
    "/api/wrap", 
)

def _is_allowed(path: str) -> bool:
    return any(path.startswith(p) for p in ALLOW_PREFIXES)

def _sb_headers(token=None):
    bearer = token or SUPABASE_ANON
    return {"apikey": SUPABASE_ANON, "Authorization": f"Bearer {bearer}"}

def _get_access_token_from_request(req):
    # 1) Authorization: Bearer ...
    auth = req.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:].strip()
    # 2) sb-access-token cookie (Supabase browser flow)
    return req.cookies.get("sb-access-token")

@app.before_request
def require_login():
    path = request.path or "/"
    if _is_allowed(path):
        return

    if path in ("/", "/underwrite.html"):
        return

    # API protection: allow if session OR Supabase JWT OR explicit dev bypass.
    if path.startswith("/api/"):
        # dev bypass (only if you really want it)
        dev_bypass = request.headers.get("X-User-Email") or request.args.get("dev_email")

        if session.get("user_email") or dev_bypass:
            return  # allowed via legacy session or bypass

        # Try Supabase JWT (header or cookie)
        at = _get_access_token_from_request(request)
        if not at:
            return jsonify({"error": "not_authenticated"}), 401

        # Validate token with Supabase
        r = requests.get(f"{AUTH_BASE}/user", headers=_sb_headers(at), timeout=10)
        if r.status_code >= 400:
            return jsonify({"error": "invalid_token"}), 401

        # Optionally stash user on session/g for downstream use:
        # session['user_email'] = r.json().get('email')  # if you want legacy UIs to see it
        return

    # Non-API pages: keep your legacy session gate
    if not session.get("user_email"):
        return redirect("/login")

@app.get("/")
def root():
    if not session.get("user_email"):
        page = PUBLIC_DIR / "login.html"
        if page.exists():
            return send_from_directory(app.static_folder, "login.html")
        return Response("<h3>Put login.html in Statements/public/</h3>", mimetype="text/html")
    return send_from_directory(app.static_folder, "underwrite.html")

@app.get("/signup")                                 
def signup_page():
    page = PUBLIC_DIR / "signup.html"
    if page.exists():
        return send_from_directory(app.static_folder, "signup.html")
    return Response("<h3>Put signup.html in Statements/public/</h3>", mimetype="text/html")


@app.get("/login")
def login_page():
    page = PUBLIC_DIR / "login.html"
    if page.exists():
        return send_from_directory(app.static_folder, "login.html")
    return Response("<h3>Put login.html in Statements/public/</h3>", mimetype="text/html")

@app.get("/ui")
def ui():
    if not session.get("user_email"):
        return redirect("/login")
    page = PUBLIC_DIR / "underwrite.html"
    if page.exists():
        return send_from_directory(app.static_folder, "underwrite.html")
    return Response("<h3>Put your UI at Statements/public/underwrite.html</h3>", mimetype="text/html")

@app.get("/favicon.ico")
def favicon():
    return ("", 204)

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.get("/debug/routes")
def debug_routes():
    out = []
    for rule in app.url_map.iter_rules():
        methods = ",".join(sorted(m for m in rule.methods if m not in ("HEAD","OPTIONS")))
        out.append(f"{methods:10s}  {rule.rule}  -> {rule.endpoint}")
    return {"routes": sorted(out)}

@app.get("/api/whoami")
def whoami():
    from flask import request, session
    return {
        "session_user_email": session.get("user_email"),
        "header_x_user_email": request.headers.get("X-User-Email"),
        "args_dev_email": request.args.get("dev_email"),
        "path": request.path,
    }

@login_manager.unauthorized_handler
def _unauth():
    return jsonify({"error": "not_authenticated"}), 401





if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5055)) 
    app.run(host="0.0.0.0", port=port, debug=True)
