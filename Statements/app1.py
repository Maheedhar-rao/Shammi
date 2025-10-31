

from pathlib import Path
from flask import Flask, send_from_directory, Response, request, jsonify, redirect, session, Blueprint as blueprint
from flask_cors import CORS
from flask import request, make_response, session
import os, requests
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

BASE_DIR = Path(__file__).resolve().parent
PUBLIC_DIR = BASE_DIR / "public"  # Keep for backwards compatibility
PUB_DIR = BASE_DIR / "pub"        # Refactored version

SUPABASE_URL  = os.environ.get("SUPABASE_URL")
SUPABASE_ANON = os.environ.get("SUPABASE_ANON")
AUTH_BASE     = f"{SUPABASE_URL.rstrip('/')}/auth/v1" if SUPABASE_URL else None

app = Flask(
    __name__,
    static_folder=str(PUB_DIR),  # Changed to serve from pub/
    static_url_path=""
)
CORS(app, supports_credentials=True,
     resources={r"/api/*": {"origins": ["http://127.0.0.1:5056", "http://localhost:5056",]}})

app.secret_key = os.environ.get("APP_SECRET", "dev-secret")
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024


from underwrite import bp as underwrite_bp
from auth_google import bp as auth_google_bp
from auth_user import bp as auth_user_bp
from decisions.decisions_api import bp as decisions_bp
from auth_user_proxy import bp as auth_user_bp
from api_wrap import bp as wrap_bp
from probe_auth import bp as probe_bp
from auth_guard import global_auth_before_request
from flask_login import LoginManager
from feedback_api import bp as feedback_bp




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
app.register_blueprint(feedback_bp)
app.before_request(global_auth_before_request)
login_manager = LoginManager(app)

print("\n" + "=" * 50)
print("🔍 Checking /send route...")
send_routes = [r for r in app.url_map.iter_rules() if 'send' in r.rule]
if send_routes:
    for r in send_routes:
        print(f"✅ Found: {r.rule} → {r.endpoint}")
else:
    print("❌ No /send route found!")
print("=" * 50 + "\n")

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
    "/login.html",
    "/signup",
    "/signup.html",
    "/css/",              # Allow CSS files
    "/js/",               # Allow JS files
    "/api/auth/user/",
    "/api/auth/google/",
    "/healthz",
    "/favicon",
    "/debug/routes",
    "/api/leads",
    "/api/wrap",
)

bp = blueprint("underwrite", __name__)
DEV_BYPASS = os.getenv("DEV_BYPASS_AUTH") == "1"
OPEN_PATHS = {
    "/api/underwrite/wrap-secure",
    "/api/underwrite/v",
    "/api/underwrite/pixel",
    "/api/underwrite/analytics",
}

def _is_allowed(path: str) -> bool:
    return any(path.startswith(p) for p in ALLOW_PREFIXES)

def _sb_headers(token=None):
    bearer = token or SUPABASE_ANON
    return {"apikey": SUPABASE_ANON, "Authorization": f"Bearer {bearer}"}

def _get_access_token_from_request(req):
    auth = req.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:].strip()

    return req.cookies.get("sb-access-token")

@app.before_request
def require_login():
    path = request.path or "/"
    if _is_allowed(path):
        return

    if path in ("/", "/underwrite.html"):
        return

    if path.startswith("/api/"):
        dev_bypass = request.headers.get("X-User-Email") or request.args.get("dev_email")

        if session.get("user_email") or dev_bypass:
            return

        at = _get_access_token_from_request(request)
        if not at:
            return jsonify({"error": "not_authenticated"}), 401

        r = requests.get(f"{AUTH_BASE}/user", headers=_sb_headers(at), timeout=10)
        if r.status_code >= 400:
            return jsonify({"error": "invalid_token"}), 401


        return

    if not session.get("user_email"):
        return redirect("/login")

@app.get("/")
def root():
    if not session.get("user_email"):
        page = PUB_DIR / "login.html"
        if page.exists():
            return send_from_directory(app.static_folder, "login.html")
        return Response("<h3>Put login.html in Statements/pub/</h3>", mimetype="text/html")
    return send_from_directory(app.static_folder, "underwrite.html")

@app.get("/signup")
def signup_page():
    return send_from_directory(app.static_folder, "signup.html")

@app.get("/signup.html")
def signup_page_html():
    return send_from_directory(app.static_folder, "signup.html")

@app.get("/login")
def login_page():
    return send_from_directory(app.static_folder, "login.html")

@app.get("/login.html")
def login_page_html():
    return send_from_directory(app.static_folder, "login.html")

@app.get("/ui")
def ui():
    if not session.get("user_email"):
        return redirect("/login")
    page = PUB_DIR / "underwrite.html"
    if page.exists():
        return send_from_directory(app.static_folder, "underwrite.html")
    return Response("<h3>Put your UI at Statements/pub/underwrite.html</h3>", mimetype="text/html")

@app.get("/underwrite.html")
def underwrite_page():
    if not session.get("user_email"):
        return redirect("/login")
    return send_from_directory(app.static_folder, "underwrite.html")

# Serve CSS files from pub/css/
@app.get("/css/<path:filename>")
def serve_css(filename):
    return send_from_directory(PUB_DIR / "css", filename)

# Serve JS files from pub/js/
@app.get("/js/<path:filename>")
def serve_js(filename):
    return send_from_directory(PUB_DIR / "js", filename)

# Serve uploaded files
@app.get("/uploads/<path:filename>")
def serve_uploads(filename):
    UPLOADS_DIR = BASE_DIR / "uploads"
    return send_from_directory(UPLOADS_DIR, filename)

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


@app.get("/trace")
def serve_trace():
    return app.send_static_file("trace.html")



@bp.before_request
def _dev_bypass_auth():
    if not DEV_BYPASS:
        return None
    p = request.path

    for open_prefix in OPEN_PATHS:
        if p.startswith(open_prefix):
            return None
    return None


from underwrite import view_secure_pdf

@app.get("/v/<secure_token>")
def public_secure_view(secure_token):
    return view_secure_pdf(secure_token)





if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("🚀 Starting REFACTORED version (app1.py)")
    print("=" * 60)
    print("📂 Serving from: pub/ folder")
    print("🌐 URL: http://localhost:5056")
    print("=" * 60 + "\n")
    app.run(host="0.0.0.0", port=5056, debug=True)
