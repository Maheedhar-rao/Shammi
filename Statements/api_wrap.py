# api_wrap.py
import os
import tempfile
from flask import Blueprint, request, send_file, jsonify, session, current_app
from supabase import create_client
from wrappers import issue_wrapper_user_branded  # adjust import path if needed

bp = Blueprint("wrap_api", __name__)

# ---- Config ----
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE = os.environ["SUPABASE_SERVICE_ROLE"]
WRAPPER_STORAGE_DIR = os.environ.get("WRAPPER_STORAGE_DIR", "./uploads/wrappers")

# Dev helpers
DEV_FAKE_USER_ID = os.environ.get("DEV_FAKE_USER_ID")          # e.g. f0d8fd0f-0726-4c33-9278-34ac8a29c7f8
USERS_TABLE = os.environ.get("USERS_TABLE", "users")           # override to a real view if you have one
DEFAULT_BUSINESS_NAME = os.environ.get("DEFAULT_BUSINESS_NAME", "Test Business")

def _sb():
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE)

def _resolve_user_id(email: str) -> str:
    """
    Resolve the user id for wrapping/tracking.
    Priority:
      1) session["user_id"] if your app sets it
      2) DEV_FAKE_USER_ID (skip DB)  <-- THIS IS THE KEY FIX
      3) Lookup USERS_TABLE by email (requires that table/view to exist)
    """
    sid = session.get("user_id")
    if sid:
        current_app.logger.info("wrap: using session user_id=%s", sid)
        return str(sid)

    if DEV_FAKE_USER_ID:
        current_app.logger.info("wrap: using DEV_FAKE_USER_ID=%s", DEV_FAKE_USER_ID)
        return DEV_FAKE_USER_ID

    # DB lookup (only works if you exposed a suitable table/view)
    try:
        res = _sb().table(USERS_TABLE).select("id").eq("email", email.lower()).limit(1).execute()
        rows = getattr(res, "data", None) or []
        if not rows:
            raise ValueError(f"{USERS_TABLE}.id not found for {email}")
        uid = rows[0]["id"]
        current_app.logger.info("wrap: resolved user_id from %s: %s", USERS_TABLE, uid)
        return uid
    except Exception as e:
        raise ValueError(
            f"User lookup failed: {e}. Set DEV_FAKE_USER_ID or create a row in '{USERS_TABLE}' (id,email)."
        )

def _coerce_deal_id() -> int:
    raw = (request.form.get("deal_id") or request.args.get("deal_id") or "").strip()
    if raw.isdigit():
        return int(raw)
    # allow testing without a real deal row
    return 909

def _ensure_deal_exists(deal_id: int, business_name: str):
    """
    Dev helper: create a minimal 'deals' row if missing.
    Will no-op in prod if RLS disallows it.
    """
    try:
        sb = _sb()
        existing = sb.table("deals").select("id").eq("id", deal_id).limit(1).execute().data or []
        if existing:
            return
        sb.table("deals").insert({
            "id": deal_id,
            "application_json": {"business_name": business_name},
        }).execute()
        current_app.logger.info("wrap: created stub deals.id=%s (%s)", deal_id, business_name)
    except Exception as e:
        current_app.logger.warning("wrap: ensure_deal_exists failed (ok in prod): %s", e)

@bp.route("/api/wrap", methods=["POST"])
def api_wrap():
    # Accept browser session OR dev header/query
    email = (
        session.get("user_email")
        or request.headers.get("X-User-Email")
        or request.args.get("dev_email")
    )
    if not email:
        return jsonify({"error": "not_authenticated", "guard": "handler"}), 401

    if "file" not in request.files:
        return jsonify({"error": "missing file"}), 400

    file = request.files["file"]
    if not (file.filename or "").lower().endswith(".pdf"):
        return jsonify({"error": "only_pdf_supported"}), 400

    # Form fields (frontend must send "lender", not "funder_name")
    lender = (request.form.get("lender") or "").strip()
    recipient_email = request.form.get("recipient_email") or email
    deal_id = _coerce_deal_id()

    # Force-params for reliable visible overlay during rollout
    force_watermark_text = request.form.get("force_watermark_text") or None
    force_footer_template = request.form.get("force_footer_template") or None
    force_logo_path = request.form.get("force_logo_path") or None
    force_logo_max_in = request.form.get("force_logo_max_in")
    force_logo_max_pct = request.form.get("force_logo_max_pct")

    # Coerce numeric overrides if present
    try:
        force_logo_max_in = float(force_logo_max_in) if force_logo_max_in is not None else None
    except ValueError:
        force_logo_max_in = None
    try:
        force_logo_max_pct = float(force_logo_max_pct) if force_logo_max_pct is not None else None
    except ValueError:
        force_logo_max_pct = None

    os.makedirs(WRAPPER_STORAGE_DIR, exist_ok=True)

    # Save upload to a temp file
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        src_path = tmp.name
        file.stream.seek(0)
        tmp.write(file.read())

    try:
        # Resolve user id (now uses DEV_FAKE_USER_ID if set)
        try:
            user_id = _resolve_user_id(email)
        except Exception as ue:
            return jsonify({"error": "user_id_missing", "detail": str(ue)}), 400

        # Dev convenience: ensure a minimal deal row exists so wrappers.py can read business_name
        _ensure_deal_exists(deal_id, DEFAULT_BUSINESS_NAME)

        # Call the wrapper
        res = issue_wrapper_user_branded(
            user_id=user_id,
            deal_id=deal_id,
            original_pdf_path=src_path,
            funder_name=lender or "Unknown Lender",
            recipient_email=recipient_email,
            storage_dir=WRAPPER_STORAGE_DIR,
            supabase_url=SUPABASE_URL,
            supabase_service_key=SUPABASE_SERVICE_ROLE,
            # force overrides to guarantee visibility while rolling out
            force_watermark_text=force_watermark_text or (f"{lender} • CONFIDENTIAL" if lender else None),
            force_footer_template=force_footer_template or "For: {funder} • {recipient} • Deal: {deal} • Track: {fp}",
            force_logo_path=force_logo_path,
            force_logo_max_in=force_logo_max_in,
            force_logo_max_pct=force_logo_max_pct,
        )

        out_path = res["wrapper_path"]
        out_name = res["wrapper_filename"]

        # Diagnostics mode: return JSON instead of PDF stream
        if request.args.get("mode") == "json":
            size = os.path.getsize(out_path) if os.path.exists(out_path) else 0
            return jsonify({
                "ok": True,
                "deal_id": deal_id,
                "business_name": res.get("business_name"),
                "wrapper_filename": out_name,
                "wrapper_path": out_path,
                "size_bytes": size,
                "diagnostics": res.get("diagnostics"),
                "lender_overrides": res.get("lender_overrides"),
                "logo_path_used": res.get("logo_path"),
            })

        # Default: stream the wrapped PDF
        return send_file(
            out_path,
            mimetype="application/pdf",
            as_attachment=True,
            download_name=out_name,
            max_age=0,
            etag=False,
            conditional=False,
        )

    except Exception as e:
        current_app.logger.exception("wrap failed")
        return jsonify({"error": "wrap_failed", "detail": str(e)}), 500

    finally:
        try:
            os.remove(src_path)
        except Exception:
            pass
