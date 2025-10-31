#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from dataclasses import asdict
from inspect import signature
from flask import Blueprint, jsonify, request, session
from werkzeug.utils import secure_filename
from wrappers import wrap_pdf_with_logo
from flask import send_file, abort
from uuid import UUID, uuid4




from flask import send_file, Response
import csv, io, re, time
import base64
import email
import email.policy
import email.utils
import email.mime.multipart
import email.mime.text
import email.mime.base
import mimetypes
import json
import logging
import os
import traceback
import requests
from datetime import datetime, timezone
import time as _time
import secrets
import uuid, re

from supabase import create_client, Client
from auth_guard import global_auth_before_request
# ------------------------------------------------------------------------------
# Supabase client
# ------------------------------------------------------------------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE = os.environ.get("SUPABASE_SERVICE_ROLE")
if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE in environment.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE)
sb = supabase

def get_sb() -> Client:
    return sb

# ------------------------------------------------------------------------------
# Paths / constants
# ------------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
EMAILS_BOOK_DIR = BASE_DIR / "emails-books"
EMAILS_BOOK_DIR.mkdir(exist_ok=True)
EMAILS_DIR = EMAILS_BOOK_DIR
LOGO_PATH = str(BASE_DIR / "static" / "logo.png")


EMAILS_JSON_PATH = Path(os.environ.get("LENDER_EMAILS_PATH", str(BASE_DIR / "emails.json")))
AUTH_STORE_PATH = Path(os.environ.get("AUTH_STORE_PATH", str(BASE_DIR / "auth_store.json")))
TOKENS_DIR = BASE_DIR / "tokens"
GOOGLE_TOKEN_FILE = TOKENS_DIR / "google.json"

bp = Blueprint("underwrite", __name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("underwrite")

# ------------------------------------------------------------------------------
# Optional extractors / rules
# ------------------------------------------------------------------------------
try:
    import Application_extractor as appx
except Exception as e:
    appx = None
    log.exception("Failed to import Application_extractor: %s", e)

try:
    import Statements_extractor as stx
except Exception as e:
    stx = None
    log.exception("Failed to import Statements_extractor: %s", e)

try:
    import lenders_rules as rules
except Exception as e:
    rules = None
    log.exception("Failed to import lenders_rules: %s", e)

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
def _unique_name(base: str) -> str:
    return f"{int(time.time())}_{secrets.token_hex(3)}_{secure_filename(base)}"

def _save_upload(fs_obj, prefix: str = "") -> dict:
    """Save a FileStorage to uploads/ and return dict(path, filename)."""
    UPLOAD_DIR.mkdir(exist_ok=True)
    name = _unique_name(f"{prefix}_{fs_obj.filename or 'file'}")
    path = UPLOAD_DIR / name
    fs_obj.save(path)
    return {"path": str(path), "filename": name}

def _inject_length_months(application: dict) -> dict:
    app = dict(application or {})
    lm = app.get("length_months")
    try:
        if lm is not None and float(lm) >= 0:
            return app
    except Exception:
        pass
    txt = (app.get("length_of_ownership") or app.get("LengthOfOwnership") or app.get("lengthOfOwnership") or "")
    if not isinstance(txt, str):
        app["length_months"] = None
        return app
    txt_low = txt.lower()
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*months?", txt_low)
    if m:
        try: app["length_months"] = float(m.group(1)); return app
        except Exception: pass
    y = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*years?", txt_low)
    if y:
        try: app["length_months"] = float(y.group(1)) * 12.0; return app
        except Exception: pass
    y2 = re.search(r"\(([0-9]+(?:\.[0-9]+)?)\s*years?\)", txt_low)
    if y2:
        try: app["length_months"] = float(y2.group(1)) * 12.0; return app
        except Exception: pass
    app["length_months"] = None
    return app

def _safe_email(s: str) -> str:
    return re.sub(r"[^a-z0-9_.+-]+", "_", (s or "").strip().lower())

# ------------------------------------------------------------------------------
# Extraction
# ------------------------------------------------------------------------------
def _extract_application_fields(app_pdf_path: Path) -> Dict:
    if appx is None:
        raise RuntimeError("Application_extractor not available")
    with open(app_pdf_path, "rb") as f:
        pdf_bytes = f.read()
    results, preview = appx.extract_fields_from_bytes(pdf_bytes)
    def val(key: str):
        fr = results.get(key)
        return getattr(fr, "value", None)
    out = {
        "business_name":          val("BusinessName"),
        "state":                  val("State"),
        "industry":               val("Industry"),
        "fico":                   val("FICO"),
        "length_of_ownership":    val("LengthOfOwnership"),
        "_preview":               (preview[:2000] if isinstance(preview, str) else None),
    }
    out = {k: v for k, v in out.items() if v is not None}
    out = _inject_length_months(out)
    return out

# Statement summarization / aggregation
def _summarize_one_statement_from_bytes(pdf_bytes: bytes, filename: Optional[str]) -> Dict:
    if stx is None:
        raise RuntimeError("Statements_extractor not available")
    summary, daily, txns = stx.summarize_statement_from_bytes(pdf_bytes, filename=filename)
    summary_dict = asdict(summary) if hasattr(summary, "__dataclass_fields__") else dict(summary)
    debit_counts, credit_counts, monthly_deposits = stx.compute_monthly_counts_and_deposits(txns)
    summary_dict["_monthly_deposits"] = monthly_deposits
    summary_dict["_debit_counts"] = debit_counts
    summary_dict["_credit_counts"] = credit_counts
    summary_dict["source_file"] = filename
    return summary_dict

def _aggregate_statements_and_revenue(per_statement: List[Dict], state_for_rule: Optional[str]) -> Dict:
    monthly_deposits: Dict[str, float] = {}
    total_neg_days = 0
    adb_values: List[float] = []
    total_debits = 0
    total_credits = 0
    for s in per_statement:
        for ym, amt in (s.get("_monthly_deposits") or {}).items():
            try:
                monthly_deposits[ym] = monthly_deposits.get(ym, 0.0) + float(amt)
            except Exception:
                pass
        try: total_neg_days += int(s.get("negative_ending_days") or 0)
        except Exception: pass
        try:
            adb = s.get("average_daily_balance")
            if adb is not None: adb_values.append(float(adb))
        except Exception: pass
        try: total_debits += int(s.get("debit_count") or 0)
        except Exception: pass
        try: total_credits += int(s.get("credit_count") or 0)
        except Exception: pass
    avg_adb = round(sum(adb_values) / len(adb_values), 2) if adb_values else None
    avg_revenue = stx.pick_avg_revenue(monthly_deposits, state_for_rule) if hasattr(stx, "pick_avg_revenue") else None
    rule = "NY/CA: average of best 3 months; others: average of all months" if avg_revenue is not None else None
    return {
        "monthly_deposits": monthly_deposits,
        "average_revenue": avg_revenue,
        "avg_revenue_rule": rule,
        "aggregate_negative_days": total_neg_days,
        "aggregate_debit_count": total_debits,
        "aggregate_credit_count": total_credits,
        "average_daily_balance": avg_adb,
    }

def _build_statements_payload(files: List[Tuple[str, bytes]], state_for_rule: Optional[str]) -> Dict:
    per_statement: List[Dict] = []
    for fname, pdf_bytes in files:
        try:
            summary_dict = _summarize_one_statement_from_bytes(pdf_bytes, filename=fname)
            per_statement.append(summary_dict)
        except Exception:
            log.exception("Failed to summarize statement: %s", fname)
    aggregates = _aggregate_statements_and_revenue(per_statement, state_for_rule)
    for s in per_statement:
        s.pop("_monthly_deposits", None)
        s.pop("_debit_counts", None)
        s.pop("_credit_counts", None)
    return {"per_statement": per_statement, **aggregates}

# Lender matching
def _match_lenders(application: Dict, statements: Dict) -> List[Dict]:
    if rules is None:
        log.error("lenders_rules module not available")
        return []
    try:
        return rules.generate_lenders(application, statements)
    except Exception:
        log.exception("Error when generating lenders")
        return []

def _append_default_lenders(lenders: List[Dict]) -> List[Dict]:
    lenders = list(lenders or [])
    have = {(x.get("business_name") or "").strip().lower() for x in lenders}
    for nm in ("test", "testing"):
        if nm not in have:
            lenders.append({"business_name": nm, "score": 1.0, "reason": "Default test lender"})
    return lenders

# ------------------------------------------------------------------------------
# Email recipients book
# ------------------------------------------------------------------------------
def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

def _parse_row_emails(s: str) -> List[str]:
    return [e.strip() for e in str(s or "").split(",") if e and e.strip()]

# NOTE: We keep only the robust CSV+JSON loader (avoid duplicate definitions).
def _user_email() -> str:
    return (session.get("google_email") or session.get("user_email") or "").strip().lower()

def _safe_email_to_fname(email: str) -> str:
    return re.sub(r"[^a-z0-9_.-]+", "_", (email or "").lower())

def _user_emails_csv_path(user: Optional[str] = None) -> Optional[Path]:
    user = (user or _user_email() or "").strip().lower()
    if not user:
        return None
    return EMAILS_BOOK_DIR / f"{_safe_email_to_fname(user)}.csv"

def _split_emails(s: str):
    return [e.strip() for e in re.split(r"[;,]", s or "") if e.strip()]

def _parse_emails_csv_bytes(raw: bytes) -> dict:
    book = {}
    text = raw.decode("utf-8", errors="ignore")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        return {}
    headers = { (h or "").strip().lower(): h for h in reader.fieldnames }
    def _get(row, key):
        return (row.get(headers.get(key, key), "") or "").strip()
    def _split(s):
        return [p.strip() for p in re.split(r"[;,]", s or "") if p.strip()]
    for row in reader:
        lender = _get(row, "lender") or _get(row, "name") or _get(row, "business")
        if not lender:
            continue
        to_list = _split(_get(row, "to") or _get(row, "email"))
        cc_list = _split(_get(row, "cc"))
        book[(lender or "").strip().lower()] = {"to": to_list, "cc": cc_list}
    return book

def _load_emails_book(email: str) -> tuple[dict, int]:
    p = _user_emails_csv_path(email)
    if not p or not p.exists():
        return {}, 0
    raw = p.read_bytes()
    book = _parse_emails_csv_bytes(raw)
    ts = int(p.stat().st_mtime)
    return book, ts

def _dedupe_emails(lst):
    seen = set()
    out = []
    for x in (lst or []):
        k = (x or "").strip().lower()
        if not k or k in seen:
            continue
        seen.add(k); out.append(x)
    return out

def resolve_recipients(lender_name: str, extra_cc: Optional[List[str]] = None) -> Tuple[Optional[str], List[str]]:
    """
    Legacy global JSON emails.json fallback (kept for compatibility).
    Format can be:
      - { "LenderA": "to@example.com", ... }
      - { "LenderA": {"to":"...", "cc":[...]}, ... }
      - [ {"lender":"...","to":"...","cc":"..."}, ... ]
    """
    try:
        if EMAILS_JSON_PATH and EMAILS_JSON_PATH.exists():
            raw = json.loads(EMAILS_JSON_PATH.read_text() or "{}")
            key = (lender_name or "").strip().lower()
            to_email, cc_list = "", []
            if isinstance(raw, dict):
                v = raw.get(lender_name) or raw.get(key)
                if isinstance(v, str):
                    to_email = v
                elif isinstance(v, dict):
                    to_email = v.get("to") or v.get("email") or ""
                    cc = v.get("cc") or []
                    if isinstance(cc, str):
                        cc_list = [e.strip() for e in re.split(r"[;,]", cc) if e.strip()]
                    elif isinstance(cc, (list, tuple)):
                        cc_list = [str(e).strip() for e in cc if str(e).strip()]
            elif isinstance(raw, list):
                for item in raw:
                    if not isinstance(item, dict): 
                        continue
                    name = (item.get("lender") or item.get("name") or "").strip().lower()
                    if not name or name != key: 
                        continue
                    to_email = item.get("to") or item.get("email") or ""
                    cc = item.get("cc") or []
                    if isinstance(cc, str):
                        cc_list = [e.strip() for e in re.split(r"[;,]", cc) if e.strip()]
                    elif isinstance(cc, (list, tuple)):
                        cc_list = [str(e).strip() for e in cc if str(e).strip()]
            cc_list = (cc_list or []) + (extra_cc or [])
            return (to_email or None), _dedupe_emails(cc_list)
    except Exception:
        traceback.print_exc()
    return None, _dedupe_emails(extra_cc or [])

def resolve_recipients_user_csv_first(lender_name: str, user_cc: list[str]):
    uid = _user_email()
    lname_key = (lender_name or "").strip().lower()

    to_email = ""
    cc_list: list[str] = []
    if uid:
        book, _ = _load_emails_book(uid)
        if lname_key in book:
            to_list = list(book[lname_key].get("to") or [])
            cc_from_csv = list(book[lname_key].get("cc") or [])

            if to_list:
                to_email = to_list[0]
                cc_list.extend(to_list[1:])
            cc_list.extend(cc_from_csv)

    cc_list.extend(user_cc or [])
    if uid:
        cc_list.append(uid)

    if not to_email:
        try:
            legacy_to, legacy_cc = resolve_recipients(lender_name, user_cc)
            to_email = to_email or (legacy_to or "")
            cc_list.extend(legacy_cc or [])
        except Exception:
            pass

    return (to_email or "").strip(), _dedupe_emails(cc_list)

# ------------------------------------------------------------------------------
# Connected sender (gmail/graph)
# ------------------------------------------------------------------------------
def _load_google_token_from_disk(pref_email: Optional[str] = None) -> tuple[Optional[str], Optional[dict]]:
    try:
        if GOOGLE_TOKEN_FILE.exists():
            data = json.loads(GOOGLE_TOKEN_FILE.read_text("utf-8"))
            if isinstance(data, dict) and data.get("email") and data.get("token"):
                if not pref_email or data.get("email") == pref_email:
                    return data.get("email"), data
    except Exception:
        pass
    try:
        if TOKENS_DIR.exists():
            candidates = []
            for p in TOKENS_DIR.glob("*.json"):
                try:
                    d = json.loads(p.read_text("utf-8"))
                except Exception:
                    continue
                if isinstance(d, dict) and d.get("email") and d.get("token"):
                    candidates.append((d.get("email"), d))
            if pref_email:
                for em, td in candidates:
                    if em == pref_email:
                        return em, td
            if len(candidates) == 1:
                return candidates[0]
    except Exception:
        pass
    return None, None

def _legacy_get_connected_sender(user_id: str) -> Tuple[Optional[str], Optional[dict], Optional[str]]:
    if not AUTH_STORE_PATH.exists():
        return None, None, None
    try:
        with AUTH_STORE_PATH.open("r", encoding="utf-8") as f:
            store = json.load(f) or {}
    except Exception:
        log.exception("Failed to read auth store")
        return None, None, None
    rec = store.get(user_id) or {}
    sender_email = rec.get("email")
    provider = rec.get("provider")
    token = rec.get("token")
    if sender_email and provider and token:
        return sender_email, token, provider
    return None, None, None

try:
    from auth_google import get_connected_sender as _GCS_mod
    _GCS = _GCS_mod
except Exception:
    _GCS = None

def safe_get_connected_sender():
    try:
        if _GCS is not None:
            sig = signature(_GCS)
            if len(sig.parameters) == 0:
                e, p, t = _GCS()
                if e and t:
                    return e, p, t
            else:
                uid = session.get("google_email") or request.headers.get("X-User-Email") or session.get("user_id") or session.get("uid")
                if uid:
                    res = _GCS(uid)
                    if isinstance(res, tuple) and len(res) == 3:
                        a, b, c = res
                        if isinstance(b, str) and b.lower() in ("gmail", "graph", "outlook"):
                            if a and c: return a, b, c
                        if isinstance(c, str) and c.lower() in ("gmail", "graph", "outlook"):
                            if a and b: return a, c, b
    except Exception:
        pass
    uid = session.get("google_email") or request.headers.get("X-User-Email") or session.get("user_id") or session.get("uid") or "demo-user"
    e, t, p = _legacy_get_connected_sender(uid)
    if e and t:
        return e, p or "gmail", t
    e2, t2 = _load_google_token_from_disk(session.get("google_email"))
    if e2 and t2:
        return e2, "gmail", t2
    return None, None, None

# ------------------------------------------------------------------------------
# Email sending helpers
# ------------------------------------------------------------------------------
def _build_mime(subject: str, html_body: str, sender_email: str, to_email: str, cc_list: List[str],
                attachments: List[Tuple[str, bytes]]) -> email.message.Message:
    msg = email.mime.multipart.MIMEMultipart()
    msg["To"] = to_email
    if cc_list:
        msg["Cc"] = ", ".join(cc_list)
    msg["From"] = sender_email
    msg["Subject"] = subject
    msg["Date"] = email.utils.formatdate(localtime=True)
    msg.attach(email.mime.text.MIMEText(html_body or "", "html", "utf-8"))
    for fname, data in attachments or []:
        ctype, enc = mimetypes.guess_type(fname)
        if ctype is None:
            ctype = "application/octet-stream"
        maintype, subtype = ctype.split("/", 1)
        part = email.mime.base.MIMEBase(maintype, subtype)
        part.set_payload(data)
        email.encoders.encode_base64(part)
        part.add_header("Content-Disposition", "attachment", filename=fname)
        msg.attach(part)
    return msg

def _flatten_google_token(td: dict) -> dict:
    td = dict(td or {})
    if isinstance(td.get("token"), dict) and any(k in td["token"] for k in ("client_id","token_uri","refresh_token","access_token","token")):
        core = dict(td["token"])
        for k in ("client_id","client_secret","token_uri","scopes","refresh_token","expiry","access_token","token"):
            if k in td and k not in core:
                core[k] = td[k]
        td = core
    if "token" not in td and "access_token" in td:
        td["token"] = td["access_token"]
    if isinstance(td.get("scopes"), str):
        td["scopes"] = [s for s in td["scopes"].split() if s]
    td.setdefault("token_uri", "https://oauth2.googleapis.com/token")
    return td

def _expiry_to_epoch(exp) -> float | None:
    if exp is None:
        return None
    if isinstance(exp, (int, float)):
        return float(exp)
    if isinstance(exp, datetime):
        if exp.tzinfo is None:
            return exp.replace(tzinfo=timezone.utc).timestamp()
        return exp.astimezone(timezone.utc).timestamp()
    if isinstance(exp, str):
        try:
            dt = datetime.fromisoformat(exp.replace("Z", "+00:00")) if exp.endswith("Z") else datetime.fromisoformat(exp)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).timestamp()
        except Exception:
            return None
    return None

def _google_refresh_access_token(td: dict) -> tuple[bool, dict | str]:
    rt = td.get("refresh_token")
    cid = td.get("client_id")
    csec = td.get("client_secret")
    token_uri = td.get("token_uri") or "https://oauth2.googleapis.com/token"
    if not (rt and cid and csec):
        return False, "missing_refresh_material"
    try:
        r = requests.post(
            token_uri,
            data={
                "grant_type": "refresh_token",
                "refresh_token": rt,
                "client_id": cid,
                "client_secret": csec,
            },
            timeout=15,
        )
    except Exception as e:
        return False, f"refresh_http_error:{e}"
    if r.status_code != 200:
        return False, f"refresh_http_{r.status_code}:{r.text[:200]}"
    js = r.json() or {}
    access_token = js.get("access_token")
    expires_in = js.get("expires_in")
    if not access_token:
        return False, f"refresh_no_access_token:{js}"
    td["token"] = access_token
    td["access_token"] = access_token
    if isinstance(expires_in, (int, float)):
        td["expiry"] = (_time.time() + float(expires_in) - 30)
    else:
        td["expiry"] = (_time.time() + 55 * 60)
    return True, td

def _ensure_google_access_token(token_dict: dict) -> tuple[bool, dict | str]:
    td = _flatten_google_token(token_dict)
    now = _time.time()
    exp_epoch = _expiry_to_epoch(td.get("expiry"))
    tok = td.get("token") or td.get("access_token")
    needs_refresh = False
    if not tok:
        needs_refresh = True
    elif exp_epoch is None:
        needs_refresh = bool(td.get("refresh_token"))
    else:
        needs_refresh = (exp_epoch - now) < 60
    if needs_refresh:
        ok, upd = _google_refresh_access_token(td)
        if not ok:
            return False, upd
        td = upd
    td["token"] = td.get("token") or td.get("access_token")
    return True, td

def gmail_send(token_dict: dict, subject: str, body_html: str,
               sender_email: str, to_email: str, cc_list: list,
               attachments: list) -> tuple[bool, str | None]:
    ok, td_or_err = _ensure_google_access_token(token_dict)
    if not ok:
        return False, f"gmail_error:{td_or_err}"
    td = td_or_err
    access_token = td.get("token") or td.get("access_token")
    if not access_token:
        return False, "gmail_error:no_access_token"

    msg = _build_mime(subject, body_html, sender_email, to_email, cc_list, attachments)
    raw = base64.urlsafe_b64encode(msg.as_bytes(policy=email.policy.SMTP)).decode("utf-8")

    try:
        r = requests.post(
            "https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
            json={"raw": raw},
            timeout=20,
        )
    except Exception as e:
        return False, f"gmail_error:http:{e}"

    if r.status_code not in (200, 202):
        return False, f"gmail_error:http_{r.status_code}:{r.text[:300]}"
    try:
        rid = r.json().get("id")
    except Exception:
        rid = None
    return True, rid

def graph_send(token_dict: dict, subject: str, body_html: str,
               sender_email: str, to_email: str, cc_list: List[str],
               attachments: List[Tuple[str, bytes]]) -> Tuple[bool, Optional[str]]:
    access_token = token_dict.get("access_token") or token_dict.get("token")
    if not access_token:
        return False, "no_graph_access_token"
    msg = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": body_html or ""},
            "toRecipients": [{"emailAddress": {"address": to_email}}] if to_email else [],
            "ccRecipients": [{"emailAddress": {"address": a}} for a in (cc_list or [])],
        },
        "saveToSentItems": True
    }
    atts = []
    for fname, data in attachments or []:
        atts.append({
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": fname,
            "contentBytes": base64.b64encode(data).decode("utf-8")
        })
    if atts:
        msg["message"]["attachments"] = atts
    try:
        r = requests.post(
            "https://graph.microsoft.com/v1.0/me/sendMail",
            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
            json=msg,
            timeout=20
        )
        if r.status_code in (202, 200):
            return True, None
        return False, f"graph_http_{r.status_code}:{r.text[:200]}"
    except Exception as e:
        log.exception("Graph send error")
        return False, f"graph_error:{e}"

# ------------------------------------------------------------------------------
# Deals & deliveries persistence
# ------------------------------------------------------------------------------
def record_deal(
    user_id: str,
    sender_email: str,
    subject: str,
    body: str,
    mode: Optional[str],
    application_json: Optional[dict] = None,
    statements_json: Optional[dict] = None,
    attachments_json: Optional[dict] = None,
) -> int:
    payload = {
        "user_id": user_id,
        "sender_email": sender_email,
        "subject": subject,
        "body": body,
        "mode": (mode or ""),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "application_json": application_json or {},
        "statements_json": statements_json or {},
        "attachments_json": attachments_json or {},
    }

    res = sb.table("deals").insert(payload, returning="representation").execute()
    rows = res.data or []
    if not rows:
        fetch = (
            sb.table("deals")
              .select("id")
              .eq("user_id", user_id)
              .eq("created_at", payload["created_at"])
              .limit(1)
              .execute()
        )
        rows = fetch.data or []

    if not rows:
        raise RuntimeError("Failed to insert deal (no row returned)")

    return int(rows[0]["id"])

def record_delivery(
    deal_id: int,
    lender_name: str,
    to_email: str,
    cc_list: List[str],
    provider: str,
    provider_msg_id: Optional[str],
    status: str,
    login_email: str,
    tracking_id: Optional[Union[str, UUID]] = None,
) -> int:
    tid = str(tracking_id or uuid4())  
    payload = {
        "deal_id": deal_id,
        "lender_name": lender_name,
        "to_email": to_email or "",
        "cc_csv": ",".join(cc_list or []),
        "provider": provider,
        "provider_msg_id": provider_msg_id or "",
        "status": status,
        "sender_email": login_email,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "tracking_id": tid, 
    }

    res = sb.table("deliveries").insert(payload, returning="representation").execute()
    rows = res.data or []
    if not rows:
        fetch = (
            sb.table("deliveries")
              .select("id")
              .eq("deal_id", deal_id)
              .eq("tracking_id", tid)
              .eq("lender_name", lender_name)
              .eq("created_at", payload["created_at"])
              .limit(1)
              .execute()
        )
        rows = fetch.data or []

    if not rows:
        raise RuntimeError("Failed to insert delivery (no row returned)")

    return rows[0]["id"]

# ------------------------------------------------------------------------------
# Wrapper 
# ------------------------------------------------------------------------------

# --- ONE: make _wrap_upload pass watermark/footer ---
def _wrap_upload(saved: dict, *, footer_text=None, watermark_text=None, prefix: str = None) -> dict:
    import os
    import re
    import wrappers

    fn = getattr(wrappers, "wrap_pdf_with_logo", None)
    if not fn:
        raise RuntimeError("wrap function missing in wrappers.py")

    # skip if already wrapped
    name = saved.get("filename") or os.path.basename(saved["path"])
    if re.search(r"(?:\.wrapped\.pdf|-\s*wrapped\.pdf)$", name, re.I):
        return {**saved, "wrap_ok": True, "wrap_msg": "already wrapped"}

    out_path = fn(
        saved["path"],
        logo_path=LOGO_PATH,
        output_dir=str(UPLOAD_DIR),  # ensure we can serve it
        footer_text=footer_text or "Submitted via Pathway Catalyst",
        watermark_text=watermark_text or "SENT VIA PATHWAY CATALYST",
    )
    log.info("wrap: ok path=%s -> %s", saved["path"], out_path)
    return {"path": out_path, "filename": os.path.basename(out_path), "wrap_ok": True, "wrap_msg": "ok"}




# ------------------------------------------------------------------------------
# Statements DB fetch (fallback)
# ------------------------------------------------------------------------------
def _fetch_statements_from_db(deal_id: Optional[int] = None,
                              application_id: Optional[int] = None,
                              limit: int = 12) -> List[Tuple[str, bytes]]:
    """
    Tries to fetch statement PDFs from Supabase Storage given a deal/application id.
    Expect a table 'deal_documents(deal_id, application_id, kind, filename, storage_path)'.
    Returns [(filename, bytes), ...] or [] if nothing found.
    """
    out: List[Tuple[str, bytes]] = []
    try:
        q = sb.table("deal_documents").select("filename,storage_path,kind").limit(limit)
        if deal_id:
            q = q.eq("deal_id", int(deal_id))
        if application_id:
            q = q.eq("application_id", int(application_id))
        rows = (q.execute().data) or []
        rows = [r for r in rows if (r.get("kind") or "").lower() == "statement" and r.get("storage_path")]
        if not rows:
            return out
        bucket = os.environ.get("STATEMENTS_BUCKET", "statements")
        st = sb.storage.from_(bucket)
        for r in rows:
            sp = r.get("storage_path")
            try:
                raw = st.download(sp)  # bytes
                name = r.get("filename") or Path(sp).name
                out.append((name, raw))
            except Exception as e:
                log.warning("storage download failed: %s -> %s", sp, e)
    except Exception as e:
        log.warning("fetch statements from db failed: %s", e)
    return out

# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------
@bp.post("/extract-application")

def extract_application_only():
    try:
        app_file = request.files.get("application")
        if not app_file:
            return jsonify({"error": "Missing application PDF"}), 400
        saved = _save_upload(app_file, prefix="application")
        wrap_saved = _wrap_upload(saved, prefix="application")
        application = _extract_application_fields(Path(saved["path"]))
        application["_wrapped_filename"] = wrap_saved["filename"]
        #application["_attachment"] = saved
        return jsonify({"application": application})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@bp.post("/statements-and-match")
def statements_and_match():
    try:
        application_json = request.form.get("application_json")
        if not application_json:
            return jsonify({"error": "Missing application_json"}), 400
        try:
            application = json.loads(application_json) or {}
            if not isinstance(application, dict):
                return jsonify({"error": "application_json must be a JSON object"}), 400
        except Exception:
            return jsonify({"error": "application_json is not valid JSON"}), 400

        application = _inject_length_months(application)
        state = (request.form.get("state") or application.get("state") or "").strip().upper()
        if len(state) != 2:
            return jsonify({"error": "State must be 2 letters (e.g., NY, CA)"}), 400

        stmt_files = request.files.getlist("statements") or []
        min_files = 4 if state in {"NY", "CA"} else 3

        # Prefer uploaded files; otherwise fetch from DB if deal_id/application_id provided
        files_for_summary: List[Tuple[str, bytes]] = []
        saved_files: List[dict] = []

        if len(stmt_files) < min_files:
            deal_id = request.form.get("deal_id")
            application_id = request.form.get("application_id")
            fetched = _fetch_statements_from_db(
                deal_id=int(deal_id) if str(deal_id or "").isdigit() else None,
                application_id=int(application_id) if str(application_id or "").isdigit() else None
            )
            if fetched:
                for fname, raw in fetched:
                    # Fetched from storage as bytes; no local path to wrap.
                    files_for_summary.append((fname, raw))
                    saved_files.append({"path": f"storage://{fname}", "filename": fname})
            else:
                return jsonify({"error": f"Need at least {min_files} statements (upload or present in DB) for state {state}"}), 400
        else:
            for f in stmt_files:
                # 1) save raw
                saved = _save_upload(f, prefix="stmt")
                # 2) wrap raw -> wrapped using _wrap_upload
                wrap_saved = _wrap_upload(saved, prefix="stmt")

                saved_files.append(wrap_saved)
                with open(wrap_saved["path"], "rb") as fh:
                    files_for_summary.append((wrap_saved["filename"], fh.read()))

        prev_json = request.form.get("existing_statements_json")
        prev = None
        if prev_json:
            try:
                prev = json.loads(prev_json) or {}
            except Exception:
                prev = None

        statements_new = _build_statements_payload(files_for_summary, state_for_rule=state)
        statements_new["_wrapped_filenames"] = [f["filename"] for f in saved_files]

        #statements_new["_saved_files"] = saved_files

        if prev and isinstance(prev, dict):
            combined_per = (prev.get("per_statement") or []) + (statements_new.get("per_statement") or [])
            combined_files = (prev.get("_saved_files") or []) + saved_files
            aggregates = _aggregate_statements_and_revenue(combined_per, state_for_rule=state)
            statements_payload = {"per_statement": combined_per, **aggregates, "_saved_files": combined_files}
        else:
            statements_payload = statements_new

        lenders = _match_lenders(application, statements_payload)
        lenders = _append_default_lenders(lenders)
        return jsonify({"statements": statements_payload, "lenders": lenders})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@bp.post("/extract-and-match")
def extract_and_match():
    try:
        app_file = request.files.get("application")
        if not app_file:
            return jsonify({"error": "Missing application PDF"}), 400
        state = (request.form.get("state") or "").strip().upper()
        if len(state) != 2:
            return jsonify({"error": "State must be 2 letters (e.g., NY, CA)"}), 400

        stmt_files = request.files.getlist("statements")
        min_files = 4 if state in {"NY", "CA"} else 3
        if not stmt_files or len(stmt_files) < min_files:
            return jsonify({"error": f"Need at least {min_files} statement PDFs for state {state}"}), 400

        saved_app = _save_upload(app_file, prefix="application")
        application = _extract_application_fields(Path(saved_app["path"]))
        application["_attachment"] = saved_app

        saved_files: List[dict] = []
        files_for_summary: List[Tuple[str, bytes]] = []
        for f in stmt_files:
            saved = _save_upload(f, prefix="stmt")
            saved_files.append(saved)
            with open(saved["path"], "rb") as fh:
                files_for_summary.append((saved["filename"], fh.read()))

        statements_payload = _build_statements_payload(files_for_summary, state_for_rule=state or application.get("state"))
        statements_payload["_saved_files"] = saved_files

        lenders = _match_lenders(application, statements_payload)
        lenders = _append_default_lenders(lenders)
        return jsonify({"application": application, "statements": statements_payload, "lenders": lenders})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# Attachments parsing
def _parse_attachments_from_json(data: dict) -> List[Tuple[str, bytes]]:
    out: List[Tuple[str, bytes]] = []
    try:
        items = (data or {}).get("attachments") or []
        for i, a in enumerate(items):
            name = (a.get("name") or a.get("filename") or f"file{i+1}").strip() or f"file{i+1}"
            b64  = a.get("data") or a.get("base64") or a.get("content") or ""
            if not b64:
                continue
            if b64.startswith("data:"):
                try:
                    b64 = b64.split(",", 1)[1]
                except Exception:
                    pass
            try:
                raw = base64.b64decode(b64)
                out.append((name, raw))
            except Exception:
                continue
    except Exception:
        pass
    return out

def _parse_per_lender_attachments_from_json(data: dict) -> Dict[str, List[Tuple[str, bytes]]]:
    out: Dict[str, List[Tuple[str, bytes]]] = {}
    src = (data or {}).get("per_lender_attachments")
    if not src:
        return out

    def add_for(lender: str, items: list):
        files: List[Tuple[str, bytes]] = []
        for i, a in enumerate(items or []):
            name = (a.get("name") or a.get("filename") or f"file{i+1}").strip() or f"file{i+1}"
            b64  = a.get("data") or a.get("base64") or a.get("content") or ""
            if not b64:
                continue
            if b64.startswith("data:"):
                try:
                    b64 = b64.split(",", 1)[1]
                except Exception:
                    pass
            try:
                raw = base64.b64decode(b64)
                files.append((name, raw))
            except Exception:
                continue
        out[lender] = files
        out[lender.lower()] = files

    if isinstance(src, dict):
        for lender, items in src.items():
            if not lender:
                continue
            add_for(str(lender), list(items or []))
    elif isinstance(src, list):
        for entry in src:
            lender = (entry or {}).get("lender")
            items  = (entry or {}).get("attachments") or []
            if lender:
                add_for(str(lender), list(items))
    return out

SUBJECT_PREFIX = "New Submission - Harvest Lending/Pathway Catalyst"

def _default_subject(biz: Optional[str], deal_id: int, user_subject: Optional[str]) -> str:
    biz_name = (biz or "").strip() or "Unknown Business"
    template = f"{SUBJECT_PREFIX} - {biz_name} - #{deal_id}"
    s = (user_subject or "").strip()
    if not s:
        return template
    return s.replace("#DealID", f"#{deal_id}")

# Emails book endpoints
LENDER_ALIASES = {"lender","lender name","name","business","company","funder","provider"}
TO_ALIASES     = {"to","email","primary","primary_email"}

def _preview_emails_csv_text(text: str):
    text = text.replace("\r\n", "\n")
    lines = [ln for ln in text.split("\n") if ln.strip()]
    if lines and lines[0].lower().startswith("sep="):
        lines = lines[1:]
    if not lines:
        return 0, []
    reader = csv.DictReader(lines)
    if not reader.fieldnames:
        return 0, []
    def col_val(row, aliases):
        r = { (k or "").strip().lower(): (v or "").strip() for k,v in (row or {}).items() }
        for a in aliases:
            if a in r and r[a]:
                return r[a]
        return ""
    count = 0
    sample = []
    for row in reader:
        name = col_val(row, LENDER_ALIASES)
        to_s = col_val(row, TO_ALIASES)
        to_list = _split_emails(to_s)
        if name and to_list:
            count += 1
            if len(sample) < 6:
                sample.append(name)
    return count, sample

@bp.get("/emails-book/status")
def emails_book_status():
    email_addr = (session.get("user_email") or session.get("google_email") or "").strip().lower()
    if not email_addr:
        return jsonify({"ok": True, "count": 0, "source": "csv"}), 200

    path = EMAILS_DIR / f"{_safe_email(email_addr)}.csv"
    if not path.exists():
        return jsonify({"ok": True, "count": 0, "source": "csv"}), 200

    count = 0
    try:
        with path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh)
            _ = next(reader, None)
            for row in reader:
                if any((c or "").strip() for c in row):
                    count += 1
    except Exception:
        return jsonify({"ok": True, "count": 0, "source": "csv"}), 200

    mtime = int(path.stat().st_mtime)
    resp = jsonify({"ok": True, "count": count, "filename": path.name, "updated_at": mtime, "source": "csv"})
    resp.headers["Cache-Control"] = "no-store"
    return resp

@bp.post("/emails-book/upload")
def emails_book_upload():
    user = session.get("google_email") or session.get("user_email") or "anonymous"
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "missing_file"}), 400

    raw = f.read()
    text = raw.decode("utf-8-sig", errors="replace")
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        return jsonify({"error": "empty_csv"}), 400

    path = _user_emails_csv_path(user)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")

    cnt, sample = _preview_emails_csv_text(text)
    return jsonify({"ok": True, "filename": path.name, "bytes": len(text.encode("utf-8")), "count": cnt, "sample": sample})

# ------------------------------------------------------------------------------
# Send emails (now with server-side wrapping fallback)
# ------------------------------------------------------------------------------
@bp.post("/send")
def send_emails():
    """
    json: {
      "selected_lenders":[...],
      "subject":"...",
      "message":"...",
      "cc":[...],
      "mode":"MCA|CCS|REV|SBA|RE|EQP",
      "application": {...},
      "statements": {...},
      "attachments": [ {name, data(base64)}, ... ],
      "per_lender_attachments": { "LenderA":[...], ... } | [...],
      "parent_deal_id": 123,    # optional
      "exclude": ["LenderA"]    # optional
    }
    """
    try:
        data = request.get_json(force=True) or {}
        selected = data.get("selected_lenders") or []
        subject  = data.get("subject") or ""
        body     = data.get("message") or ""
        user_cc  = data.get("cc") or []
        mode     = (data.get("mode") or "").upper() or "MCA"

        application_obj = data.get("application") or {}
        statements_obj  = data.get("statements") or {}

        attachments_global = _parse_attachments_from_json(data)
        per_map = _parse_per_lender_attachments_from_json(data)
        need_wrap_fallback = not per_map and bool(attachments_global)

        if isinstance(user_cc, str):
            user_cc = [x.strip() for x in user_cc.split(",") if x.strip()]

        if not selected:
            return jsonify({"error": "No lenders selected"}), 400

        sender_email, provider, token = safe_get_connected_sender()
        if not sender_email or not token:
            return jsonify({"error": "No connected mailbox. Connect Gmail/Outlook first."}), 403

        deal_id = record_deal(
            session.get("google_email") or "demo-user",
            sender_email, subject, body, mode,
            application_json=application_obj,
            statements_json=statements_obj
        )

        final_subject = _default_subject(application_obj.get("business_name"), deal_id, subject)
        if final_subject != subject:
            try:
                sb.table("deals").update({"subject": final_subject}).eq("id", deal_id).execute()
            except Exception:
                pass

        exclude = set([(s or "").strip().lower() for s in (data.get("exclude") or []) if s])
        parent_deal_id = data.get("parent_deal_id")
        if parent_deal_id and not exclude:
            try:
                resp = sb.table("deliveries").select("lender_name").eq("deal_id", int(parent_deal_id)).execute()
                names = [(r.get("lender_name") or "").strip().lower() for r in (resp.data or [])]
                exclude = set([n for n in names if n])
                cur = sb.table("deals").select("resend_count").eq("id", int(parent_deal_id)).limit(1).execute()
                cur_val = int((cur.data or [{}])[0].get("resend_count") or 0)
                sb.table("deals").update({"resend_count": cur_val + 1}).eq("id", int(parent_deal_id)).execute()
            except Exception:
                pass

        deliveries = []
        for lender in selected:
            lname = (lender or "").strip()
            lname_key = lname.lower()

            if lname_key in exclude:
                deliveries.append({"lender": lname, "from": sender_email, "status": "skipped", "reason": "already sent"})
                record_delivery(deal_id, lname, "", user_cc, provider or "", None, "skipped", sender_email)
                continue

            attachments_for_this = per_map.get(lname) or per_map.get(lname_key)

            # If UI didn’t send per-lender files, build them now and wrap server-side
            if not attachments_for_this and attachments_global:
                attachments_for_this = []
                for (fname, raw) in attachments_global:
                    if _is_wrapped_filename(fname):
                        attachments_for_this.append((fname, raw))
                        continue
                    okw, out_or_err, final_name = _wrap_via_api(
                        lender=lname,
                        raw_pdf=raw,
                        filename=fname,
                        deal_id=deal_id,
                        recipient_email=sender_email,
                    )
                    if okw:
                        attachments_for_this.append((final_name, out_or_err))
                    else:
                        log.warning("wrap failed for %s -> %s: %s", fname, lname, out_or_err)
                        attachments_for_this.append((fname, raw))

            to_email, cc_list = resolve_recipients_user_csv_first(lname, user_cc)
            if not to_email:
                deliveries.append({"lender": lname, "from": sender_email, "status": "skipped",
                                   "reason": "No recipient email in emails-book/JSON"})
                record_delivery(deal_id, lname, "", user_cc, provider or "", None, "skipped", sender_email)
                continue

            ok, provider_id = False, None
            if (provider or "").lower() == "gmail":
                ok, provider_id = gmail_send(token, final_subject, body, sender_email, to_email, cc_list, attachments=attachments_for_this)
            elif (provider or "").lower() in ("outlook", "graph"):
                ok, provider_id = graph_send(token, final_subject, body, sender_email, to_email, cc_list, attachments=attachments_for_this)
            else:
                deliveries.append({"lender": lname, "from": sender_email, "to": to_email, "cc": cc_list,
                                   "status": "error", "reason": f"Unsupported provider {provider}"})
                record_delivery(deal_id, lname, to_email, cc_list, provider or "", None, "error", sender_email)
                continue

            status = "sent" if ok else "error"
            deliveries.append({
                "lender": lname, "from": sender_email, "to": to_email, "cc": cc_list,
                "status": status, "provider": (provider or ""), "provider_id": provider_id
            })
            record_delivery(deal_id, lname, to_email, cc_list, provider or "", provider_id, status, sender_email)

        return jsonify({"ok": True, "from": sender_email, "deal_id": deal_id, "subject": final_subject, "deliveries": deliveries})
    except Exception as e:
        log.exception("send_emails failed: %s", e)
        return jsonify({"error": str(e)}), 500

# ------------------------------------------------------------------------------
# Deals list / deliveries / single deal
# ------------------------------------------------------------------------------
@bp.get("/deals")
def list_deals():
    try:
        sess_email = session.get("google_email") or request.headers.get("X-User-Email")
        limit = max(1, int(request.args.get("limit", 50)))
        offset = max(0, int(request.args.get("offset", 0)))
        end = offset + limit - 1

        q = (sb.table("deals")
              .select("id,user_id,sender_email,subject,mode,created_at,application_json,statements_json,resend_count,deliveries(status)")
              .order("created_at", desc=True)
              .range(offset, end))
        if sess_email:
            q = q.or_(f"user_id.eq.{sess_email},sender_email.eq.{sess_email}")

        rows = (q.execute().data) or []

        deals = []
        for r in rows:
            app_json = r.get("application_json") or {}
            deliveries = r.get("deliveries") or []
            statuses = [(x or {}).get("status", "").lower() for x in deliveries]
            deals.append({
                "id": r.get("id"),
                "user_id": r.get("user_id"),
                "sender_email": r.get("sender_email"),
                "subject": r.get("subject"),
                "business_name": (app_json or {}).get("business_name"),
                "mode": r.get("mode"),
                "created_at": r.get("created_at"),
                "sent_count": sum(1 for s in statuses if s in ("sent", "delivered")),
                "error_count": sum(1 for s in statuses if s in ("error", "failed")),
                "skipped_count": statuses.count("skipped"),
                "total_count": len(statuses),
                "has_app": 1 if app_json else 0,
                "has_stmts": 1 if (r.get("statements_json") or {}) else 0,
                "resend_count": int(r.get("resend_count") or 0),
            })
        return jsonify({"deals": deals})
    except Exception as e:
        log.exception("list_deals failed: %s", e)
        return jsonify({"error": "Failed to load deals"}), 500

@bp.get("/deals/<int:deal_id>/deliveries")
def list_deliveries(deal_id: int):
    try:
        resp = (sb.table("deliveries")
                  .select("id,lender_name,to_email,cc_csv,provider,provider_msg_id,status,created_at")
                  .eq("deal_id", int(deal_id))
                  .order("created_at", desc=False)
                  .execute())
        rows = resp.data or []
        deliveries = []
        for r in rows:
            deliveries.append({
                "id": r.get("id"),
                "lender": r.get("lender_name"),
                "to": r.get("to_email"),
                "cc": [s for s in (r.get("cc_csv") or "").split(",") if s],
                "provider": r.get("provider"),
                "provider_id": r.get("provider_msg_id"),
                "status": r.get("status"),
                "created_at": r.get("created_at"),
            })
        return jsonify({"deliveries": deliveries})
    except Exception as e:
        log.exception("list_deliveries failed: %s", e)
        return jsonify({"error": "Failed to load deliveries"}), 500

@bp.get("/deal/<int:deal_id>")
def get_deal(deal_id: int):
    try:
        resp = (sb.table("deals")
                .select("id,user_id,sender_email,subject,body,mode,created_at,application_json,statements_json,attachments_json,"
                        "deliveries(id,lender_name,to_email,cc_csv,provider,provider_msg_id,status,created_at)")
                .eq("id", int(deal_id))
                .limit(1)
                .execute())
        rows = resp.data or []
        if not rows:
            return jsonify({"error": "not found"}), 404
        row = rows[0]
        deal = {
            "id": row.get("id"),
            "user_id": row.get("user_id"),
            "sender_email": row.get("sender_email"),
            "subject": row.get("subject"),
            "body": row.get("body"),
            "mode": row.get("mode"),
            "created_at": row.get("created_at"),
            "application": row.get("application_json") or {},
            "statements": row.get("statements_json") or {},
            "attachments": row.get("attachments_json") or {},
            "deliveries": []
        }
        for d in (row.get("deliveries") or []):
            deal["deliveries"].append({
                "lender": d.get("lender_name"),
                "to": d.get("to_email"),
                "cc": [s for s in (d.get("cc_csv") or "").split(",") if s],
                "provider": d.get("provider"),
                "provider_id": d.get("provider_msg_id"),
                "status": d.get("status"),
                "created_at": d.get("created_at"),
            })
        return jsonify({"deal": deal})
    except Exception as e:
        log.exception("get_deal failed: %s", e)
        return jsonify({"error": "Failed to load deal"}), 500

# ------------------------------------------------------------------------------
# Rematch & Leads
# ------------------------------------------------------------------------------
@bp.post("/rematch")
def rematch():
    try:
        data = request.get_json(force=True) or {}
        mode = str(data.get("mode") or "MCA").upper()
        application = _inject_length_months(data.get("application") or {})
        statements  = data.get("statements") or {}
        if mode != "MCA":
            return jsonify({"lenders": []})
        lenders = _match_lenders(application, statements) or []
        lenders = _append_default_lenders(lenders)
        return jsonify({"lenders": lenders})
    except Exception as e:
        log.exception("rematch failed: %s", e)
        return jsonify({"error": str(e)}), 500

@bp.get("/leads")
def api_leads():
    sbu = get_sb()
    try:
        limit = int(request.args.get("limit", 100))
    except Exception:
        limit = 100
    status = request.args.get("status")

    q = (sbu.table("applications")
           .select("id,business_legal_name,industry,loan_amount,owners,created_at")
           .order("created_at", desc=True)
           .limit(limit))
    if status:
        q = q.eq("status", status)

    resp = q.execute()
    if getattr(resp, "error", None):
        return jsonify({"error": str(resp.error)}), 500

    return jsonify({"leads": resp.data or []})

# ------------------------------------------------------------------------------
# New: Hydrator + submit/resubmit for dashboard/modes
# ------------------------------------------------------------------------------
@bp.get("/deal/<int:deal_id>")
def api_underwrite_deal(deal_id: int):
    """
    Slim hydrator for the UI: application JSON + statement docs with (optional) signed URLs.
    Uses 'deal_documents' if present; otherwise returns what's stored on the deal row.
    """
    try:
        base = (sb.table("deals")
                  .select("id, mode, application_json, statements_json")
                  .eq("id", int(deal_id)).limit(1).execute().data) or []
        if not base:
            return jsonify({"error":"not found"}), 404
        row = base[0]
        app_json = row.get("application_json") or {}
        # Try documents
        docs = []
        try:
            bucket = os.environ.get("STATEMENTS_BUCKET", "statements")
            st = sb.storage.from_(bucket)
            q = (sb.table("deal_documents")
                   .select("id,kind,month,filename,storage_path")
                   .eq("deal_id", int(deal_id))
                   .order("month", desc=False))
            rs = (q.execute().data) or []
            for m in rs:
                if (m.get("kind") or "").lower() != "statement":
                    continue
                path = m.get("storage_path")
                url = None
                if path:
                    try:
                        url = st.create_signed_url(path, 3600)["signedURL"]
                    except Exception:
                        url = None
                docs.append({
                    "id": m.get("id"),
                    "kind": m.get("kind"),
                    "month": m.get("month"),
                    "filename": m.get("filename") or (Path(path).name if path else None),
                    "url": url
                })
        except Exception:
            pass
        if not docs:
            return jsonify({"id": deal_id, "mode": (row.get("mode") or "MCA"), "application": app_json, "statements": []})
        return jsonify({"id": deal_id, "mode": (row.get("mode") or "MCA"), "application": app_json, "statements": docs})
    except Exception as e:
        log.exception("api_underwrite_deal failed: %s", e)
        return jsonify({"error":"internal_error"}), 500

@bp.post("/submit")
def api_submit():
    """
    Records/updates a deal row and returns tracker_id so the dashboard can show it.
    Body: { deal_id?, mode, application, lenders? }
    """
    try:
        j = request.get_json(force=True) or {}
        deal_id = j.get("deal_id")
        mode = (j.get("mode") or "MCA").upper()
        app_json = j.get("application") or {}
        lenders = j.get("lenders") or []

        if not deal_id:
            ins = sb.table("deals").insert({
                "user_id": (session.get("google_email") or "demo-user"),
                "sender_email": session.get("google_email") or "",
                "mode": mode,
                "application_json": app_json,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }, returning="representation").execute().data
            deal_id = ins[0]["id"]
        else:
            sb.table("deals").update({
                "mode": mode,
                "application_json": app_json,
            }).eq("id", int(deal_id)).execute()

        tracker_id = f"trk_{int(_time.time())}"
        sb.table("submissions").insert({
            "deal_id": deal_id,
            "mode": mode,
            "lenders": lenders,
            "tracker_id": tracker_id,
        }).execute()

        return jsonify({"deal_id": deal_id, "tracker_id": tracker_id})
    except Exception as e:
        log.exception("api_submit failed: %s", e)
        return jsonify({"error": "submit_failed"}), 500

@bp.post("/resubmit")
def api_resubmit():
    """
    Records a resubmission intent. UI can then prompt for updated docs and call /send.
    Body: { deal_id, mode }
    """
    try:
        j = request.get_json(force=True) or {}
        deal_id = j.get("deal_id")
        mode = (j.get("mode") or "MCA").upper()
        if not deal_id:
            return jsonify({"error": "deal_id_required"}), 400
        sb.table("submissions").insert({
            "deal_id": int(deal_id),
            "mode": mode,
            "lenders": [],
            "tracker_id": f"res_{int(_time.time())}",
        }).execute()
        return jsonify({"ok": True})
    except Exception as e:
        log.exception("api_resubmit failed: %s", e)
        return jsonify({"error":"resubmit_failed"}), 500


@bp.get("/uploads/<path:fname>")
def serve_upload(fname):
    p = UPLOAD_DIR / secure_filename(fname)
    if not p.exists():
        abort(404)
    return send_file(str(p), mimetype="application/pdf")

def _upload_path_from_filename(fname: str) -> str:
    return str(UPLOAD_DIR / secure_filename(fname))

"""
# --- TWO: /wrap endpoint should accept and forward the texts ---
@bp.post("/wrap")
def api_wrap():
    
    from flask import send_file, jsonify, request
    import re

    f = request.files.get("file")
    if not f:
        return jsonify({"error": "missing file"}), 400

    saved = _save_upload(f, prefix="wrap")
    ctx = {
        "lender": (request.form.get("lender") or "").strip(),
        "deal": (request.form.get("deal_id") or "").strip(),
        "email": (request.form.get("recipient_email") or session.get("user_email") or "").strip(),
        "user": (session.get("user_name") or session.get("user_email") or "").strip(),
        "tracking": uuid.uuid4().hex[:8],
        }

    # Always have defaults so watermark/footer appear even if client didn't pass them
    wm = (request.form.get("force_watermark_text")
          or request.form.get("watermark_text")
          or "SENT VIA PATHWAY CATALYST").strip()
    ft = (request.form.get("force_footer_template")
          or request.form.get("footer_text")
          or "Submitted via Pathway Catalyst").strip()
              # --- TRY: use production wrapper when we have enough context ---

    tid = session.get("_tracking_id")
    if not tid:
      tid = uuid.uuid4().hex[:8]
      session["_tracking_id"] = tid
    try:
        use_prod = bool((ctx["deal"] or "").strip() and (ctx["lender"] or "").strip())
        if use_prod:
            import os, wrappers
            _deal = int(ctx["deal"]) if str(ctx["deal"]).isdigit() else ctx["deal"]
            _user_id = session.get("google_email") or session.get("user_id") or "demo-user"
            info = wrappers.issue_wrapper_user_branded(
                user_id=_user_id,
                deal_id=_deal,
                original_pdf_path=saved["path"],
                funder_name=ctx["lender"],
                recipient_email=ctx["email"],
                storage_dir=str(UPLOAD_DIR),
                supabase_url=SUPABASE_URL,
                supabase_service_key=SUPABASE_SERVICE_ROLE,

            # pass your texts as-is (wrapper uses {funder}/{recipient}/{deal}/{fp})
                force_watermark_text=wm or None,
                force_footer_template=ft or None,

            # ensure in-transit docs reuse the same tracker via {fp}
                force_tracking_id=tid,
            )

            out_path = info["wrapper_path"]
            out_name = os.path.basename(out_path)
            return send_file(out_path, mimetype="application/pdf",
                         as_attachment=False, download_name=out_name, max_age=0)
    except Exception as e:
        log.exception("issue_wrapper_user_branded failed; falling back to _wrap_upload: %s", e)

        
    try:
        use_prod = bool((ctx["deal"] or "").strip() and (ctx["lender"] or "").strip())
        if use_prod and not already_wrapped:
            import os
            import wrappers
            # Normalize deal id (int if possible; otherwise pass string)
            _deal = int(ctx["deal"]) if str(ctx["deal"]).isdigit() else ctx["deal"]
            _user_id = session.get("google_email") or session.get("user_id") or "demo-user"

            info = wrappers.issue_wrapper_user_branded(
                user_id=_user_id,
                deal_id=_deal,
                original_pdf_path=saved["path"],
                funder_name=ctx["lender"],
                recipient_email=ctx["email"],
                storage_dir=str(UPLOAD_DIR),
                supabase_url=SUPABASE_URL,
                supabase_service_key=SUPABASE_SERVICE_ROLE,
                # force the exact texts you already built (logo is resolved internally)
                force_watermark_text=wm,
                force_footer_template=ft,
                # Optional: explicitly force a logo file you control
                # force_logo_path=LOGO_PATH,
            )

            out_path = info["wrapper_path"]
            out_name = os.path.basename(out_path)

            return send_file(
                out_path,
                mimetype="application/pdf",
                as_attachment=False,
                download_name=out_name,
                max_age=0,
            )
    except Exception as e:
        log.exception("issue_wrapper_user_branded failed; falling back to _wrap_upload: %s", e)

        


    # Skip only if the UPLOADED file is *already* a wrapped PDF; otherwise wrap now
    name_up = (f.filename or "").strip()
    name_saved = (saved.get("filename") or "").strip()
    already_wrapped = bool(
        re.search(r"(?:\.wrapped\.pdf|-\s*wrapped\.pdf)$", name_up, flags=re.I) or
        re.search(r"(?:\.wrapped\.pdf|-\s*wrapped\.pdf)$", name_saved, flags=re.I)
    )


    name_up = (f.filename or "").strip()
    name_saved = (saved.get("filename") or "").strip()
    already_wrapped = bool(
        re.search(r"(?:\.wrapped\.pdf|-\s*wrapped\.pdf)$", name_up, flags=re.I) or
        re.search(r"(?:\.wrapped\.pdf|-\s*wrapped\.pdf)$", name_saved, flags=re.I)  
    )

    if already_wrapped:
        wrapped = saved  # trust client-provided wrapped PDF
    else:
        wrapped = _wrap_upload(saved, footer_text=ft, watermark_text=wm)

    return send_file(
        wrapped["path"],                       # serve the exact file we produced
        mimetype="application/pdf",
        as_attachment=False,
        download_name=wrapped["filename"],
        max_age=0,
    )

"""
"""
@bp.post("/wrap")
def api_wrap():
    
    from flask import send_file, jsonify, request, session
    import os, re, uuid

    f = request.files.get("file")
    if not f:
        return jsonify({"error": "missing file"}), 400

    # Save upload
    saved = _save_upload(f, prefix="wrap")

    # Context (in-transit)
    ctx = {
        "lender": (request.form.get("lender") or "").strip(),
        "deal":   (request.form.get("deal_id") or "").strip(),
        "email":  (request.form.get("recipient_email")
                   or session.get("user_email")
                   or session.get("google_email")
                   or "").strip(),
        "user":   (session.get("user_name")
                   or session.get("user_email")
                   or session.get("google_email")
                   or "").strip(),
    }

    # Stable per-session tracking id
    tid = session.get("_tracking_id")
    if not tid:
        tid = uuid.uuid4().hex[:8]
        session["_tracking_id"] = tid

    # Texts (keep your inputs; production wrapper uses {funder}/{recipient}/{deal}/{fp})
    wm = (request.form.get("force_watermark_text")
          or request.form.get("watermark_text")
          or "SENT VIA PATHWAY CATALYST • {fp}").strip()
    ft = (request.form.get("force_footer_template")
          or request.form.get("footer_text")
          or "Submitted to {funder} by {recipient} • Track {fp}").strip()

    # Compute already_wrapped first (needed below)
    name_up = (f.filename or "").strip()
    name_saved = (saved.get("filename") or "").strip()
    already_wrapped = bool(
        re.search(r"(?:\.wrapped\.pdf|-\s*wrapped\.pdf)$", name_up, flags=re.I) or
        re.search(r"(?:\.wrapped\.pdf|-\s*wrapped\.pdf)$", name_saved, flags=re.I)
    )

    def _normalize_tokens(t: str) -> str:
        if not t: return ""
        import re
        t = t.replace("{{", "{").replace("}}", "}")  # allow {{token}}
        t = re.sub(r"{\s*deal_id\s*}", "{deal}", t)        # alias -> canonical
        t = re.sub(r"{\s*tracking_id\s*}", "{fp}", t)
        t = re.sub(r"{\s*tracking\s*}", "{fp}", t)
        return t

    ft_norm = _normalize_tokens(ft)
    wm_norm = _normalize_tokens(wm)

    # Try production wrapper if we have enough context
    try:
        use_prod = bool(ctx["lender"] and ctx["deal"])
        if use_prod and not already_wrapped:
            import wrappers
            _deal = int(ctx["deal"]) if str(ctx["deal"]).isdigit() else ctx["deal"]
            _user_id = (session.get("google_email") or session.get("user_id") or "demo-user")
            

            info = wrappers.issue_wrapper_user_branded(
                user_id=_user_id,
                deal_id=_0,
                original_pdf_path=saved["path"],
                funder_name=ctx["lender"],
                recipient_email=ctx["email"],
                storage_dir=str(UPLOAD_DIR),
                supabase_url=SUPABASE_URL,
                supabase_service_key=SUPABASE_SERVICE_ROLE,
                # pass through texts and the session tracker
                force_watermark_text=wm or None,
                force_footer_template=ft or None,
                force_tracking_id=tid,
                # optional: you can force a logo file if you want:
                # force_logo_path=LOGO_PATH,
            )

            out_path = info["wrapper_path"]
            return send_file(
                out_path,
                mimetype="application/pdf",
                as_attachment=False,
                download_name=os.path.basename(out_path),
                max_age=0,
            )
    except Exception as e:
        log.exception("issue_wrapper_user_branded failed; falling back to _wrap_upload: %s", e)

    # Fallback path: expand tokens locally for _wrap_upload (it doesn't expand)
    token_map = {
        "funder": ctx["lender"],
        "lender": ctx["lender"],
        "recipient": ctx["email"],
        "email": ctx["email"],
        "deal": ctx["deal"],
        "fp": tid,
        "tracking": tid,
        "tracking_id": tid,
        "user": ctx["user"],
        "sender": ctx["user"],
    }
    def _expand(text: str) -> str:
        if not text: return ""
        # supports {token} and {{token}}
        text = re.sub(r"{{\s*(\w+)\s*}}", lambda m: str(token_map.get(m.group(1), "")), text)
        text = re.sub(r"{\s*(\w+)\s*}",  lambda m: str(token_map.get(m.group(1), "")), text)
        return text

    ft_expanded = _expand(ft)
    wm_expanded = _expand(wm)

    if already_wrapped:
        wrapped = saved
    else:
        wrapped = _wrap_upload(saved, footer_text=ft_expanded, watermark_text=wm_expanded)

    return send_file(
        wrapped["path"],
        mimetype="application/pdf",
        as_attachment=False,
        download_name=wrapped["filename"],
        max_age=0,
    )
"""



@bp.post("/wrap")
def api_wrap():
    from flask import send_file, jsonify, request, session
    import os, re, uuid

    f = request.files.get("file")
    if not f:
        return jsonify({"error": "missing file"}), 400

    # ----------------------------
    # Helpers
    # ----------------------------
    def _get_or_create_tracking_id():
        """
        Stable tracking_id resolution order:
        1) request.form['tracking_id'] (if client supplied)
        2) session['tracking_id'] (sticky within user session)
        3) generate a new UUID4 and store in session
        """
        tid = (request.form.get("tracking_id") or "").strip()
        if not tid:
            tid = (session.get("tracking_id") or "").strip()
        if not tid:
            tid = str(uuid.uuid4())
            session["tracking_id"] = tid
        return tid

    def _normalize(t: str) -> str:
        """
        - Unify aliases to canonical placeholders
        - Keep {fp} as-is (wrapper will fill fingerprint)
        - Do NOT stamp deal IDs visibly
        """
        if not t:
            return ""
        # normalize double braces -> single
        t = t.replace("{{", "{").replace("}}", "}")
        # aliases -> canonical
        t = re.sub(r"{\s*(lender|lender_name|funder_name)\s*}", "{funder}", t, flags=re.I)
        t = re.sub(r"{\s*(email|recipient|to_email)\s*}", "{recipient}", t, flags=re.I)
        t = re.sub(r"{\s*(tracking_id|tracking)\s*}", "{tracking}", t, flags=re.I)
        # explicitly DO NOT replace {fp}; wrapper fills it
        # strip literal "Deal #" and deal tokens from visible text
        t = re.sub(r"{\s*(deal|deal_id)\s*}", "", t, flags=re.I)
        t = re.sub(r"(?:^|\s)[•\-\u2022]?\s*Deal\s*#\s*", " ", t, flags=re.I)
        return t

    def _safe_resolve_known_tokens(t: str, vals: dict) -> str:
        """
        Only replace {funder}, {recipient}, {user}, {tracking}.
        Leave unknown tokens like {fp} intact so the wrapper can fill them.
        """
        if not t:
            return ""
        def rep(m):
            key = m.group(1).strip().lower()
            if key in ("funder", "recipient", "user", "tracking"):
                return str(vals.get(key, ""))
            # leave anything else (e.g., fp) untouched
            return "{" + key + "}"
        return re.sub(r"{\s*([a-zA-Z0-9_]+)\s*}", rep, t)

    # ----------------------------
    # Save upload
    # ----------------------------
    saved = _save_upload(f, prefix="wrap")

    # ----------------------------
    # Context
    # ----------------------------
    ctx = {
        "lender": (request.form.get("lender") or "").strip(),
        "email":  (request.form.get("recipient_email")
                   or session.get("user_email")
                   or session.get("google_email")
                   or "").strip(),
        "user":   (session.get("user_name")
                   or session.get("user_email")
                   or session.get("google_email")
                   or "").strip(),
    }
    deal_id_raw = (request.form.get("deal_id") or session.get("deal_id") or "").strip()
    deal_id = int(deal_id_raw) if deal_id_raw.isdigit() else None
    tracking_id = _get_or_create_tracking_id()

    # ----------------------------
    # Incoming texts (defaults include tracker + fp)
    # ----------------------------
    wm_in = (request.form.get("force_watermark_text")
             or request.form.get("watermark_text")
             or "SENT VIA PATHWAY CATALYST • Track {tracking}").strip()

    ft_in = (request.form.get("force_footer_template")
             or request.form.get("footer_text")
             or "Submitted to {funder} by {recipient} • Track {tracking}").strip()

    # ----------------------------
    # Already wrapped?
    # ----------------------------
    name_up = (f.filename or "").strip()
    name_saved = (saved.get("filename") or "").strip()
    already_wrapped = bool(
        re.search(r"(?:\.wrapped\.pdf|-\s*wrapped\.pdf)$", name_up, flags=re.I) or
        re.search(r"(?:\.wrapped\.pdf|-\s*wrapped\.pdf)$", name_saved, flags=re.I)
    )

    # ----------------------------
    # Normalize + resolve (leave {fp} for wrapper)
    # ----------------------------
    ft_norm = _normalize(ft_in)
    wm_norm = _normalize(wm_in)

    vals = {
        "funder": ctx["lender"],
        "recipient": ctx["email"],
        "user": ctx["user"],
        "tracking": tracking_id,
    }

    ft_resolved = _safe_resolve_known_tokens(ft_norm, vals)
    wm_resolved = _safe_resolve_known_tokens(wm_norm, vals)

    # ----------------------------
    # Prefer production wrapper (if lender present and not already wrapped)
    # ----------------------------
    try:
        if ctx["lender"] and not already_wrapped and deal_id is not None:
            import wrappers
            info = wrappers.issue_wrapper_user_branded(
                user_id=(session.get("google_email") or session.get("user_id") or "demo-user"),
                deal_id=deal_id, 
                original_pdf_path=saved["path"],
                funder_name=ctx["lender"],
                recipient_email=ctx["email"],
                storage_dir=str(UPLOAD_DIR),
                supabase_url=SUPABASE_URL,
                supabase_service_key=SUPABASE_SERVICE_ROLE,
                # pass fully RESOLVED strings so lender/email/tracking are baked in;
                # leave {fp} so wrapper can fill fingerprint on render
                force_watermark_text=wm_resolved,
                force_footer_template=ft_resolved,
                # NEW: ensure stable fingerprint derivation in wrapper
                force_tracking_id=tracking_id,
            )
            out_path = info["wrapper_path"]
            return send_file(
                out_path,
                mimetype="application/pdf",
                as_attachment=False,
                download_name=os.path.basename(out_path),
                max_age=0,
            )
    except Exception as e:
        log.exception("issue_wrapper_user_branded failed; falling back: %s", e)

    # ----------------------------
    # Fallback (simple wrapper; uses resolved text as-is)
    # ----------------------------
    if already_wrapped:
        wrapped = saved
    else:
        wrapped = _wrap_upload(saved, footer_text=ft_resolved, watermark_text=wm_resolved)

    return send_file(
        wrapped["path"],
        mimetype="application/pdf",
        as_attachment=False,
        download_name=wrapped["filename"],
        max_age=0,
    )
