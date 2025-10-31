

from __future__ import annotations
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from dataclasses import asdict
from inspect import signature
from flask import Blueprint, jsonify, request, session
from werkzeug.utils import secure_filename
from wrappers import wrap_pdf_with_logo
from wrappers import wrap_pdf_secure
from email_preview_system import send_secure_document_email
from flask import send_file, abort
from uuid import UUID, uuid4
from urllib.parse import quote_plus
import html
import secrets 
import urllib.parse 



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
from datetime import datetime, timezone, timedelta
import time as _time
import secrets
import uuid, re
from io import BytesIO
import base64


from supabase import create_client, Client
from auth_guard import global_auth_before_request
from mca_analyzer import MCAAnalyzer
analyzer = MCAAnalyzer(os.environ.get("ANTHROPIC_API_KEY", ""))




SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE = os.environ.get("SUPABASE_SERVICE_ROLE")
if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE in environment.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE)
sb = supabase

def get_sb() -> Client:
    return sb


BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
EMAILS_BOOK_DIR = BASE_DIR / "emails-books"
EMAILS_BOOK_DIR.mkdir(exist_ok=True)
EMAILS_DIR = EMAILS_BOOK_DIR
LOGO_PATH = str(BASE_DIR / "static" / "logo.png")

_ONE_BY_ONE_GIF = base64.b64decode(
    "R0lGODlhAQABAPAAAP///wAAACH5BAAAAAAALAAAAAABAAEAAAICRAEAOw=="
)



EMAILS_JSON_PATH = Path(os.environ.get("LENDER_EMAILS_PATH", str(BASE_DIR / "emails.json")))
AUTH_STORE_PATH = Path(os.environ.get("AUTH_STORE_PATH", str(BASE_DIR / "auth_store.json")))
TOKENS_DIR = BASE_DIR / "tokens"
GOOGLE_TOKEN_FILE = TOKENS_DIR / "google.json"

bp = Blueprint("underwrite", __name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("underwrite")


import sys
sys.path.insert(0, str(BASE_DIR))

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

def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

def _parse_row_emails(s: str) -> List[str]:
    return [e.strip() for e in str(s or "").split(",") if e and e.strip()]


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
    if td is None:
        td = {}
    elif isinstance(td, dict):
        td = td
    elif isinstance(td, list):
        
        if len(td) == 1 and isinstance(td[0], dict):
            td = td[0]
        else:
            raise ValueError(f"Invalid Google token format (list): {td}")
    elif isinstance(td, str):
        
        raise ValueError(f"Invalid Google token format (string): {td}")
    else:
        raise ValueError(f"Unexpected Google token type {type(td)}: {td}")

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
               attachments: list) -> tuple[bool, str | None, str | None]:
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
        return False, f"gmail_error:http_{r.status_code}:{r.text[:300]}", None
    try:
        response_data = r.json()
        rid = response_data.get("id")
        thread_id = response_data.get("threadId")
    except Exception:
        rid = None
        thread_id = None
    return True, rid, thread_id

def graph_send(token_dict: dict, subject: str, body_html: str,
               sender_email: str, to_email: str, cc_list: List[str],
               attachments: List[Tuple[str, bytes]]) -> Tuple[bool, Optional[str], Optional[str]]:
    access_token = token_dict.get("access_token") or token_dict.get("token")
    if not access_token:
        return False, "no_graph_access_token", None
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
            # Note: sendMail doesn't return message ID or conversationId
            # We'd need to use /me/messages endpoint instead to get those
            return True, None, None
        return False, f"graph_http_{r.status_code}:{r.text[:200]}", None
    except Exception as e:
        log.exception("Graph send error")
        return False, f"graph_error:{e}", None


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
        "business_name": (application_json or {}).get("business_name", ""),
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
    thread_id: Optional[str] = None, 
) -> int:
    tid = str(tracking_id or uuid4())  
    payload = {
        "deal_id": deal_id,
        "lender_name": lender_name,
        "to_email": to_email or "",
        "cc_csv": ",".join(cc_list or []),
        "provider": provider,
        "provider_msg_id": provider_msg_id or "",
        "thread_id": thread_id or "",
        "status": status,
        "sender_email": login_email,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "tracking_id": tid, 
    }

    res = sb.table("deliveries").insert(payload, returning="representation").execute()
    rows = res.data or []
def record_open_event(
    tracking_id: str,
    deal_id: Optional[int],
    lender_name: str,
    req,
) -> None:
    """
    Logs an open event to a central table.

    Create a Supabase table, e.g. `email_opens` with columns:
      id           bigint, primary key
      tracking_id  text
      deal_id      int8
      lender_name  text
      opened_at    timestamptz
      user_agent   text
      ip           text
    """
    try:
        ua = req.headers.get("User-Agent") or ""
        ip = (
            req.headers.get("X-Forwarded-For", "").split(",")[0].strip()
            or req.remote_addr
            or ""
        )
        payload = {
            "tracking_id": tracking_id,
            "deal_id": deal_id,
            "lender_name": lender_name or "",
            "opened_at": datetime.now(timezone.utc).isoformat(),
            "user_agent": ua,
            "ip": ip,
        }
        sb.table("email_opens").insert(payload).execute()
    except Exception:
        log.exception("Failed to record open event for %s", tracking_id)


@bp.get("/pixel/<tracking_id>.gif")
def pixel(tracking_id: str):
    """
    1x1 tracking pixel endpoint.
    Called when the email client loads the image; we log the open
    and return a transparent GIF.
    """
    try:
        deal_id = request.args.get("deal_id", type=int)
    except Exception:
        deal_id = None

    lender_name = request.args.get("lender") or ""

    try:
        record_open_event(tracking_id, deal_id, lender_name, request)
    except Exception:
        log.exception("Error recording open event for %s", tracking_id)

  
    return send_file(
        BytesIO(_ONE_BY_ONE_GIF),
        mimetype="image/gif",
        as_attachment=False,
        download_name="pixel.gif",
    )

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



def _wrap_upload(saved: dict, *, footer_text=None, watermark_text=None, prefix: str = None) -> dict:
    import os
    import re
    import wrappers

    fn = getattr(wrappers, "wrap_pdf_with_logo", None)
    if not fn:
        raise RuntimeError("wrap function missing in wrappers.py")

    
    name = saved.get("filename") or os.path.basename(saved["path"])
    if re.search(r"(?:\.wrapped\.pdf|-\s*wrapped\.pdf)$", name, re.I):
        return {**saved, "wrap_ok": True, "wrap_msg": "already wrapped"}

    out_path = fn(
        saved["path"],
        logo_path=LOGO_PATH,
        output_dir=str(UPLOAD_DIR),  
        footer_text=footer_text or "Submitted via Pathway Catalyst",
        watermark_text=watermark_text or "SENT VIA PATHWAY CATALYST",
    )
    log.info("wrap: ok path=%s -> %s", saved["path"], out_path)
    return {"path": out_path, "filename": os.path.basename(out_path), "wrap_ok": True, "wrap_msg": "ok"}

def _wrap_upload_secure(
    saved: dict,
    *,
    recipient_email: str,
    deal_id: int,
    user_id: str,
    tracking_url: str = None
):
    """
    Secure wrapper: apply 6-layer fingerprinting using wrappers.wrap_pdf_secure()
    Does NOT apply logo / visual watermark.
    """
    import wrappers

    fn = getattr(wrappers, "wrap_pdf_secure", None)
    if not fn:
        raise RuntimeError("wrap_pdf_secure missing in wrappers.py")

    input_path = saved["path"]
    file_name = os.path.basename(input_path)

    if file_name.endswith(".secure.pdf") or ".secure_" in file_name:
        return {**saved, "wrap_ok": True, "wrap_msg": "already secure wrapped"}

 
    out_path, fingerprint = fn(
        input_path,
        recipient_email=recipient_email,
        deal_id=str(deal_id),
        user_id=str(user_id),
        tracking_url=tracking_url,
    )

    return {
        **saved,
        "path": out_path,
        "filename": os.path.basename(out_path),
        "wrap_ok": True,
        "fingerprint": fingerprint,
        "wrap_type": "secure",
    }

def _wrap_upload_combined(
    saved: dict,
    *,
    recipient_email: str,
    deal_id: int,
    user_id: str,
    footer_text: str = None,
    watermark_text: str = None,
    tracking_url: str = None
):
    """
    Combined wrapper:
    1. Apply secure 6-layer PDF fingerprint (metadata, ID, annotation, embedded original)
    2. Then apply cosmetic logo/watermark overlay.
    """
 
    wrapped = _wrap_upload_secure(
        saved,
        recipient_email=recipient_email,
        deal_id=deal_id,
        user_id=user_id,
        tracking_url=tracking_url,
    )

    
    final = _wrap_upload_with_logo(
        {"path": wrapped["path"], "filename": wrapped["filename"]},
        footer_text=footer_text,
        watermark_text=watermark_text,
    )

    final["fingerprint"] = wrapped["fingerprint"]
    final["wrap_type"] = "combined"

    return final



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


@bp.post("/extract-application")

def extract_application_only():
    try:
        app_file = request.files.get("application")
        if not app_file:
            return jsonify({"error": "Missing application PDF"}), 400
        saved = _save_upload(app_file, prefix="application")
        application = _extract_application_fields(Path(saved["path"]))
        application["_wrapped_filename"] = saved["filename"]
        #application["_attachment"] = saved
        return jsonify({"application": application})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@bp.post("/statements-and-match")
def statements_and_match():
    analysis = None  
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

        files_for_summary: List[Tuple[str, bytes]] = []
        saved_files: List[dict] = []

        if len(stmt_files) < min_files:
            deal_id = request.form.get("deal_id")
            application_id = request.form.get("application_id")
            fetched = _fetch_statements_from_db(
                deal_id=int(deal_id) if str(deal_id or "").isdigit() else None,
                application_id=int(application_id) if str(application_id or "").isdigit() else None,
            )
            if fetched:
                for fname, raw in fetched:
                    
                    files_for_summary.append((fname, raw))
                    saved_files.append({"path": f"storage://{fname}", "filename": fname})
            else:
                return jsonify({"error": f"Need at least {min_files} statements (upload or present in DB) for state {state}"}), 400
        else:
            for f in stmt_files:
               
                saved = _save_upload(f, prefix="stmt")
                
                saved_files.append(saved)
                with open(saved["path"], "rb") as fh:
                    files_for_summary.append((saved["filename"], fh.read()))


        prev_json = request.form.get("existing_statements_json")
        prev = None
        if prev_json:
            try:
                prev = json.loads(prev_json) or {}
            except Exception:
                prev = None

        statements_new = _build_statements_payload(files_for_summary, state_for_rule=state)
        statements_new["_wrapped_filenames"] = [f["filename"] for f in saved_files]

        if prev and isinstance(prev, dict):
            combined_per = (prev.get("per_statement") or []) + (statements_new.get("per_statement") or [])
            combined_files = (prev.get("_saved_files") or []) + saved_files
            aggregates = _aggregate_statements_and_revenue(combined_per, state_for_rule=state)
            statements_payload = {"per_statement": combined_per, **aggregates, "_saved_files": combined_files}
        else:
            statements_payload = statements_new

       
        try:
            
            pdf_paths = [
                f["path"]
                for f in saved_files
                if isinstance(f.get("path"), str) and not f["path"].startswith("storage://")
            ]
            api_key = os.environ.get("ANTHROPIC_API_KEY", "")
            if api_key and pdf_paths:
                analyzer = MCAAnalyzer(api_key)
                combined = {"text": "", "tables": []}
                for p in pdf_paths:
                    try:
                        payload = analyzer.load_bank_statement_pdf(p)
                        combined["text"] += "\n\n" + (payload.get("text") or "")
                        combined["tables"].extend(payload.get("tables") or [])
                    except Exception:
                        continue

                business_info = {
                    "business_name": application.get("business_name"),
                    "state": application.get("state"),
                    "industry": application.get("industry"),
                    "fico": application.get("fico"),
                    "length_of_ownership": application.get("length_of_ownership"),
                    "length_months": application.get("length_months"),
                    "positions": application.get("positions"),
                }

                if combined["text"] or combined["tables"]:
                    analysis = analyzer.prepare_analysis_data(combined, business_info)
        except Exception:
            traceback.print_exc()
            analysis = None

        lenders = _match_lenders(application, statements_payload)
        lenders = _append_default_lenders(lenders)

        return jsonify({
            "statements": statements_payload,
            "lenders": lenders,
            "analysis": analysis,
        })
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


def _build_tracking_pixel_url(
    tracking_id: str,
    deal_id: int,
    lender_name: str,
) -> str:
    """
    Build an absolute URL to the Supabase pixel function.
    """
    docs_base = os.getenv(
        "DOCS_BASE_URL",
        "https://lxqsswgqugwszhovfsxw.functions.supabase.co",
    )
    qs = urllib.parse.urlencode(
        {
            "tracking_id": tracking_id,
            "deal_id": deal_id,
            "lender": lender_name or "",
        }
    )
    return f"{docs_base}/pixel?{qs}"


def _inject_tracking_pixel(
    body_html: str,
    *,
    tracking_id: str,
    deal_id: int,
    lender_name: str,
) -> str:
    """
    Append a hidden 1x1 tracking pixel <img> to the HTML body.
    If </body> exists, we inject before it; otherwise we append at the end.
    """
    pixel_url = f"https://lxqsswgqugwszhovfsxw.functions.supabase.co/email-pixel?tracking_id={tracking_id}&deal_id={deal_id}&lender={lender_name}"

    pixel_tag = (
    f'<img src="{pixel_url}" alt="" width="1" height="1" '
    f'style="display:none;border:0;" />'
)
    lower = body_html.lower()
    closing_idx = lower.rfind("</body>")
    if closing_idx != -1:
        
        return (
            body_html[:closing_idx]
            + pixel_tag
            + body_html[closing_idx:]
        )
    else:
        
        return body_html + "\n" + pixel_tag

"""
@bp.post("/send")
def send_emails():

    json: 
    {
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
    
    try:
        lname = ""
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
        prov = (provider or "").lower()

        for lender in selected:
            lname = (lender or "").strip()
            lname_key = lname.lower()
            if not lname or lname_key in exclude:
                continue

            # 1) Resolve recipients (CSV first; falls back to emails.json)
            try:
                to_email, cc_list = resolve_recipients_user_csv_first(lname, user_cc)
            except Exception as e:
                log.warning(f"resolve_recipients failed for {lname}: {e}")
                to_email, cc_list = None, _dedupe_emails(user_cc or [])

            if not to_email:
                reason = "no_recipient"
                deliveries.append({
                    "lender": lname, "from": sender_email, "to": "",
                    "cc": cc_list, "status": "error", "provider": (provider or ""), "reason": reason
                })
                try:
                    record_delivery(deal_id, lname, "", cc_list, provider or "", None, "error", sender_email)
                except Exception:
                    pass
                continue

            # 2) Choose attachments per-lender (dict → list; list → same for all; else global)
            if isinstance(per_map, dict):
                attachments_for_this = per_map.get(lname) or per_map.get(lname_key) or []
            elif isinstance(per_map, list):
                attachments_for_this = per_map
            else:
                attachments_for_this = attachments_global

            _ = need_wrap_fallback  # reserved flag; wrapping handled earlier if needed

            # 3) Inject tracking pixel
            delivery_tracking_id = str(uuid4())
            body_with_pixel = _inject_tracking_pixel(
                body_html=body,
                tracking_id=delivery_tracking_id,
                deal_id=deal_id,
                lender_name=lname,
            )

            # 4) Send
            ok, provider_id, thread_id = False, None, None
            if prov == "gmail":
                ok, provider_id, thread_id = gmail_send(
                    token, final_subject, body_with_pixel,
                    sender_email, to_email, cc_list,
                    attachments=attachments_for_this,
                )
            elif prov in ("outlook", "graph"):
                ok, provider_id, thread_id = graph_send(
                    token, final_subject, body_with_pixel,
                    sender_email, to_email, cc_list,
                    attachments=attachments_for_this,
                )
            else:
                reason = f"unsupported_provider:{provider}"
                deliveries.append({
                    "lender": lname, "from": sender_email, "to": to_email,
                    "cc": cc_list, "status": "error", "provider": (provider or ""), "reason": reason
                })
                try:
                    record_delivery(deal_id, lname, to_email, cc_list, provider or "", None, "error", sender_email,
                                    tracking_id=delivery_tracking_id)
                except Exception:
                    pass
                continue

            status = "sent" if ok else "error"
            reason = None if ok else (provider_id if isinstance(provider_id, str) else "send_failed")

            deliveries.append({
                "lender": lname, "from": sender_email, "to": to_email, "cc": cc_list,
                "status": status, "provider": (provider or ""), "provider_id": (None if not ok else provider_id),
                "reason": reason
            })
            try:
                record_delivery(
                    deal_id, lname, to_email, cc_list, provider or "",
                    (None if not ok else provider_id), status, sender_email,
                    tracking_id=delivery_tracking_id,
                    thread_id=thread_id
                )
            except Exception:
                pass

        return jsonify({
            "ok": True,
            "from": sender_email,
            "deal_id": deal_id,
            "subject": final_subject,
            "deliveries": deliveries
        })
    except Exception as e:
        log.exception("send_emails failed: %s", e)
        return jsonify({"error": str(e)}), 500

"""
def _record_open_event_for_tracking_id(tracking_id: str) -> None:
    """
    Lookup doc_tracking by tracking_id and insert an open event row.
    Safe to fail silently (we never want to break the pixel response).
    """
    try:
     
        resp = (
            sb.table("doc_tracking")
              .select("id")
              .eq("tracking_id", tracking_id)
              .limit(1)
              .execute()
        )
        rows = resp.data or []
        if not rows:
            
            return

        doc_tracking_id = rows[0]["id"]


        ua = request.headers.get("User-Agent") or ""
        ip = (
            (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip()
            or request.remote_addr
            or ""
        )

        payload = {
            "doc_tracking_id": doc_tracking_id,
            "opened_at": datetime.now(timezone.utc).isoformat(),
            "user_agent": ua,
            "ip_address": ip,
        }

        sb.table("doc_tracking_opens").insert(payload).execute()
    except Exception:
        
        log.exception("Failed to record open event for tracking_id=%s", tracking_id)



@bp.get("/pixel/<tracking_id>.gif")
def tracking_pixel(tracking_id: str):
    """
    1x1 tracking pixel endpoint.
    When the email client loads this URL, we record an 'open' event and
    return a transparent GIF.
    """
    try:
        _record_open_event_for_tracking_id(tracking_id)
    except Exception:
        
        log.exception("tracking_pixel failed for %s", tracking_id)

    return send_file(
        BytesIO(_ONE_BY_ONE_GIF),
        mimetype="image/gif",
        as_attachment=False,
        download_name="pixel.gif",
    )


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

@bp.get("/lead/<int:application_id>")
def api_lead_detail(application_id: int):
    """Fetch lead data from applications table - check payload for bank statements"""
    try:
        sbu = get_sb()
        
   
        app_resp = (sbu.table("applications")
                      .select("*")
                      .eq("id", int(application_id))
                      .limit(1)
                      .execute())
        
        if not app_resp.data:
            return jsonify({"error": "Lead not found"}), 404
        
        lead = app_resp.data[0]
        
        payload = lead.get("payload") or {}
        
        documents = {
            "statements": [],
            "applications": []
        }
        has_statements = False
        
        
        if payload and isinstance(payload, dict):
            documents["applications"].append({
                "id": f"app_{application_id}",
                "filename": "application_data.json",
                "data": json.dumps(payload)
            })
        
     
        bank_statements = (
            payload.get("bank_statements") or
            payload.get("statements") or
            payload.get("statement_data") or
            payload.get("statement") or
            None
        )
        
        if bank_statements:
          
            if isinstance(bank_statements, list):
                
                for i, stmt in enumerate(bank_statements):
                    if stmt:  
                        documents["statements"].append({
                            "id": f"stmt_{application_id}_{i}",
                            "filename": f"statement_{i+1}.json",
                            "data": json.dumps(stmt) if isinstance(stmt, dict) else stmt
                        })
                        has_statements = True
            elif isinstance(bank_statements, dict):
               
                documents["statements"].append({
                    "id": f"stmt_{application_id}",
                    "filename": "bank_statements.json",
                    "data": json.dumps(bank_statements)
                })
                has_statements = True
            elif isinstance(bank_statements, str):
             
                try:
                    parsed = json.loads(bank_statements)
                    documents["statements"].append({
                        "id": f"stmt_{application_id}",
                        "filename": "bank_statements.json",
                        "data": json.dumps(parsed)
                    })
                    has_statements = True
                except:
                    
                    documents["statements"].append({
                        "id": f"stmt_{application_id}",
                        "filename": "bank_statements.txt",
                        "data": bank_statements
                    })
                    has_statements = True
        
        log.info(f"Loaded lead {application_id}: has_app=true, has_statements={has_statements}")
        
        return jsonify({
            "lead": lead,
            "documents": documents,
            "has_statements": has_statements,
            "has_application": True  
        })
        
    except Exception as e:
        log.exception("Failed to fetch lead detail: %s", e)
        return jsonify({"error": str(e)}), 500

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



@bp.post("/pdf/link")
def create_pdf_link():
    try:
        data = request.get_json()
        pdf_path = data.get("pdf_path")
        recipient = data.get("recipient_email", "")
        deal_id = data.get("deal_id")
        
        # Generate token
        token = secrets.token_urlsafe(24)
        
        # Store in DB
        get_sb().table("pdf_links").insert({
            "token": token,
            "pdf_path": pdf_path,
            "recipient": recipient,
            "deal_id": deal_id,
            "created": datetime.now(timezone.utc).isoformat(),
            "expires": (datetime.now(timezone.utc) + timedelta(days=30)).isoformat(),
            "views": 0
        }).execute()
        
        link = f"{request.host_url}v/{token}"
        log.info(f"Created PDF link: {token} for {recipient}")
        
        return jsonify({"success": True, "link": link, "token": token})
        
    except Exception as e:
        log.exception("Failed to create PDF link")
        return jsonify({"error": str(e)}), 500


@bp.get("/v/<token>")
def view_pdf(token: str):
    try:
       
        result = get_sb().table("pdf_links").select("*").eq("token", token).single().execute()
        
        if not result.data:
            abort(404)
        
        info = result.data
        
        
        expires = datetime.fromisoformat(info["expires"].replace("Z", "+00:00"))
        if datetime.now(timezone.utc) > expires:
            return "Link expired", 403
        
       
        get_sb().table("pdf_views").insert({
            "token": token,
            "deal_id": info.get("deal_id"),
            "time": datetime.now(timezone.utc).isoformat(),
            "ip": request.headers.get("X-Forwarded-For", request.remote_addr),
            "agent": request.headers.get("User-Agent", "")[:500]
        }).execute()
        
      
        get_sb().table("pdf_links")\
            .update({"views": info.get("views", 0) + 1})\
            .eq("token", token)\
            .execute()
        
        log.info(f"PDF viewed: {token} (view #{info.get('views', 0) + 1})")
        
    
        return send_file(
            info["pdf_path"],
            mimetype="application/pdf",
            as_attachment=False,
            max_age=0
        )
        
    except Exception as e:
        log.exception(f"Failed to serve PDF: {token}")
        abort(500)


@bp.get("/stats/<token>")
def pdf_stats(token: str):
    try:
       
        link = get_sb().table("pdf_links").select("*").eq("token", token).single().execute()
        
        
        views = get_sb().table("pdf_views").select("*").eq("token", token)\
            .order("time", desc=True).execute()
        
        if not link.data:
            return jsonify({"error": "Not found"}), 404
        
        unique_ips = len(set(v["ip"] for v in views.data))
        
        return jsonify({
            "token": token,
            "recipient": link.data.get("recipient"),
            "total_views": link.data.get("views", 0),
            "unique_ips": unique_ips,
            "created": link.data["created"],
            "expires": link.data["expires"],
            "last_view": views.data[0]["time"] if views.data else None,
            "recent_views": views.data[:10]  
        })
        
    except Exception as e:
        log.exception("Stats failed")
        return jsonify({"error": str(e)}), 500

@bp.post("/wrap")
def api_wrap():
    from flask import send_file, jsonify, request, session
    import os, re, uuid

    f = request.files.get("file")
    if not f:
        return jsonify({"error": "missing file"}), 400

    def _get_or_create_tracking_id():
        tid = (request.form.get("tracking_id") or "").strip()
        if not tid:
            tid = (session.get("tracking_id") or "").strip()
        if not tid:
            tid = str(uuid.uuid4())
            session["tracking_id"] = tid
        return tid

    def _normalize(t: str) -> str:
        if not t:
            return ""
        
        t = t.replace("{{", "{").replace("}}", "}")
        t = re.sub(r"{\s*(lender|lender_name|funder_name)\s*}", "{funder}", t, flags=re.I)
        t = re.sub(r"{\s*(email|recipient|to_email)\s*}", "{recipient}", t, flags=re.I)
        t = re.sub(r"{\s*(tracking_id|tracking)\s*}", "{tracking}", t, flags=re.I)
        
        t = re.sub(r"{\s*(deal|deal_id)\s*}", "", t, flags=re.I)
        t = re.sub(r"(?:^|\s)[•\-\u2022]?\s*Deal\s*#\s*", " ", t, flags=re.I)
        return t

    def _safe_resolve_known_tokens(t: str, vals: dict) -> str:
        if not t:
            return ""
        def rep(m):
            key = m.group(1).strip().lower()
            if key in ("funder", "recipient", "user", "tracking"):
                return str(vals.get(key, ""))
            
            return "{" + key + "}"
        return re.sub(r"{\s*([a-zA-Z0-9_]+)\s*}", rep, t)


    saved = _save_upload(f, prefix="wrap")


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


    wm_in = (request.form.get("force_watermark_text")
             or request.form.get("watermark_text")
             or "SENT VIA PATHWAY CATALYST • Track {tracking}").strip()

    ft_in = (request.form.get("force_footer_template")
             or request.form.get("footer_text")
             or "Submitted to {funder} by {recipient} • Track {tracking}").strip()

    
    name_up = (f.filename or "").strip()
    name_saved = (saved.get("filename") or "").strip()
    already_wrapped = bool(
        re.search(r"(?:\.wrapped\.pdf|-\s*wrapped\.pdf)$", name_up, flags=re.I) or
        re.search(r"(?:\.wrapped\.pdf|-\s*wrapped\.pdf)$", name_saved, flags=re.I)
    )

    
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

  
    try:
        if ctx["lender"] and not already_wrapped and deal_id is not None:
            import wrappers
            info = wrappers.issue_tamper_proof_wrapper(
                user_id=(session.get("google_email") or session.get("user_id") or "demo-user"),
                deal_id=deal_id,
                original_pdf_path=saved["path"],
                funder_name=ctx["lender"],
                recipient_email=ctx["email"],
                storage_dir=str(UPLOAD_DIR),
                supabase_url=SUPABASE_URL,
                supabase_service_key=SUPABASE_SERVICE_ROLE,
               
                tracking_id=tracking_id,
               
                force_watermark_text=wm_resolved,
                force_footer_template=ft_resolved,
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
        log.exception("issue_tamper_proof_wrapper failed; falling back: %s", e)

  
    if already_wrapped:
        wrapped = saved
    else:
        wrapped = _wrap_upload(saved, footer_text=ft_resolved, watermark_text=wm_resolved)
  
    if request.form.get("return_link") == "true":
        link_resp = requests.post(
            f"{request.host_url}pdf/link",
            json={
                "pdf_path": wrapped["path"],
                "recipient_email": ctx["email"],
                "deal_id": deal_id
            }
        )
        
        return jsonify({
            "success": True,
            "link": link_resp.json()["link"],
            "message": "Send this link instead of attaching PDF"
        })
    return send_file(
        wrapped["path"],
        mimetype="application/pdf",
        as_attachment=False,
        download_name=wrapped["filename"],
        max_age=0,
    )

@bp.post("/wrap-secure")
@bp.post("/wrap-secure/")
def api_wrap_secure():
    file = request.files.get("file")
    recipient_email = request.form.get("email")
    recipient_name = request.form.get("name")
    deal_id = request.form.get("deal_id")

    if not file or not recipient_email:
        return jsonify({"error": "Missing PDF or recipient email"}), 400
    
   
    temp_path = os.path.join("/tmp", f"{uuid.uuid4()}.pdf")
    file.save(temp_path)

    wrapped_path, fingerprint_id = wrap_pdf_secure(temp_path, recipient_email)

  
    tracking_id = str(uuid.uuid4())

    
    base_url = os.getenv("BASE_URL", "https://lxqsswgqugwszhovfsxw.functions.supabase.co")
    view_link = f"{base_url}/view-pdf?token={secure_token}"


    supabase.table("pdf_links").insert({
        "tracking_id": tracking_id,
        "recipient_email": recipient_email,
        "fingerprint_id": fingerprint_id,
        "deal_id": deal_id,
        "created_at": datetime.datetime.utcnow().isoformat(),
        "expires_at": (datetime.datetime.utcnow() + datetime.timedelta(days=30)).isoformat(),
        "view_link": view_link,
        "status": "created",
        "pdf_path": wrapped_path,
    }).execute()

    send_secure_document_email(
        recipient_email=recipient_email,
        recipient_name=recipient_name or "",
        pdf_path=wrapped_path,
        tracking_id=tracking_id,
        view_link=view_link,
    )

    return jsonify({
        "message": "Secure document sent successfully.",
        "tracking_id": tracking_id,
        "view_link": view_link
    }), 200

@bp.post("/send")
def send_emails_secure():
    """
    COMBINED FUNCTIONALITY: Sends emails to multiple lenders with tamper-proof PDF protection.
    
    This function combines:
    1. send_emails(): Bulk sending, tracking, deal management
    2. api_wrap_secure(): Tamper-proof PDF wrapping and secure view links
    
    JSON Payload:
    {
      "selected_lenders": [...],           # List of lender names
      "subject": "...",                     # Email subject
      "message": "...",                     # Email body (HTML)
      "cc": [...],                          # CC recipients
      "mode": "MCA|CCS|REV|SBA|RE|EQP",    # Deal type
      "application": {...},                 # Application data
      "statements": {...},                  # Statement data
      "attachments": [{name, data}, ...],   # Base64 attachments
      "per_lender_attachments": {...},      # Per-lender specific attachments
      "parent_deal_id": 123,                # Optional parent deal
      "exclude": ["LenderA"],               # Optional exclusions
      "use_secure_links": true,             # NEW: Enable secure link mode
      "wrap_pdfs": true,                    # NEW: Enable tamper-proof wrapping
      "link_expiry_days": 30                # NEW: Link expiration (default 30)
    }
    
    Returns:
    {
      "ok": true,
      "from": "sender@example.com",
      "deal_id": 123,
      "subject": "...",
      "deliveries": [
        {
          "lender": "Lender Name",
          "from": "sender@example.com",
          "to": "lender@example.com",
          "cc": [...],
          "status": "sent|error",
          "provider": "gmail|outlook",
          "provider_id": "...",
          "tracking_id": "...",            # NEW: Unique tracking ID
          "view_link": "...",              # NEW: Secure view link (if enabled)
          "fingerprint_id": "...",         # NEW: PDF fingerprint (if wrapped)
          "reason": "..."                   # Error reason if status=error
        }
      ]
    }
    """
    log.info("=" * 70)
    log.info("🚀 send_emails_secure() CALLED")
    log.info("=" * 70)
    try:
        
        data = request.get_json(force=True) or {}
        log.info(f"📦 Data keys: {list(data.keys())}")
        log.info(f"📧 Selected lenders: {data.get('selected_lenders', [])}")
        log.info(f"🔒 wrap_pdfs: {data.get('wrap_pdfs', False)}")
        log.info(f"🔗 use_secure_links: {data.get('use_secure_links', False)}")
        selected = data.get("selected_lenders") or []
        subject = data.get("subject") or ""
       #body = data.get("message") or ""
        user_message = data.get("message") or ""
        user_cc = data.get("cc") or []
        mode = (data.get("mode") or "").upper() or "MCA"
        
        
        use_secure_links = data.get("use_secure_links", False)
        wrap_pdfs = data.get("wrap_pdfs", False)
        link_expiry_days = data.get("link_expiry_days", 30)
        
        application_obj = data.get("application") or {}
        statements_obj = data.get("statements") or {}
        business_name =  application_obj.get("business_name") or data.get("application_name") or "App and Bank Statements"
        attachments_global = _parse_attachments_from_json(data)
        per_map = _parse_per_lender_attachments_from_json(data)
        
        if isinstance(user_cc, str):
            user_cc = [x.strip() for x in user_cc.split(",") if x.strip()]
        
        if not selected:
            return jsonify({"error": "No lenders selected"}), 400
        
    
        sender_email, provider, token = safe_get_connected_sender()
        if not sender_email or not token:
            return jsonify({"error": "No connected mailbox. Connect Gmail/Outlook first."}), 403
        

        deal_id = record_deal(
            session.get("google_email") or "demo-user",
            sender_email, subject, user_message, mode,
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
            except Exception:
                pass
        
       
        deliveries = []
        prov = (provider or "").lower()
        docs_base = os.getenv(
         "DOCS_BASE_URL",
        "https://lxqsswgqugwszhovfsxw.functions.supabase.co",
        )
        
        for lender in selected:
            lname = (lender or "").strip()
            lname_key = lname.lower()
            
            if not lname or lname_key in exclude:
                continue
            
            # Resolve recipients
            try:
                to_email, cc_list = resolve_recipients_user_csv_first(lname, user_cc)
            except Exception as e:
                log.warning(f"resolve_recipients failed for {lname}: {e}")
                to_email, cc_list = None, _dedupe_emails(user_cc or [])
            
            if not to_email:
                deliveries.append({
                    "lender": lname, "from": sender_email, "to": "",
                    "cc": cc_list, "status": "error", "provider": provider or "",
                    "reason": "no_recipient"
                })
                record_delivery(deal_id, lname, "", cc_list, provider or "", None, "error", sender_email)
                continue
            
          
            if isinstance(per_map, dict):
                attachments_for_this = per_map.get(lname) or per_map.get(lname_key) or []
            elif isinstance(per_map, list):
                attachments_for_this = per_map
            else:
                attachments_for_this = attachments_global
           
            from email_preview_system import generate_pdf_preview, build_email_preview_html
            
            tracking_id = str(uuid.uuid4())
            view_links = [] 
            fingerprint_id = None
            wrapped_attachments = []
            body_with_link = user_message  
            pdf_previews = []  
            
            if wrap_pdfs and attachments_for_this:
                log.info(f"🔒 Wrapping {len(attachments_for_this)} PDFs for {lname}")
                
                per_statement_list = statements_obj.get("per_statement") or []
                statement_name_map = {}
                for stmt in per_statement_list:
                    src = stmt.get("source_file") or ""
                    bank = stmt.get("bank_name") or stmt.get("institution") or ""
                    period = stmt.get("statement_period") or stmt.get("month") or ""
                    if bank or period:
                        statement_name_map[src] = f"{bank}_{period}".strip("_") or src
                    else:
                        statement_name_map[src] = os.path.splitext(src)[0] if src else ""
                
               
                for att_idx, att in enumerate(attachments_for_this):
                    
                    att_name, att_data = att
                    log.info(f"   Processing: {att_name}")
                    
                    if att_name.lower().endswith(".pdf"):
                       
                        temp_path = os.path.join("/tmp", f"{uuid.uuid4()}.pdf")
                        with open(temp_path, "wb") as f:
                            f.write(att_data)  
                        log.info(f"   Wrote {len(att_data)} bytes to {temp_path}")
                        
                        
                        log.info(f"   Calling wrap_pdf_secure for {to_email}")
                        wrapped_path, fp_id = wrap_pdf_secure(
                            temp_path,
                            to_email,
                            deal_id=deal_id,
                            #user_id=str(user_id),
                            #watermark_text=f"CONFIDENTIAL – Sent to {lname} – Deal {deal_id}",
                            footer_text=f"Submitted  to {lname} via Pathway Catalyst",
                            logo_path="/Users/maheedharraogovada/Desktop/Paradise again/Statements/static/logo.png",
                        )

                        fingerprint_id = fp_id
                        log.info(f"   ✅ Wrapped: {wrapped_path}, fingerprint: {fp_id}")
                        
                        preview_b64 = None
                        try:
                            log.info(f"   Generating PDF preview for {att_name}...")
                            preview_b64 = generate_pdf_preview(wrapped_path)
                            pdf_previews.append({
                                'name': att_name,
                                'preview': preview_b64
                            })
                            log.info(f"   ✅ Preview generated for {att_name}")
                        except Exception as preview_error:
                            log.warning(f"   ⚠️  Preview generation failed for {att_name}: {preview_error}")
                      
                        if use_secure_links:
                          
                            secure_token = str(uuid.uuid4())
                            attachments_to_send = []
                            
                            now = datetime.utcnow()
                            expires_dt = now + timedelta(days=link_expiry_days)

                           
                            this_link = f"{os.getenv('PDF_PROXY_URL', 'https://endpoint-production-a456.up.railway.app')}/docs/{secure_token}"
                            view_links.append({
                                "name": att_name,
                                 "link": this_link
                            })
                            log.info(f"   Creating secure link: {view_links}")
                            
                            if att_idx == 0:
                                storage_file_name = f"{business_name}__{lname}__{fp_id}.pdf"
                            else:
                               
                                stmt_name = statement_name_map.get(att_name) or os.path.splitext(att_name)[0]
                                storage_file_name = f"{stmt_name}__{lname}__{fp_id}.pdf"


                            with open(wrapped_path, "rb") as f:
                                sb.storage.from_("secure-pdfs").upload(
                                storage_file_name,
                                f.read(),
                                {"content-type": "application/pdf"}
                            )

                            row = {
                                "token": secure_token,                       
                                "tracking_id": tracking_id,
                                "recipient_email": to_email,
                                "fingerprint_id": fingerprint_id,
                                "deal_id": deal_id,
                                "lender_name": lname,
                                "created_at": now.isoformat(),
                                "expires_at": expires_dt.isoformat(),
                                "view_link": (view_links[0]["link"] if view_links else None),
                                "status": "created",
                                "pdf_path": storage_file_name,
                            }

                            log.info(f"   pdf_links row type={type(row)} row={row!r}")

                            
                            sb.table("pdf_links").insert(row).execute()
                            log.info("   ✅ Logged to database")
                        else:
                           
                            log.info(f"   Adding wrapped PDF as attachment")
                            with open(wrapped_path, "rb") as f:
                                wrapped_attachments.append((att_name, f.read()))
                        
                       
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
                            log.info(f"   Cleaned up temp file")
                    else:
                        
                        log.info(f"   Passing through: {att_name}")
                        wrapped_attachments.append(att)
                
               
                if use_secure_links and pdf_previews and view_links:
   
                    #first = pdf_previews[0]
                    previews_html = ""
                for item in pdf_previews:
                    previews_html += f"""
<div style="margin: 18px 0; padding: 14px; background: #fafafa;
            border: 1px solid #e0e0e0; border-radius: 12px;">
    
    <div style="font-size: 15px; margin-bottom: 10px; color:#333;">
        <strong>📄 Document</strong><br>
        <span style="font-size:14px;color:#777;">{item['name']}</span>
    </div>

    <div style="text-align:center;">
        <img src="data:image/png;base64,{item['preview']}"
             alt="Preview"
             style="width: 30%; max-width: 260px; border-radius: 12px;
                    box-shadow: 0 4px 12px rgba(0,0,0,0.15);">
    </div>
</div>
        """

    
                    links_html = ""
                    for link_item in view_links:    
                        links_html += f"""
        <div style="margin: 6px 0;">
                <a href="{link_item['link']}" 
       style="display:inline-block; background:#007bff; color:#fff; 
              padding:12px 24px; border-radius:6px; text-decoration:none;
              font-weight:600; font-size:14px;">
            📥 Download {link_item['name']}
            </a>
        </div>
        """

    
                    body_with_link = f"""
<table width="100%" cellpadding="0" cellspacing="0" 
       style="max-width:620px;margin:auto;
              font-family:Arial,Helvetica,sans-serif;
              border:1px solid #e5e5e5;border-radius:14px;
              overflow:hidden;">

    <!-- Header -->
    <tr>
        <td style="background:#111;color:#fff;padding:20px 24px;">
            <h2 style="margin:0;font-size:22px;font-weight:600;">
                Harvest Lending X Pathway Catalyst 
            </h2>
        </td>
    </tr>

    <!-- Body -->
    <tr>
        <td style="padding:24px;">

            <p style="font-size:15px;color:#333;margin:0 0 12px;">
                Hi {lname or 'there'},
            </p>

            <p style="font-size:14px;color:#555;margin:0 0 20px;line-height:1.5;">
                {user_message}
            </p>

            <!-- Secure Links -->
            <div style="margin:20px 0;">
                {links_html}
            </div>

            <p style="font-size:13px;color:#777;margin-top:10px;">
                🔒 Links expire in <strong>{link_expiry_days} days</strong>
            </p>

            <hr style="margin:28px 0;border:none;border-top:1px solid #ddd;">

            <!-- Previews Section -->
            <h3 style="margin:0 0 16px;font-size:17px;color:#222;">
                Document Previews
            </h3>

            {previews_html}

        </td>
    </tr>

</table>
"""

                log.info(f"   ✅ Using HTML preview email with {len(pdf_previews)} PDF previews and {len(view_links)} secure links")

            else:
                
                attachments_to_send = attachments_for_this
                body_to_send = body
            
          
            delivery_tracking_id = str(uuid.uuid4())
            body_to_send = body_with_link
            email_tracking_id = str(uuid.uuid4())

            body_with_pixel = _inject_tracking_pixel(
                body_html=body_to_send,
                tracking_id=email_tracking_id,
                deal_id=deal_id,
                lender_name=lname,
            )


            ok, provider_id, thread_id = False, None, None
            if prov == "gmail":
                ok, provider_id, thread_id = gmail_send(
                    token, final_subject, body_with_pixel,
                    sender_email, to_email, cc_list,
                    attachments=attachments_to_send,
                )
            elif prov in ("outlook", "graph"):
                ok, provider_id, thread_id = graph_send(
                    token, final_subject, body_with_pixel,
                    sender_email, to_email, cc_list,
                    attachments=attachments_to_send,
                )
            else:
                ok = False
                provider_id = f"unsupported_provider:{provider}"
            
            status = "sent" if ok else "error"
            reason = None if ok else (provider_id if isinstance(provider_id, str) else "send_failed")
            
          
            delivery_record = {
                "lender": lname,
                "from": sender_email,
                "to": to_email,
                "cc": cc_list,
                "status": status,
                "provider": provider or "",
                "provider_id": None if not ok else provider_id,
                "tracking_id": tracking_id,  
                "view_link": (view_links[0]["link"] if view_links else None),      
                "fingerprint_id": fingerprint_id,  
                "reason": reason
            }
            deliveries.append(delivery_record)
            
            try:
                record_delivery(
                    deal_id, lname, to_email, cc_list, provider or "",
                    (None if not ok else provider_id), status, sender_email,
                    tracking_id=delivery_tracking_id,thread_id=thread_id
                )
            except Exception:
                pass
        
        
        return jsonify({
            "ok": True,
            "from": sender_email,
            "deal_id": deal_id,
            "subject": final_subject,
            "deliveries": deliveries,
            "security_features": {
                "secure_links_enabled": use_secure_links,
                "pdf_wrapping_enabled": wrap_pdfs,
                "link_expiry_days": link_expiry_days
            }
        })
        
    except Exception as e:
        log.exception("send_emails_secure failed: %s", e)
        return jsonify({"error": str(e)}), 500


@bp.get("/analytics/<tracking_id>")
def analytics_by_tracking(tracking_id):
    rows = supabase.table("pdf_views").select("*").eq("tracking_id", tracking_id).order("viewed_at").execute().data or []
    return jsonify({
        "tracking_id": tracking_id,
        "total_views": len(rows),
        "unique_visitors": len({r.get("fingerprint") for r in rows}),
        "views": rows
    })

@bp.get("/analytics/deal/<deal_id>")
def analytics_by_deal(deal_id):
    links = supabase.table("pdf_links").select("tracking_id,recipient_email,view_link,created_at,expires_at").eq("deal_id", deal_id).execute().data or []
    out = []
    for l in links:
        v = supabase.table("pdf_views").select("*").eq("tracking_id", l["tracking_id"]).order("viewed_at").execute().data or []
        out.append({
            "tracking_id": l["tracking_id"],
            "recipient_email": l["recipient_email"],
            "view_link": l["view_link"],
            "total_views": len(v),
            "unique_visitors": len({r.get("fingerprint") for r in v}),
            "first_viewed": v[0]["viewed_at"] if v else None,
            "last_viewed": v[-1]["viewed_at"] if v else None
        })
    return jsonify({"deal_id": deal_id, "documents": out})
"""
@bp.get("/v/<secure_token>")
def view_secure_pdf(secure_token):
    try:
        # 1. Find link entry
        resp = sb.table("pdf_links").select("*").eq("token", secure_token).execute()
        rows = resp.data or []
        if not rows:
            return "Invalid or expired link.", 404

        row = rows[0]

        # 2. Validate expiration
        expires_at = datetime.fromisoformat(row["expires_at"])
        if datetime.utcnow() > expires_at:
            return "This secure document link has expired.", 410

        # 3. Path inside Supabase Storage (saved earlier)
        storage_path = row["pdf_path"]   # e.g. secure-pdfs/abcd1234.pdf

        # 4. Create short-lived signed URL (15 minutes)
        signed = sb.storage.from_("secure-pdfs").create_signed_url(
            storage_path,
            60 * 15   # link is valid 15 minutes after click
        )
        secure_pdf_url = signed["signedURL"]

        # 5. Optional: record access analytics for your admin system
        try:
            sb.table("pdf_links").update({
                "last_accessed_at": datetime.utcnow().isoformat(),
                "access_count": (row.get("access_count") or 0) + 1
            }).eq("token", secure_token).execute()
        except:
            pass

        # 6. Redirect user to short-lived Supabase URL
        return redirect(secure_pdf_url)

    except Exception as e:
        log.exception("secure_link_viewer failed")
        return f"Error: {str(e)}", 500

@bp.get("/download/<token>")
def download_pdf(token: str):
    try:
        result = get_sb().table("pdf_links").select("*").eq("token", token).single().execute()
        if not result.data:
            abort(404)
        
        info = result.data
        
        # Check expiry
        expires = datetime.fromisoformat(info["expires_at"].replace("Z", "+00:00"))
        if datetime.now(timezone.utc) > expires:
            return "Link expired", 403
        
        # Log download
        get_sb().table("pdf_downloads").insert({
            "token": token,
            "tracking_id": info.get("tracking_id"),
            "deal_id": info.get("deal_id"),
            "business_name": info.get("business_name"), 
            "lender_name": info.get("lender_name"), 
            "ip": request.headers.get("X-Forwarded-For", request.remote_addr),
            "user_agent": request.headers.get("User-Agent", "")[:500],
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
        }).execute()
        
        # Serve as attachment (triggers download)
        return send_file(
            info["pdf_path"],
            mimetype="application/pdf",
            as_attachment=True,  # <-- key difference
            download_name=info.get("filename", "document.pdf")
        )
        
    except Exception as e:
        log.exception(f"Failed to download PDF: {token}")
        abort(500)
"""
@bp.get("/v/<secure_token>")
def view_secure_pdf(secure_token):
    """View PDF inline - proxies through backend for tracking"""
    try:
        
        resp = sb.table("pdf_links").select("*").eq("token", secure_token).execute()
        rows = resp.data or []
        if not rows:
            return "Invalid or expired link.", 404

        row = rows[0]

        
        expires_at = datetime.fromisoformat(row["expires_at"].replace("Z", "+00:00"))
        if datetime.now(timezone.utc) > expires_at:
            return "This secure document link has expired.", 410

       
        storage_path = row["pdf_path"]

        
        signed = sb.storage.from_("secure-pdfs").create_signed_url(
            storage_path,
            60  
        )
        signed_url = signed.get("signedURL") or signed.get("signedUrl")
        
        if not signed_url:
            log.error(f"Failed to get signed URL for {storage_path}")
            return "Failed to retrieve document.", 500

        
        pdf_response = requests.get(signed_url)
        if pdf_response.status_code != 200:
            log.error(f"Failed to fetch PDF: {pdf_response.status_code}")
            return "Failed to retrieve document.", 500
        
        pdf_bytes = pdf_response.content

      
        try:
            sb.table("pdf_views").insert({
                "token": secure_token,
                "tracking_id": row.get("tracking_id"),
                "deal_id": row.get("deal_id"),
                "lender_name": row.get("lender_name"),
                "recipient_email": row.get("recipient_email"),
                "ip": request.headers.get("X-Forwarded-For", request.remote_addr),
                "user_agent": request.headers.get("User-Agent", "")[:500],
                "viewed_at": datetime.now(timezone.utc).isoformat(),
            }).execute()
            log.info(f"📊 Logged view: {row.get('lender_name')} - {row.get('recipient_email')}")
        except Exception as e:
            log.warning(f"Failed to log view: {e}")

        
        try:
            sb.table("pdf_links").update({
                "last_accessed_at": datetime.now(timezone.utc).isoformat(),
                "access_count": (row.get("access_count") or 0) + 1
            }).eq("token", secure_token).execute()
        except:
            pass

        
        return send_file(
            BytesIO(pdf_bytes),
            mimetype="application/pdf",
            as_attachment=False
        )

    except Exception as e:
        log.exception("view_secure_pdf failed")
        return f"Error: {str(e)}", 500


@bp.get("/download/<token>")
def download_pdf(token: str):
    """Download PDF as attachment - proxies through backend for tracking"""
    try:
        
        result = sb.table("pdf_links").select("*").eq("token", token).execute()
        rows = result.data or []
        if not rows:
            abort(404)
        
        info = rows[0]
        
        
        expires = datetime.fromisoformat(info["expires_at"].replace("Z", "+00:00"))
        if datetime.now(timezone.utc) > expires:
            return "Link expired", 403
        
        
        storage_path = info["pdf_path"]

       
        signed = sb.storage.from_("secure-pdfs").create_signed_url(
            storage_path,
            60  
        )
        signed_url = signed.get("signedURL") or signed.get("signedUrl")
        
        if not signed_url:
            log.error(f"Failed to get signed URL for {storage_path}")
            abort(500)

        
        pdf_response = requests.get(signed_url)
        if pdf_response.status_code != 200:
            log.error(f"Failed to fetch PDF: {pdf_response.status_code}")
            abort(500)
        
        pdf_bytes = pdf_response.content

      
        try:
            sb.table("pdf_downloads").insert({
                "token": token,
                "tracking_id": info.get("tracking_id"),
                "deal_id": info.get("deal_id"),
                "lender_name": info.get("lender_name"),
                "recipient_email": info.get("recipient_email"),
                "ip": request.headers.get("X-Forwarded-For", request.remote_addr),
                "user_agent": request.headers.get("User-Agent", "")[:500],
                "downloaded_at": datetime.now(timezone.utc).isoformat(),
            }).execute()
            log.info(f"📥 Logged download: {info.get('lender_name')} - {info.get('recipient_email')}")
        except Exception as e:
            log.warning(f"Failed to log download: {e}")

        
        try:
            sb.table("pdf_links").update({
                "last_accessed_at": datetime.now(timezone.utc).isoformat(),
                "access_count": (info.get("access_count") or 0) + 1
            }).eq("token", token).execute()
        except:
            pass
        
        
        filename = info.get("filename") or f"{info.get('lender_name', 'document')}.pdf"
        return send_file(
            BytesIO(pdf_bytes),
            mimetype="application/pdf",
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        log.exception(f"Failed to download PDF: {token}")
        abort(500)


@bp.get("/analytics/downloads/<tracking_id>")
def download_analytics(tracking_id):
    rows = supabase.table("pdf_downloads").select("*").eq("tracking_id", tracking_id).order("downloaded_at").execute().data or []
    return jsonify({
        "tracking_id": tracking_id,
        "total_downloads": len(rows),
        "unique_ips": len({r.get("ip") for r in rows}),
        "downloads": rows
    })

@bp.route("/api/feedback/reply", methods=["POST"])
def feedback_reply():
    """
    Handle inline replies from the Feedback Hub.
    FormData fields:
      - deal_id
      - lender_name
      - status_context  ("approved" | "stips" | "declined")
      - to              (single or comma-separated; UI allows multiple)
      - body
      - files[]         (0..n attachments)
    """
    try:
        deal_id = request.form.get("deal_id")
        lender_name = request.form.get("lender_name")
        status_context = request.form.get("status_context")
        to_raw = request.form.get("to", "")
        body = request.form.get("body", "")

        # Allow multiple addresses separated by comma/semicolon/space
        to_addresses = [
            addr.strip()
            for chunk in to_raw.replace(";", ",").split(",")
            for addr in [chunk.strip()]
            if addr
        ]

        files = request.files.getlist("files")

        app.logger.info(
            "📩 /api/feedback/reply: deal=%s lender=%s status=%s to=%s files=%s",
            deal_id,
            lender_name,
            status_context,
            to_addresses,
            [f.filename for f in files],
        )

        # TODO: validate inputs
        if not deal_id or not lender_name or not to_addresses or not body:
            return jsonify({"error": "Missing required fields"}), 400

        # TODO: send email using your existing Gmail/Graph helper
        # send_lender_reply_email(
        #     deal_id=deal_id,
        #     lender_name=lender_name,
        #     to_addresses=to_addresses,
        #     body=body,
        #     attachments=files,
        #     status_context=status_context,
        # )

        # TODO: log this reply in Supabase (email_responses / manual_review / activity)
        # log_feedback_reply(
        #     deal_id=deal_id,
        #     lender_name=lender_name,
        #     status_context=status_context,
        #     to_addresses=to_addresses,
        #     body=body,
        #     attachment_names=[f.filename for f in files],
        # )

        return jsonify({"ok": True}), 200

    except Exception as e:
        app.logger.exception("❌ /api/feedback/reply failed: %s", e)
        return jsonify({"error": str(e)}), 500