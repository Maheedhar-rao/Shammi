#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from dataclasses import asdict
from inspect import signature
from flask import Blueprint, jsonify, request, session
from werkzeug.utils import secure_filename


from flask import session, send_file, Response
from pathlib import Path
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
import re
import sqlite3
import time
import traceback
import requests
from datetime import datetime, timezone
import time as _time
import secrets

# ---------- Paths & setup ----------
BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
#USERDATA_DIR = BASE_DIR / "userdata"
#UPLOAD_DIR.mkdir(exist_ok=True)
EMAILS_BOOK_DIR = BASE_DIR / "emails-books"
EMAILS_BOOK_DIR.mkdir(exist_ok=True)

EMAILS_JSON_PATH = Path(os.environ.get("LENDER_EMAILS_PATH", str(BASE_DIR / "emails.json")))
AUTH_STORE_PATH = Path(os.environ.get("AUTH_STORE_PATH", str(BASE_DIR / "auth_store.json")))
DEALS_DB_PATH = Path(os.environ.get("DEALS_DB_PATH", str(BASE_DIR / "deals.db")))
TOKENS_DIR = BASE_DIR / "tokens"
GOOGLE_TOKEN_FILE = TOKENS_DIR / "google.json"

bp = Blueprint("underwrite", __name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("underwrite")

# ---------- Import your modules ----------
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

# =========================================================
# SQLite
# =========================================================
def _db():
    conn = sqlite3.connect(str(DEALS_DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn

def _init_db():
    conn = _db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS deals (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id TEXT,
          sender_email TEXT,
          subject TEXT,
          body TEXT,
          mode TEXT,
          created_at INTEGER,
          application_json TEXT,
          statements_json TEXT,
          attachments_json TEXT,
          resend_count INTEGER DEFAULT 0
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS deliveries (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          deal_id INTEGER,
          lender_name TEXT,
          to_email TEXT,
          cc_csv TEXT,
          provider TEXT,
          provider_msg_id TEXT,
          status TEXT,
          created_at INTEGER,
          FOREIGN KEY(deal_id) REFERENCES deals(id)
        )
    """)
    conn.commit()
    conn.close()

_init_db()

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
    conn = _db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO deals (user_id, sender_email, subject, body, mode, created_at, application_json, statements_json, attachments_json) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            user_id,
            sender_email,
            subject,
            body,
            (mode or ""),
            int(time.time()),
            json.dumps(application_json or {}),
            json.dumps(statements_json or {}),
            json.dumps(attachments_json or {}),
        ),
    )
    deal_id = cur.lastrowid
    conn.commit()
    conn.close()
    return deal_id

def record_delivery(
    deal_id: int,
    lender_name: str,
    to_email: str,
    cc_list: List[str],
    provider: str,
    provider_msg_id: Optional[str],
    status: str,
) -> int:
    conn = _db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO deliveries (deal_id, lender_name, to_email, cc_csv, provider, provider_msg_id, status, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (deal_id, lender_name, to_email or "", ",".join(cc_list or []), provider, provider_msg_id or "", status, int(time.time())),
    )
    rid = cur.lastrowid
    conn.commit()
    conn.close()
    return rid

# =========================================================
# Helpers
# =========================================================
def _unique_name(base: str) -> str:
    return f"{int(time.time())}_{secrets.token_hex(3)}_{secure_filename(base)}"

def _save_upload(fs_obj, prefix: str = "") -> dict:
    """Save a FileStorage to uploads/ and return dict(path, filename)."""
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

# =========================================================
# Application extraction
# =========================================================
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

# =========================================================
# Statements extraction & aggregation
# =========================================================
def _summarize_one_statement_from_bytes(pdf_bytes: bytes, filename: Optional[str]) -> Dict:
    if stx is None:
        raise RuntimeError("Statements_extractor not available")
    summary, daily, txns = stx.summarize_statement_from_bytes(pdf_bytes, filename=filename)
    summary_dict = asdict(summary) if hasattr(summary, "__dataclass_fields__") else dict(summary)
    debit_counts, credit_counts, monthly_deposits = stx.compute_monthly_counts_and_deposits(txns)  # type: ignore
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

# =========================================================
# Lender matching
# =========================================================
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

# =========================================================
# Emails book + recipients
# =========================================================
_emails_cache = {"book": {}, "mtime": 0.0}

def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

def _parse_row_emails(s: str) -> List[str]:
    return [e.strip() for e in str(s or "").split(",") if e and e.strip()]

def load_emails_book() -> Dict[str, List[str]]:
    try:
        mtime = EMAILS_JSON_PATH.stat().st_mtime
    except Exception:
        _emails_cache["book"], _emails_cache["mtime"] = {}, 0.0
        return {}
    if mtime == _emails_cache["mtime"]:
        return _emails_cache["book"]
    try:
        with EMAILS_JSON_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f) or {}
    except Exception:
        log.exception("Failed reading %s", EMAILS_JSON_PATH)
        _emails_cache["book"], _emails_cache["mtime"] = {}, 0.0
        return {}
    book: Dict[str, List[str]] = {}
    rows = data.get("emails") if isinstance(data, dict) else None
    if isinstance(rows, list):
        for row in rows:
            name = (row or {}).get("business_name")
            emails_str = (row or {}).get("email") or (row or {}).get("emails") or ""
            if not name:
                continue
            key = _norm(name)
            addrs = _parse_row_emails(emails_str)
            if addrs:
                book[key] = addrs
    aliases = {"broadwayadvance": "Broadway Advance","gg": "G&G","gandg": "G&G","tmrnow": "TMR Now","501advance": "501 Advance","quikstone": "Quikstone","quickstone": "Quikstone"}
    for alias, canonical in aliases.items():
        akey, ckey = _norm(alias), _norm(canonical)
        if ckey in book and akey not in book:
            book[akey] = book[ckey]
    _emails_cache["book"], _emails_cache["mtime"] = book, mtime
    return book

def load_emails_book() -> dict:
    """
    Returns a dict keyed by normalized lender name -> {"to": str, "cc": [str, ...]}.

    Priority:
      1) Per-user CSV at emails-books/<email>.csv (columns: lender,to,cc)
      2) Global JSON at EMAILS_JSON_PATH (flexible shape)
    """
    # 1) Per-user CSV
    try:
        p = _user_emails_csv_path()
        if p and p.exists():
            out = {}
            with p.open("r", encoding="utf-8-sig", newline="") as fh:
                reader = csv.DictReader(fh)
                # normalize column names
                for row in reader:
                    r = { (k or "").strip().lower(): (v or "").strip() for k,v in (row or {}).items() }
                    name = r.get("lender") or r.get("name") or r.get("business") or ""
                    if not name:
                        continue
                    to_ = r.get("to") or r.get("email") or ""
                    cc_ = r.get("cc") or r.get("ccs") or ""
                    # split addresses by comma/semicolon
                    def split_emails(s):
                        return [e.strip() for e in re.split(r"[;,]", s or "") if e.strip()]
                    cc_list = split_emails(cc_)
                    # If "to" has multiple addresses, pick the first as primary
                    to_addr = split_emails(to_)[0] if split_emails(to_) else ""
                    out[name.strip().lower()] = {"to": to_addr, "cc": cc_list}
            return out
    except Exception:
        traceback.print_exc()  # fall through to global json

    # 2) Global JSON fallback
    m = {}
    try:
        if EMAILS_JSON_PATH and EMAILS_JSON_PATH.exists():
            raw = json.loads(EMAILS_JSON_PATH.read_text() or "{}")
            if isinstance(raw, dict):
                # could be {"LenderA": {"to": "...", "cc": [...]}, ...} or {"LenderA":"x@y.com"}
                for k,v in raw.items():
                    k2 = (k or "").strip().lower()
                    to_, cc_list = "", []
                    if isinstance(v, str):
                        to_ = v
                    elif isinstance(v, dict):
                        to_ = v.get("to") or v.get("email") or ""
                        cc = v.get("cc") or []
                        if isinstance(cc, str):
                            cc_list = [e.strip() for e in re.split(r"[;,]", cc) if e.strip()]
                        elif isinstance(cc, (list, tuple)):
                            cc_list = [str(e).strip() for e in cc if str(e).strip()]
                    m[k2] = {"to": to_, "cc": cc_list}
            elif isinstance(raw, list):
                # list of objects: [{"lender":"...","to":"...","cc":"..."}, ...]
                for item in raw:
                    if not isinstance(item, dict): 
                        continue
                    name = (item.get("lender") or item.get("name") or "").strip().lower()
                    if not name: 
                        continue
                    to_ = item.get("to") or item.get("email") or ""
                    cc = item.get("cc") or []
                    if isinstance(cc, str):
                        cc_list = [e.strip() for e in re.split(r"[;,]", cc) if e.strip()]
                    elif isinstance(cc, (list, tuple)):
                        cc_list = [str(e).strip() for e in cc if str(e).strip()]
                    else:
                        cc_list = []
                    m[name] = {"to": to_, "cc": cc_list}
    except Exception:
        traceback.print_exc()
    return m


def resolve_recipients(lender_name: str, extra_cc: Optional[List[str]] = None) -> Tuple[Optional[str], List[str]]:
    book = load_emails_book()
    key = _norm(lender_name)
    addrs = book.get(key)
    if not addrs:
        for k, v in book.items():
            if key in k or k in key:
                addrs = v
                break
    addrs = addrs or []
    to_email = addrs[0] if addrs else None
    cc_list = (addrs[1:] if len(addrs) > 1 else []) + (extra_cc or [])
    return to_email, cc_list

# =========================================================
# Connected sender (gmail/graph)
# =========================================================
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

# =========================================================
# Email sending helpers
# =========================================================
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

# =========================================================
# Routes
# =========================================================
@bp.post("/extract-application")
def extract_application_only():
    try:
        app_file = request.files.get("application")
        if not app_file:
            return jsonify({"error": "Missing application PDF"}), 400
        saved = _save_upload(app_file, prefix="application")
        application = _extract_application_fields(Path(saved["path"]))
        # attach the saved file path so the front-end can include it later
        application["_attachment"] = saved
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

        stmt_files = request.files.getlist("statements")
        min_files = 4 if state in {"NY", "CA"} else 3
        if not stmt_files or len(stmt_files) < min_files:
            return jsonify({"error": f"Need at least {min_files} statement PDFs for state {state}"}), 400

        # Save the uploaded statements; also build data for analysis
        saved_files: List[dict] = []
        files_for_summary: List[Tuple[str, bytes]] = []
        for f in stmt_files:
            saved = _save_upload(f, prefix="stmt")
            saved_files.append(saved)
            # read back for summary
            with open(saved["path"], "rb") as fh:
                files_for_summary.append((saved["filename"], fh.read()))

        # Combine with any previous statements if provided (resend merge)
        prev_json = request.form.get("existing_statements_json")
        prev = None
        if prev_json:
            try:
                prev = json.loads(prev_json) or {}
            except Exception:
                prev = None

        statements_new = _build_statements_payload(files_for_summary, state_for_rule=state)
        statements_new["_saved_files"] = saved_files

        if prev and isinstance(prev, dict):
            combined_per = (prev.get("per_statement") or []) + (statements_new.get("per_statement") or [])
            # merge saved files too
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

@bp.post("/send")
def send_emails():
    """
    json: {
      "selected_lenders":[...],
      "subject":"...",
      "message":"...",
      "cc":[...],
      "mode":"MCA|CCS|REV",
      "application": {...},
      "statements": {...},
      "attachments": [ {name, data(base64 or dataURL)}, ... ],                 # optional (global fallback)
      "per_lender_attachments": { "LenderA":[...], "LenderB":[...] } | [...],  # optional (preferred)
      "parent_deal_id": 123,    # optional
      "exclude": ["LenderA"]    # optional
    }
    """
    try:
        data = request.get_json(force=True) or {}
        selected = data.get("selected_lenders") or []
        subject  = data.get("subject") or ""        # may be blank or contain #DealID
        body     = data.get("message") or ""
        user_cc  = data.get("cc") or []
        mode     = data.get("mode") or ""

        application_obj = data.get("application") or {}
        statements_obj  = data.get("statements") or {}

        # Global attachments (fallback for lenders without their own)
        attachments_global = _parse_attachments_from_json(data)
        # Per-lender attachments
        per_map = _parse_per_lender_attachments_from_json(data)

        if isinstance(user_cc, str):
            user_cc = [x.strip() for x in user_cc.split(",") if x.strip()]

        if not selected:
            return jsonify({"error": "No lenders selected"}), 400

        sender_email, provider, token = safe_get_connected_sender()
        if not sender_email or not token:
            return jsonify({"error": "No connected mailbox. Connect Gmail/Outlook first."}), 403

        # Create the deal first (to get deal_id) and persist artifacts
        deal_id = record_deal(
            session.get("google_email") or "demo-user",
            sender_email, subject, body, mode,
            application_json=application_obj,
            statements_json=statements_obj
        )

        # Compute final subject (default or #DealID replacement), and persist if changed
        final_subject = _default_subject(application_obj.get("business_name"), deal_id, subject)
        if final_subject != subject:
            try:
                conn = _db()
                conn.execute("UPDATE deals SET subject=? WHERE id=?", (final_subject, deal_id))
                conn.commit()
            except Exception:
                pass
            finally:
                try: conn.close()
                except: pass

        # Resend de-dupe
        exclude = set([(s or "").strip().lower() for s in (data.get("exclude") or []) if s])
        parent_deal_id = data.get("parent_deal_id")
        if parent_deal_id and not exclude:
            try:
                conn = _db()
                rows = conn.execute(
                    "SELECT DISTINCT lender_name FROM deliveries WHERE deal_id=?",
                    (parent_deal_id,)
                ).fetchall()
                exclude = set([(r["lender_name"] or "").strip().lower() for r in rows])
                # ensure resend_count column and bump it
                try:
                    conn.execute("ALTER TABLE deals ADD COLUMN resend_count INTEGER DEFAULT 0")
                except Exception:
                    pass
                try:
                    conn.execute("UPDATE deals SET resend_count = COALESCE(resend_count,0)+1 WHERE id=?", (parent_deal_id,))
                    conn.commit()
                except Exception:
                    pass
            except Exception:
                pass
            finally:
                try: conn.close()
                except: pass

        deliveries = []
        for lender in selected:
            lname = (lender or "").strip()
            lname_key = lname.lower()

            if lname_key in exclude:
                deliveries.append({"lender": lname, "from": sender_email, "status": "skipped", "reason": "already sent"})
                record_delivery(deal_id, lname, "", user_cc, provider or "", None, "skipped")
                continue

            # choose attachments for this lender
            attachments_for_this = per_map.get(lname) or per_map.get(lname_key) or attachments_global

            to_email, cc_list = resolve_recipients_user_csv_first(lname, user_cc)
            if not to_email:
                deliveries.append({"lender": lname, "from": sender_email, "status": "skipped",
                                   "reason": "No recipient email in emails.json"})
                record_delivery(deal_id, lname, "", user_cc, provider or "", None, "skipped")
                continue

            ok, provider_id = False, None
            if (provider or "").lower() == "gmail":
                ok, provider_id = gmail_send(token, final_subject, body, sender_email, to_email, cc_list, attachments=attachments_for_this)
            elif (provider or "").lower() in ("outlook", "graph"):
                ok, provider_id = graph_send(token, final_subject, body, sender_email, to_email, cc_list, attachments=attachments_for_this)
            else:
                deliveries.append({"lender": lname, "from": sender_email, "to": to_email, "cc": cc_list,
                                   "status": "error", "reason": f"Unsupported provider {provider}"})
                record_delivery(deal_id, lname, to_email, cc_list, provider or "", None, "error")
                continue

            status = "sent" if ok else "error"
            deliveries.append({
                "lender": lname, "from": sender_email, "to": to_email, "cc": cc_list,
                "status": status, "provider": (provider or ""), "provider_id": provider_id
            })
            record_delivery(deal_id, lname, to_email, cc_list, provider or "", provider_id, status)

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

# --- Dashboard APIs -----------------------------------------------------------
@bp.get("/deals")
def list_deals():
    try:
        sess_email = session.get("google_email") or request.headers.get("X-User-Email")
        limit = max(1, int(request.args.get("limit", 50)))
        offset = max(0, int(request.args.get("offset", 0)))

        conn = _db()
        cur = conn.cursor()
        select_sql = """
            SELECT d.id, d.user_id, d.sender_email, d.subject, d.mode, d.created_at,
                   d.application_json,
                   COALESCE(SUM(CASE WHEN del.status='sent'    THEN 1 ELSE 0 END),0) AS sent_count,
                   COALESCE(SUM(CASE WHEN del.status='error'   THEN 1 ELSE 0 END),0) AS error_count,
                   COALESCE(SUM(CASE WHEN del.status='skipped' THEN 1 ELSE 0 END),0) AS skipped_count,
                   COALESCE(COUNT(del.id),0) AS total_count,
                   CASE WHEN d.application_json IS NOT NULL AND TRIM(d.application_json) <> '' AND TRIM(d.application_json) <> '{}' THEN 1 ELSE 0 END AS has_app,
                   CASE WHEN d.statements_json IS NOT NULL  AND TRIM(d.statements_json)  <> '' AND TRIM(d.statements_json)  <> '{}' THEN 1 ELSE 0 END AS has_stmts,
                   COALESCE(d.resend_count, 0) AS resend_count
            FROM deals d
            LEFT JOIN deliveries del ON del.deal_id = d.id
        """
        if sess_email:
            cur.execute(
                select_sql + """
                WHERE d.user_id = ? OR d.sender_email = ?
                GROUP BY d.id
                ORDER BY d.id DESC
                LIMIT ? OFFSET ?
                """,
                (sess_email, sess_email, limit, offset),
            )
        else:
            cur.execute(
                select_sql + """
                GROUP BY d.id
                ORDER BY d.id DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            )

        rows = cur.fetchall()
        conn.close()

        deals = []
        for r in rows:
            app_json = {}
            try:
                app_json = json.loads(r["application_json"] or "{}")
            except Exception:
                app_json = {}
            deals.append({
                "id": r["id"],
                "user_id": r["user_id"],
                "sender_email": r["sender_email"],
                "subject": r["subject"],
                "business_name": (app_json or {}).get("business_name"),
                "mode": r["mode"],
                "created_at": r["created_at"],
                "sent_count": r["sent_count"],
                "error_count": r["error_count"],
                "skipped_count": r["skipped_count"],
                "total_count": r["total_count"],
                "has_app": r["has_app"],
                "has_stmts": r["has_stmts"],
                "resend_count": r["resend_count"],
            })
        return jsonify({"deals": deals})
    except Exception as e:
        log.exception("list_deals failed: %s", e)
        return jsonify({"error": str(e)}), 500

@bp.get("/deals/<int:deal_id>/deliveries")
def list_deliveries(deal_id: int):
    try:
        conn = _db()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, lender_name, to_email, cc_csv, provider, provider_msg_id, status, created_at
              FROM deliveries
             WHERE deal_id = ?
             ORDER BY id ASC
            """,
            (deal_id,),
        )
        rows = cur.fetchall()
        conn.close()

        deliveries = []
        for r in rows:
            deliveries.append({
                "id": r["id"],
                "lender": r["lender_name"],
                "to": r["to_email"],
                "cc": [s for s in (r["cc_csv"] or "").split(",") if s],
                "provider": r["provider"],
                "provider_id": r["provider_msg_id"],
                "status": r["status"],
                "created_at": r["created_at"],
            })
        return jsonify({"deliveries": deliveries})
    except Exception as e:
        log.exception("list_deliveries failed: %s", e)
        return jsonify({"error": str(e)}), 500

@bp.get("/deal/<int:deal_id>")
def get_deal(deal_id: int):
    conn = _db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM deals WHERE id=?", (deal_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "not found"}), 404

    deal = dict(row)
    try:
        deal["application"] = json.loads(deal.get("application_json") or "{}")
    except Exception:
        deal["application"] = {}
    try:
        deal["statements"]  = json.loads(deal.get("statements_json") or "{}")
    except Exception:
        deal["statements"] = {}
    try:
        deal["attachments"] = json.loads(deal.get("attachments_json") or "{}")
    except Exception:
        deal["attachments"] = {}

    cur.execute("""SELECT lender_name as lender, to_email, cc_csv, provider, provider_msg_id, status, created_at
                   FROM deliveries WHERE deal_id=? ORDER BY created_at ASC""", (deal_id,))
    deal["deliveries"] = [dict(r) for r in cur.fetchall()]

    conn.close()
    for k in ("application_json","statements_json","attachments_json"):
        deal.pop(k, None)
    return jsonify({"deal": deal})

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
# ---- attachments helper (JSON -> [(filename, bytes), ...]) ----
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


# ---- per-lender attachments: accepts dict or list ----
def _parse_per_lender_attachments_from_json(data: dict) -> Dict[str, List[Tuple[str, bytes]]]:
    """
    Accepts either:
      "per_lender_attachments": {
         "Spartan":[{"name":"app.pdf","data":"data:application/pdf;base64,..."}, ...],
         "Legendary":[...]
      }
    OR
      "per_lender_attachments": [
         {"lender":"Spartan","attachments":[...]} , {"lender":"Legendary","attachments":[...]}
      ]
    Returns mapping with both exact and lower-case keys for convenience.
    """
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
        out[lender.lower()] = files  # case-insensitive lookup

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

    # ---- Subject defaults --------------------------------------------------------
SUBJECT_PREFIX = "New Submission - Harvest Lending/Pathway Catalyst"

def _default_subject(biz: Optional[str], deal_id: int, user_subject: Optional[str]) -> str:
    """
    If user_subject is empty, build:
      'New Submission - Harvest Lending/Pathway Catalyst - <Business> - #<deal_id>'
    If user_subject contains the token '#DealID', replace it with '#<deal_id>'.
    """
    biz_name = (biz or "").strip() or "Unknown Business"
    template = f"{SUBJECT_PREFIX} - {biz_name} - #{deal_id}"
    s = (user_subject or "").strip()
    if not s:
        return template
    return s.replace("#DealID", f"#{deal_id}")

# --- ADD: user + emails.csv helpers ---

def _parse_emails_csv_bytes(raw: bytes) -> dict:
    """
    CSV columns (case-insensitive):
      lender,to,cc
    'to' can contain multiple addresses (comma/semicolon separated).
    We use the first 'to' as the To: address; remaining 'to' entries merge into CC.
    'cc' also supports comma/semicolon separators.

    Returns mapping:
      { lender_lower: { "to": [..], "cc": [..] } }
    """
    book = {}
    text = raw.decode("utf-8", errors="ignore")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        return {}

    # normalize header access
    headers = { (h or "").strip().lower(): h for h in reader.fieldnames }
    def _get(row, key):
        return (row.get(headers.get(key, key), "") or "").strip()

    def _split_emails(s):
        parts = re.split(r"[;,]", s or "")
        return [p.strip() for p in parts if p.strip()]

    for row in reader:
        lender = _get(row, "lender")
        if not lender:
            continue
        to_list = _split_emails(_get(row, "to"))
        cc_list = _split_emails(_get(row, "cc"))
        book[lender.strip().lower()] = {"to": to_list, "cc": cc_list}
    return book

def _load_emails_book(email: str) -> tuple[dict, int]:
    p = _emails_csv_path(email)
    if not p.exists():
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
# --- ADD: prefer per-user emails.csv; fallback to legacy resolver (if present) ---
def resolve_recipients_user_csv_first(lender_name: str, user_cc: list[str]):
    uid = _current_user_email()
    lname_key = (lender_name or "").strip().lower()

    to_email = ""
    cc_list: list[str] = []
    if uid:
        book, _ = _load_emails_book(uid)
        if lname_key in book:
            to_list = list(book[lname_key].get("to") or [])
            cc_from_csv = list(book[lname_key].get("cc") or [])

            if to_list:
                to_email = to_list[0]              # primary To
                cc_list.extend(to_list[1:])        # rest of 'to' merge into CC
            cc_list.extend(cc_from_csv)

    # merge UI-provided CC
    cc_list.extend(user_cc or [])

    # always CC the signed-in user (deduped later)
    if uid:
        cc_list.append(uid)

    # Fallback: use your legacy mapping if still no To:
    if not to_email:
        try:
            legacy_to, legacy_cc = resolve_recipients(lender_name, user_cc)  # your existing function
            to_email = to_email or (legacy_to or "")
            cc_list.extend(legacy_cc or [])
        except Exception:
            pass

    return (to_email or "").strip(), _dedupe_emails(cc_list)

def _user_email() -> str:
    return (session.get("google_email") or session.get("user_email") or "").strip().lower()

def _safe_email_to_fname(email: str) -> str:
    return re.sub(r"[^a-z0-9_.-]+", "_", (email or "").lower())

def _user_emails_csv_path(user: Optional[str] = None) -> Optional[Path]:
    user = (user or _user_email() or "").strip().lower()
    if not user:
        return None
    return EMAILS_BOOK_DIR / f"{_safe_email_to_fname(user)}.csv"

@bp.get("/emails-book/status")
def emails_book_status():
    email = (session.get("user_email") or session.get("google_email") or "").strip().lower()
    if not email:
        # anonymous session: report not uploaded
        return jsonify({"ok": True, "count": 0, "source": "csv"}), 200

    path = EMAILS_DIR / f"{_safe_email(email)}.csv"
    if not path.exists():
        return jsonify({"ok": True, "count": 0, "source": "csv"}), 200

    # count non-empty data rows (skip header)
    count = 0
    try:
        with path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh)
            header = next(reader, None)
            for row in reader:
                if any((c or "").strip() for c in row):
                    count += 1
    except Exception:
        # if unreadable, treat as not uploaded
        return jsonify({"ok": True, "count": 0, "source": "csv"}), 200

    mtime = int(path.stat().st_mtime)
    resp = jsonify({
        "ok": True,
        "count": count,
        "filename": path.name,
        "updated_at": mtime,
        "source": "csv",
    })
    resp.headers["Cache-Control"] = "no-store"
    return resp

@bp.post("/emails-book/upload")
def emails_book_upload():
    user = session.get("google_email") or session.get("user_email") or "anonymous"
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "missing_file"}), 400

    raw = f.read()  # bytes
    text = raw.decode("utf-8-sig", errors="replace")

    # OPTIONAL: validate headers up front (reuse your aliases)
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        return jsonify({"error": "empty_csv"}), 400

    # Save the CSV to the user-scoped path you already use
    path = _user_emails_csv_path(user)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")

    # Compute a count for the UI
    cnt, sample = _preview_emails_csv_text(text)

    return jsonify({
        "ok": True,
        "filename": path.name,
        "bytes": len(text.encode("utf-8")),
        "count": cnt,
        "sample": sample,   # optional: first few lender names
    })


LENDER_ALIASES = {"lender","lender name","name","business","company","funder","provider"}
TO_ALIASES     = {"to","email","primary","primary_email"}

def _split_emails(s: str):
    return [e.strip() for e in re.split(r"[;,]", s or "") if e.strip()]

def _preview_emails_csv_text(text: str):
    """
    Returns (count, sample_names) where count is the number of rows with a lender + to-address.
    Accepts BOM, strips 'sep=,' header lines, normalizes headers.
    """
    # strip BOM and blank lines
    text = text.replace("\r\n", "\n")
    lines = [ln for ln in text.split("\n") if ln.strip()]

    # some Excel exports start with 'sep=,' on first line
    if lines and lines[0].lower().startswith("sep="):
        lines = lines[1:]

    if not lines:
        return 0, []

    reader = csv.DictReader(lines)
    if not reader.fieldnames:
        return 0, []

    fields = [(c or "").strip().lower().lstrip("\ufeff") for c in reader.fieldnames]

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

