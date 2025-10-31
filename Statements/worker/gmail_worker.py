# -*- coding: utf-8 -*-
from __future__ import annotations
import os, json, time, re, base64, requests
from pathlib import Path
from typing import Optional, Dict, List, Tuple
import sqlite3
# ---------- Paths ----------
BASE_DIR = Path(__file__).resolve().parents[1]           # -> Statements/
TOKENS_DIR = BASE_DIR / "tokens"
DB_PATH = Path(os.environ.get("DEALS_DB_PATH", str(BASE_DIR / "deals.db")))
EMAILS_JSON_PATH = BASE_DIR / "emails.json"              # for lender name mapping (optional)

# ---------- Tiny token helper (compatible with auth_google.py output) ----------
def _read_token(email: str) -> dict | None:
    p = TOKENS_DIR / f"{email}.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None

def _ensure_access_token(td: dict) -> Optional[str]:
    # td follows google Credentials.to_json format (contains refresh_token, client_id, client_secret, token/expiry)
    tok = td.get("token") or td.get("access_token")
    exp = td.get("expiry")
    needs = False
    if not tok:
        needs = True
    else:
        # expire a bit early
        try:
            # stored as RFC3339 string; we’ll refresh if within 60s
            import datetime
            dt = datetime.datetime.fromisoformat(exp.replace("Z","+00:00")) if isinstance(exp,str) else None
            if dt:
                if (dt - datetime.datetime.now(datetime.timezone.utc)).total_seconds() < 60:
                    needs = True
        except Exception:
            needs = True
    if not needs:
        return tok

    # refresh
    rt = td.get("refresh_token")
    cid = td.get("client_id"); csec = td.get("client_secret")
    token_uri = td.get("token_uri") or "https://oauth2.googleapis.com/token"
    if not (rt and cid and csec):
        return None
    try:
        r = requests.post(token_uri, data={
            "grant_type": "refresh_token",
            "refresh_token": rt,
            "client_id": cid,
            "client_secret": csec,
        }, timeout=15)
        r.raise_for_status()
        js = r.json()
        access = js.get("access_token")
        if not access:
            return None
        td["token"] = access
        if "expires_in" in js:
            td["expiry"] = None  # not strictly needed for this loop
        # persist back to disk
        p = TOKENS_DIR / f"{td.get('email') or 'account'}.json"
        try: p.write_text(json.dumps(td, indent=2))
        except Exception: pass
        return access
    except Exception:
        return None

# ---------- Gmail REST ----------
class Gmail:
    def __init__(self, email: str):
        self.email = email
        self.token_doc = _read_token(email)

    def _hdrs(self) -> dict:
        tok = _ensure_access_token(self.token_doc or {})
        if not tok:
            raise RuntimeError("No valid Gmail token; reconnect Gmail.")
        return {"Authorization": f"Bearer {tok}"}

    def get_message(self, msg_id: str, fmt: str = "metadata", headers: List[str] | None = None) -> dict:
        params = {"format": fmt}
        if headers:
            params["metadataHeaders"] = headers
        r = requests.get(
            f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg_id}",
            headers=self._hdrs(), params=params, timeout=15
        )
        r.raise_for_status()
        return r.json()

    def get_thread(self, thread_id: str) -> dict:
        r = requests.get(
            f"https://gmail.googleapis.com/gmail/v1/users/me/threads/{thread_id}",
            headers=self._hdrs(), timeout=15
        )
        r.raise_for_status()
        return r.json()

    def search(self, q: str, max_results: int = 50) -> List[str]:
        r = requests.get(
            f"https://gmail.googleapis.com/gmail/v1/users/me/messages",
            headers=self._hdrs(),
            params={"q": q, "maxResults": max_results},
            timeout=15
        )
        r.raise_for_status()
        js = r.json() or {}
        return [m["id"] for m in js.get("messages", [])]

# ---------- DB ----------
def db():
    c = sqlite3.connect(str(DB_PATH))
    c.row_factory = sqlite3.Row
    return c

def init_db():
    con = db(); cur = con.cursor()
    # normalized decisions table (idempotent)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS decisions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      deal_id INTEGER,
      lender TEXT,
      status TEXT,            -- APPROVED | DECLINED | STIPS_REQUIRED | OTHER
      reason TEXT,
      offer_json TEXT,
      stips_json TEXT,
      provider TEXT,          -- 'gmail'
      message_id TEXT UNIQUE, -- gmail message id (dedupe)
      thread_id TEXT,
      snippet TEXT,
      updated_at INTEGER
    )
    """)
    # helpful index
    cur.execute("CREATE INDEX IF NOT EXISTS idx_decisions_deal ON decisions(deal_id)")
    con.commit(); con.close()

# ---------- Helpers ----------
def _parse_hdr(hdrs: List[dict], name: str) -> str:
    for h in hdrs or []:
        if (h.get("name") or "").lower() == name.lower():
            return h.get("value") or ""
    return ""

def _extract_email(addr: str) -> str:
    # "Name <x@y.com>" -> x@y.com
    m = re.search(r"<([^>]+)>", addr)
    return (m.group(1) if m else addr).strip().lower()

def _classify(text: str) -> str:
    t = (text or "").lower()
    if re.search(r"\b(approved|clear to fund|ctf|green light)\b", t): return "APPROVED"
    if re.search(r"\b(declined|cannot|won't|pass|not a fit)\b", t):   return "DECLINED"
    if re.search(r"\b(stips|stip|need(ed)?|please provide|missing|docs|documents|more info)\b", t): return "STIPS_REQUIRED"
    return "OTHER"

def _offer_hint(text: str) -> dict:
    t = (text or "").lower()
    amt = None; factor = None
    m = re.search(r"\$?([0-9][0-9,]{3,})", t)
    if m:
        try: amt = int(m.group(1).replace(",", ""))
        except: pass
    m = re.search(r"(factor|buy rate)[^\d]*([1][.]\d{1,2})", t)
    if m:
        factor = float(m.group(2))
    return {"amount": amt, "factor": factor}

def _load_email_book() -> Dict[str, str]:
    """
    Reverse lookup map email -> lender name from emails.json.
    """
    try:
        data = json.loads(EMAILS_JSON_PATH.read_text("utf-8"))
        rows = data.get("emails") or []
        out = {}
        for row in rows:
            name = (row or {}).get("business_name") or ""
            ems  = (row or {}).get("email") or (row or {}).get("emails") or ""
            for e in [s.strip().lower() for s in ems.split(",") if s.strip()]:
                out[e] = name
        return out
    except Exception:
        return {}

def row_as_dict(row):
    if row is None:
        return {}
    if isinstance(row, dict):
        return row
    if isinstance(row, sqlite3.Row):
        return {k: row[k] for k in row.keys()}
    try:
        return dict(row)
    except Exception:
        return {}

# ---------- Watcher ----------
class GmailWatcher:
    def __init__(self, email: str, db_path: str):
        self.email = email
        global DB_PATH
        DB_PATH = Path(db_path)
        init_db()
        self.gmail = Gmail(email)
        self.email_book = _load_email_book()

    def _active_deals(self) -> List[sqlite3.Row]:
        con = db()
        # recent deals for this sender (last 45 days)
        rows = con.execute("""
            SELECT id, subject, sender_email, created_at
              FROM deals
             WHERE sender_email = ?
               AND created_at >= strftime('%s','now') - 45*24*3600
             ORDER BY id DESC
             LIMIT 400
        """, (self.email,)).fetchall()
        con.close()
        return rows

    def _deliveries_for(self, deal_id: int) -> List[sqlite3.Row]:
        con = db()
        rows = con.execute("""
            SELECT lender_name, provider, provider_msg_id, created_at
              FROM deliveries
             WHERE deal_id = ?
             ORDER BY id ASC
        """, (deal_id,)).fetchall()
        con.close()
        return rows

    def _insert_decision(self, deal_id: int, lender: str, status: str, reason: str,
                         offer: dict, stips: dict, provider: str, message_id: str,
                         thread_id: str, snippet: str):
        con = db(); cur = con.cursor()
        try:
            cur.execute("""
                INSERT OR IGNORE INTO decisions
                (deal_id, lender, status, reason, offer_json, stips_json, provider, message_id, thread_id, snippet, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                deal_id, lender, status, reason, json.dumps(offer or {}), json.dumps(stips or {}),
                provider, message_id, thread_id, snippet, int(time.time())
            ))
            con.commit()
        finally:
            con.close()

    def _process_message(self, deal_id: int, lender_hint: str, msg_meta: dict):
        hdrs = msg_meta.get("payload", {}).get("headers", [])
        frm  = _extract_email(_parse_hdr(hdrs, "From"))
        subj = _parse_hdr(hdrs, "Subject")
        msg_id = msg_meta.get("id")
        thread_id = msg_meta.get("threadId")
        snippet = msg_meta.get("snippet") or ""

        # infer lender name
        lender = lender_hint or self.email_book.get(frm) or frm

        text = f"{subj}\n{snippet}"
        status = _classify(text)
        reason = snippet or subj
        offer  = _offer_hint(text)
        stips  = {}

        self._insert_decision(
            deal_id=deal_id, lender=lender, status=status, reason=reason,
            offer=offer, stips=stips, provider="gmail",
            message_id=msg_id, thread_id=thread_id, snippet=snippet
        )

    def tick(self):
        deals = self._active_deals()
        if not deals:
            return

        for d in deals:
            deal_id = d["id"]
            # Strategy 1: subject tag (#DealID)
            try:
                q = f'subject:"#%s" newer_than:45d -from:%s' % (deal_id, self.email)
                for mid in self.gmail.search(q, max_results=20):
                    meta = self.gmail.get_message(mid, fmt="metadata",
                        headers=["Subject","From","To","Date","Message-Id","In-Reply-To","References"])
                    self._process_message(deal_id, lender_hint="", msg_meta=meta)
            except Exception as e:
                print(f"[{self.email}] search-subject #{deal_id} failed: {e}")

            # Strategy 2: follow the original sent thread for each delivery
            for deliv in self._deliveries_for(deal_id):
                if (deliv["provider"] or "").lower() != "gmail":
                    continue
                sent_id = deliv["provider_msg_id"] or ""
                if not sent_id:
                    continue
                try:
                    sent_meta = self.gmail.get_message(sent_id, fmt="metadata",
                        headers=["Subject","From","To","Date","Message-Id"])
                    thread_id = sent_meta.get("threadId")
                    if not thread_id:
                        continue
                    thr = self.gmail.get_thread(thread_id)
                    for m in thr.get("messages", []):
                        # Only messages after we sent, and not from us
                        hdrs = m.get("payload", {}).get("headers", [])
                        frm  = _extract_email(_parse_hdr(hdrs, "From"))
                        if frm == self.email:
                            continue
                        self._process_message(deal_id, lender_hint=deliv["lender_name"] or "", msg_meta=m)
                except Exception as e:
                    print(f"[{self.email}] thread follow for deal {deal_id} failed: {e}")

    def _count_decisions(self, deal_id: int) -> int:
        con = db()
        try:
            row = con.execute(
                "SELECT COUNT(*) AS c FROM decisions WHERE deal_id=?",
                (deal_id,)
            ).fetchone()
            if not row:
                return 0
            # Prefer named -> positional -> dict fallback
            try:
                return int(row["c"])
            except Exception:
                try:
                    return int(row[0])
                except Exception:
                    return int(row_as_dict(row).get("c", 0) or 0)
        except Exception:
            return 0
        finally:
            con.close()
   




    def ingest_deal(self, deal_id: int) -> dict:
        """
        One-off ingest for a specific deal:
          - Subject-tag search (#DealID)
          - Follow original sent thread(s) for each delivery
        Returns: {"processed": N, "added": M}
        """
        before = self._count_decisions(deal_id)
        processed = 0

        # Strategy 1: Subject tag search
        try:
            q = f'subject:"#%s" newer_than:365d -from:%s' % (deal_id, self.email)
            for mid in self.gmail.search(q, max_results=50):
                meta = self.gmail.get_message(
                    mid,
                    fmt="metadata",
                    headers=["Subject","From","To","Date","Message-Id","In-Reply-To","References"]
                )
                self._process_message(deal_id, lender_hint="", msg_meta=meta)
                processed += 1
        except Exception:
            pass

        # Strategy 2: Follow threads of deliveries
        for deliv in self._deliveries_for(deal_id):
            try:
                if (deliv["provider"] or "").lower() != "gmail":
                    continue
                sent_id = deliv["provider_msg_id"] or ""
                if not sent_id:
                    continue

                sent_meta = self.gmail.get_message(
                    sent_id,
                    fmt="metadata",
                    headers=["Subject","From","To","Date","Message-Id"]
                )
                thread_id = sent_meta.get("threadId")
                if not thread_id:
                    continue

                thr = self.gmail.get_thread(thread_id)
                for m in thr.get("messages", []):
                    hdrs = m.get("payload", {}).get("headers", [])
                    frm  = _extract_email(_parse_hdr(hdrs, "From"))
                    if frm == self.email:  # skip our own messages
                        continue
                    self._process_message(deal_id, lender_hint=(deliv["lender_name"] or ""), msg_meta=m)
                    processed += 1
            except Exception:
                # keep going per-delivery
                continue

        after = self._count_decisions(deal_id)
        return {"processed": processed, "added": after - before}

