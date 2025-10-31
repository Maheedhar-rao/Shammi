#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Bank statement extractor (PDF -> summaries + helpers)

Extracts per statement:
- Business Name
- Account Number (masked: ****1234 when possible)
- Bank Name
- Statement Month (YYYY-MM if detectable)
- Debit Count
- Credit Count
- Negative Ending Days
- Average Daily Balance
- Recurring positions (daily/weekly) by description periodicity
- Monthly deposits excluding Zelle (used for revenue)

pick_avg_revenue(...) rules:
  * NY/CA => average of best 3 months
  * others => average of all months

Exposes summarize_statement_from_bytes(pdf_bytes, filename=None) for Flask UI.
"""

import os, re
from io import BytesIO
from typing import List, Optional, Tuple, Dict
from dataclasses import dataclass
from collections import defaultdict
from statistics import median
from datetime import datetime, date, timedelta

import pdfplumber
from pdf2image import convert_from_path, convert_from_bytes
import pytesseract
from dateutil import parser as dateparser
from PIL import Image

# ---------------- Config ----------------
TESS_LANG = "eng"
TESS_PSM = 6
OCR_DPI = 300

DATE_PAT = r"(?:\b\d{1,2}[/-]\d{1,2}\b)"             # 4/3 or 04-03
DATE_Y_PAT = r"(?:\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b)" # 04/03/2025
MONEY_PAT = r"[-+]?\$?\s?\d{1,3}(?:,\d{3})*(?:\.\d{2})"

EXCLUDE_DEPOSIT_KEYWORDS = ("zelle", "zelle®")

CORP_SUFFIX_RE = r"(?:LLC|L\.L\.C\.|INC\.?|CORP\.?|CORPORATION|LTD\.?|CO\.?|COMPANY|PLC|LLP|L\.L\.P\.|PLLC|P\.L\.L\.C\.)"

# ===================== NEW: Account-number helpers (ONLY change) =====================
# Boundary-safe patterns + context filters; returns masked last-4 via _mask_last4.
_LABEL_ACCOUNT_RE = re.compile(
    r"\b(?:primary\s+)?account\s*(?:number|no\.?|#)\b[:\-\s]*(?:ending\s+in\s*)?(?P<num>[xX\*#\-\s]*\d{2,})",
    re.IGNORECASE,
)
_ENDING_IN_RE = re.compile(r"\b(?:ending|ends)\s+in\s+(?P<num>\d{3,6})\b", re.IGNORECASE)
_MASKED_RE = re.compile(r"\b[xX\*#]{2,}\s*(?P<num>\d{3,6})\b")
_ACCT_SHORT_RE = re.compile(r"\b(?:acct|acct\.|a/c)\b[:\-\s]*(?P<num>[xX\*#\-\s]*\d{3,})", re.IGNORECASE)

_BAD_CONTEXT_RE = re.compile(r"\b(routing|aba|swift|phone|tel|fax|ein|ssn|tax|zip|date|statement)\b", re.IGNORECASE)
_CARD_CONTEXT_RE = re.compile(r"\b(card|visa|mastercard|debit)\b", re.IGNORECASE)

def _mask_last4(s: str) -> str:
    digits = re.sub(r"\D", "", s or "")
    if len(digits) >= 4:
        return "****" + digits[-4:]
    return s or ""

def extract_account_number_from_text(text: str) -> str:
    """
    Return masked account number (****1234) by scoring only account-context matches.
    Avoids phone/zip/routing/card numbers. Returns '' if not found.
    """
    if not text:
        return ""
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    head = "\n".join(lines[:60])  # headers often carry acct info

    # candidates: (weight, digits_len, digits)
    candidates: List[Tuple[int,int,str]] = []

    def add(raw: str, ctx: str, base_weight: int):
        if not raw:
            return
        if _BAD_CONTEXT_RE.search(ctx):
            return
        digits = re.sub(r"\D", "", raw)
        # Reject likely card numbers when card context present
        if len(digits) >= 13 and _CARD_CONTEXT_RE.search(ctx):
            return
        # Plausible account-length window (statements reveal 4–12)
        if not (4 <= len(digits) <= 12):
            return
        weight = base_weight
        if len(digits) == 7:
            weight -= 1  # slight downweight for date-ish length
        if 4 <= len(digits) <= 6:
            weight += 1  # prefer typical 'ending in' length
        candidates.append((weight, len(digits), digits))

    # 1) Strong labels (Primary account number / Account number)
    for m in _LABEL_ACCOUNT_RE.finditer(text):
        w = 9 if re.search(r"\bprimary\s+account\s+number\b", m.group(0), re.IGNORECASE) else 7
        add(m.group("num"), m.group(0), w)

    # 2) “ending in 1234”
    for m in _ENDING_IN_RE.finditer(text):
        add(m.group("num"), m.group(0), 6)

    # 3) Masked like ****1234 / XXXXX1422
    for m in _MASKED_RE.finditer(text):
        add(m.group("num"), m.group(0), 5)

    # 4) Short “Acct # …”
    for m in _ACCT_SHORT_RE.finditer(text):
        add(m.group("num"), m.group(0), 5)

    # 5) Header-only fallback: “Account ****1234 …”
    for m in re.finditer(r"\baccount\b[:\-\s]*([xX\*#\-\s]*\d{3,})", head, re.IGNORECASE):
        add(m.group(1), m.group(0), 5)

    # 6) Last fallback: lines with “account/acct” and trailing number
    if not candidates:
        acct_ctx = re.compile(r"\b(account|acct|a/c)\b", re.IGNORECASE)
        for ln in lines[:80]:
            if acct_ctx.search(ln):
                m = re.search(r"([xX\*#\-\s]*\d{3,})$", ln)
                if m:
                    add(m.group(1), ln, 4)

    if not candidates:
        return ""

    # Pick best: highest weight, then longer digit string
    _, _, digits = sorted(candidates, key=lambda t: (t[0], t[1]))[-1]
    return _mask_last4(digits)
# =================== END of Account-number helpers (ONLY change) =====================

# ---------------- Models ----------------
@dataclass
class Txn:
    dt: date
    desc: str
    amount: float
    runbal: Optional[float] = None

@dataclass
class StatementSummary:
    business_name: Optional[str]
    account_number: Optional[str]
    bank_name: Optional[str]
    statement_month: str
    debit_count: int
    credit_count: int
    negative_ending_days: int
    average_daily_balance: Optional[float]
    monthly_deposits_excl_zelle: float
    positions_daily: List[str]
    positions_weekly: List[str]

# ---------------- Text helpers ----------------
def _clean_lines(text: str) -> List[str]:
    out = []
    for ln in text.splitlines():
        ln = ln.replace("\x00", " ")
        ln = re.sub(r"\s{2,}", " ", ln).strip()
        if ln: out.append(ln)
    return out

def _parse_amount(tok: str) -> Optional[float]:
    tok = tok.replace("$","").replace(",","").strip()
    try: return float(tok)
    except: return None

def _normalize_desc(desc: str) -> str:
    s = desc.lower()
    s = re.sub(r"\s{2,}", " ", s)
    s = re.sub(r"\b\d{4,}\b", "", s)
    s = re.sub(r"[-/#*]+", " ", s)
    return " ".join(s.split())

# ---------------- Read PDF text ----------------
def read_pdf_text(path: str) -> Tuple[str, bool]:
    # Try native text
    try:
        blocks = []
        with pdfplumber.open(path) as pdf:
            for p in pdf.pages:
                t = p.extract_text() or ""
                if t.strip(): blocks.append(t)
        full = "\n".join(blocks).strip()
        if len(full) >= 300:
            return full, False
    except Exception:
        pass
    # OCR fallback
    try:
        imgs = convert_from_path(path, dpi=OCR_DPI)
    except Exception:
        with open(path, "rb") as f:
            imgs = convert_from_bytes(f.read(), dpi=OCR_DPI)
    txts = [pytesseract.image_to_string(img, lang=TESS_LANG, config=f"--psm {TESS_PSM}") for img in imgs]
    return "\n".join(txts), True

def read_pdf_text_from_bytes(pdf_bytes: bytes) -> Tuple[str, bool]:
    # Native text first
    try:
        blocks = []
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            for p in pdf.pages:
                t = p.extract_text() or ""
                if t.strip(): blocks.append(t)
        full = "\n".join(blocks).strip()
        if len(full) >= 300:
            return full, False
    except Exception:
        pass
    # OCR
    imgs = convert_from_bytes(pdf_bytes, dpi=OCR_DPI)
    txts = [pytesseract.image_to_string(img, lang=TESS_LANG, config=f"--psm {TESS_PSM}") for img in imgs]
    return "\n".join(txts), True

# ---------------- Filename helpers ----------------
def _business_from_filename(filename: Optional[str]) -> Optional[str]:
    if not filename: return None
    base = os.path.basename(filename)
    name, _ = os.path.splitext(base)
    parts = [p.strip() for p in name.split(" - ")]
    if parts:
        cand = parts[-1]
        if not re.fullmatch(r"\d{1,2}[_-]\d{1,2}[_-]\d{4}", cand):
            cand = re.sub(r"\b(e-?statement|statement|checking|savings|e-statement)\b", "", cand, flags=re.I).strip(" -_")
            if len(cand) >= 2:
                return cand
    return None

def _month_from_filename(filename: Optional[str]) -> Optional[Tuple[str, date, date]]:
    if not filename: return None
    base = os.path.basename(filename)
    name, _ = os.path.splitext(base)
    m = re.search(r"(\d{1,2})[_-](\d{1,2})[_-](\d{4})", name)
    if not m:
        m = re.search(r"(\d{4})[_-](\d{1,2})[_-](\d{1,2})", name)
        if m:
            y, mo, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
            try:
                d2 = date(y, mo, dd)
                d1 = date(y, mo, 1)
                d_end = (date(y + (mo // 12), (mo % 12) + 1, 1) - timedelta(days=1))
                return f"{y:04d}-{mo:02d}", d1, d_end
            except Exception:
                return None
        return None
    mo, dd, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        d2 = date(y, mo, dd)
        d1 = date(y, mo, 1)
        d_end = (date(y + (mo // 12), (mo % 12) + 1, 1) - timedelta(days=1))
        return f"{y:04d}-{mo:02d}", d1, d_end
    except Exception:
        return None

# ---------------- Period parsing (Chase-aware) ----------------
def _parse_period_variants(text: str) -> Optional[Tuple[date, date]]:
    """
    Try multiple generic/brand variants:
      - 06/01/2025 to 06/30/2025
      - 06/01/25 - 06/30/25
      - June 1, 2025 through June 30, 2025   (CHASE style)
      - From June 1, 2025 to June 30, 2025
    Returns (start_date, end_date) if found.
    """
    # 1) numeric with 'to' or '-' (YYYY or YY)
    for pat in [
        rf"\b({DATE_Y_PAT})\s*(?:to|through|\-)\s*({DATE_Y_PAT})\b",
        rf"\b({DATE_PAT})\s*(?:to|through|\-)\s*({DATE_PAT})\b",
    ]:
        m = re.search(pat, text, flags=re.I)
        if m:
            try:
                d1 = dateparser.parse(m.group(1), dayfirst=False, yearfirst=False).date()
                d2 = dateparser.parse(m.group(2), dayfirst=False, yearfirst=False).date()
                if d1 <= d2: return d1, d2
            except Exception:
                pass

    # 2) worded months (e.g., "June 1, 2025 through June 30, 2025")
    m2 = re.search(
        r"\b([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{2,4})\s*(?:to|through|\-)\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{2,4})\b",
        text, flags=re.I
    )
    if m2:
        try:
            d1 = dateparser.parse(m2.group(1)).date()
            d2 = dateparser.parse(m2.group(2)).date()
            if d1 <= d2: return d1, d2
        except Exception:
            pass

    # 3) phrases like "For the period June 1, 2025 to June 30, 2025" (allow prefix)
    m3 = re.search(
        r"(?:for\s+the\s+period|period\s+covered|statement\s+period)\s*[:\-]?\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{2,4}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s*(?:to|through|\-)\s*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{2,4}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        text, flags=re.I
    )
    if m3:
        try:
            d1 = dateparser.parse(m3.group(1)).date()
            d2 = dateparser.parse(m3.group(2)).date()
            if d1 <= d2: return d1, d2
        except Exception:
            pass

    return None

def month_from_period(text: str, filename: Optional[str]=None) -> Optional[Tuple[str, date, date]]:
    """
    Chase-friendly month extractor:
      - try general 'statement period' style
      - try Chase's 'June 1, 2025 through June 30, 2025' wording
      - try generic numeric ranges
      - fallback to any dated tokens
      - last resort: filename
    """
    # (A) First, explicit "Statement period/cycle" if present
    m = re.search(r"(statement (?:period|cycle).{0,30}?)(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s*(?:to|through|\-)\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", text, re.I)
    if m:
        try:
            d1 = dateparser.parse(m.group(2)).date()
            d2 = dateparser.parse(m.group(3)).date()
            return f"{d2.year:04d}-{d2.month:02d}", d1, d2
        except:
            pass

    # (B) Chase-style & other variants
    rng = _parse_period_variants(text)
    if rng:
        d1, d2 = rng
        return f"{d2.year:04d}-{d2.month:02d}", d1, d2

    # (C) Try "ending/for period Month YYYY"
    m2 = re.search(r"(for|ending|period)\s+(?:on\s+)?([A-Za-z]{3,9}\s+\d{4})", text, re.I)
    if m2:
        try:
            d = dateparser.parse(m2.group(2)).date()
            ym = f"{d.year:04d}-{d.month:02d}"
            d1 = date(d.year, d.month, 1)
            d2 = (date(d.year + (d.month // 12), (d.month % 12) + 1, 1) - timedelta(days=1))
            return ym, d1, d2
        except:
            pass

    # (D) Fallback: any dated tokens → choose max date's month
    ds = []
    for mm in re.finditer(DATE_Y_PAT, text):
        try: ds.append(dateparser.parse(mm.group(0)).date())
        except: pass
    if ds:
        d2 = max(ds)
        d1 = date(d2.year, d2.month, 1)
        d_end = (date(d2.year + (d2.month // 12), (d2.month % 12) + 1, 1) - timedelta(days=1))
        return f"{d2.year:04d}-{d2.month:02d}", d1, d_end

    # (E) Last resort: filename
    return _month_from_filename(filename)

# ---------------- Header parsing ----------------
def extract_header_info(text: str, filename: Optional[str]=None) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    lines = _clean_lines(text)
    head = " | ".join(lines[:80])

    # Bank name (explicit first)
    bank = None
    m = re.search(r"\b([A-Z][A-Za-z&.\- ]+(?:Bank|BANK|Credit Union|National Association|N\.A\.|N A|N.A\.|FSB|Association))\b", head)
    if m:
        bank = m.group(1).strip()
    else:
        m2 = re.search(r"\b(PRIMIS|CHASE|JPMORGAN\s+CHASE|WELLS\s+FARGO|BANK\s+OF\s+AMERICA|PNC|CITIBANK|CAPITAL\s+ONE|TD\s+BANK|TRUIST|US\s*BANK|ALLY|NAVY\s+FEDERAL)\b", head, re.I)
        if m2:
            bank = m2.group(1).upper().replace("JPMORGAN ", "").strip()

    # -------------------- Account number (ONLY change) --------------------
    acct = extract_account_number_from_text(text) or None
    # ---------------------------------------------------------------------

    # Business name
    biz = None
    # Tier 1: explicit label
    m = re.search(r"(?:Account Holder|Account Name|Account Owner|Business Name)\s*[:\-]\s*([A-Za-z0-9&'.\- ]{2,120})", text, re.I)
    if m:
        cand = m.group(1).strip(" :-")
        if len(cand) >= 2: biz = cand

    # Tier 2: corp suffix on top lines (skip noisy account/date headers)
    if not biz:
        for ln in lines[:50]:
            low = ln.lower()
            if re.search(r"\baccount\b", low) and re.search(r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b", low):
                continue
            if re.search(r"^\s*(a\s+)?[A-Z]{2,}\s+ACCOUNT", ln):
                continue
            m2 = re.search(rf"\b([A-Z0-9&' .-]+?\s+{CORP_SUFFIX_RE})\b", ln, re.I)
            if m2:
                cand = m2.group(1).strip(" :-")
                if len(cand) >= 2:
                    biz = cand
                    break

    # Tier 3: filename-derived
    if not biz:
        biz = _business_from_filename(filename)

    # Tier 4: first reasonable title line
    if not biz:
        for ln in lines[:40]:
            if bank and bank.lower() in ln.lower(): 
                continue
            if "statement" in ln.lower(): 
                continue
            if re.search(r"\d{1,6}\s+[A-Za-z]", ln): 
                continue  # likely address
            if re.search(r"[A-Za-z]{3,}", ln) and len(ln) <= 80:
                if "account" in ln.lower():
                    continue
                biz = ln.strip(" :-")
                break

    return biz, acct, bank

# ---------------- Transactions ----------------
@dataclass
class Txn:
    dt: date
    desc: str
    amount: float
    runbal: Optional[float] = None

def parse_transactions(text: str, period: Optional[Tuple[str, date, date]]) -> Tuple[List['Txn'], Optional[float], Optional[float]]:
    lines = _clean_lines(text)
    opening, ending = None, None
    for ln in lines[:120]:
        m = re.search(r"(beginning|starting|opening)\s+balance\s*[:\-]?\s*(" + MONEY_PAT + r")", ln, re.I)
        if m: opening = _parse_amount(m.group(2))
    for ln in lines[-200:]:
        m = re.search(r"(ending|closing)\s+balance\s*[:\-]?\s*(" + MONEY_PAT + r")", ln, re.I)
        if m: ending = _parse_amount(m.group(2))

    txns: List[Txn] = []
    for ln in lines:
        m = re.match(r"^\s*(" + DATE_Y_PAT + r"|" + DATE_PAT + r")\s+(.+?)\s+(" + MONEY_PAT + r")\s*(?:" + MONEY_PAT + r")?\s*$", ln)
        if m:
            dt_raw, desc, amt_raw = m.group(1), m.group(2).strip(), m.group(3)
            try: dt = dateparser.parse(dt_raw, yearfirst=False, dayfirst=False).date()
            except: continue
            amt = _parse_amount(amt_raw)
            if amt is None: continue
            if period and not re.search(r"\d{4}", dt_raw):
                _, _, d2 = period
                dt = dt.replace(year=d2.year)
            txns.append(Txn(dt=dt, desc=desc, amount=amt))
            continue
        m2 = re.match(r"^\s*(" + DATE_PAT + r")\s+(.+?)\s+(" + MONEY_PAT + r")\s*(CR|DR)\s*$", ln, re.I)
        if m2:
            dt_raw, desc, amt_raw, flag = m2.group(1), m2.group(2).strip(), m2.group(3), m2.group(4).upper()
            try: dt = dateparser.parse(dt_raw, yearfirst=False, dayfirst=False).date()
            except: continue
            amt = _parse_amount(amt_raw)
            if amt is None: continue
            amt = -abs(amt) if flag == "DR" else abs(amt)
            if period and not re.search(r"\d{4}", dt_raw):
                _, _, d2 = period
                dt = dt.replace(year=d2.year)
            txns.append(Txn(dt=dt, desc=desc, amount=amt))

    txns.sort(key=lambda t: (t.dt, t.desc))
    return txns, opening, ending

# ---------------- Metrics ----------------
def rebuild_daily_balances(txns: List[Txn], opening_balance: Optional[float], period: Optional[Tuple[str, date, date]]) -> Dict[date, float]:
    daily: Dict[date, float] = {}
    if not txns: return daily
    if period: _, d1, d2 = period
    else:
        d1, d2 = min(t.dt for t in txns), max(t.dt for t in txns)

    by_day: Dict[date, List[Txn]] = defaultdict(list)
    for t in txns: by_day[t.dt].append(t)

    bal = opening_balance if opening_balance is not None else 0.0
    d = d1
    while d <= d2:
        if d in by_day:
            for t in by_day[d]:
                bal += t.amount
        daily[d] = bal
        d += timedelta(days=1)
    return daily

def count_negative_days(daily: Dict[date, float]) -> int:
    return sum(1 for v in daily.values() if v < 0)

def average_daily_balance(daily: Dict[date, float]) -> Optional[float]:
    if not daily: return None
    return round(sum(daily.values()) / len(daily), 2)

def detect_positions(txns: List[Txn]) -> Tuple[List[str], List[str]]:
    groups: Dict[str, List[date]] = defaultdict(list)
    for t in txns:
        n = _normalize_desc(t.desc)
        if len(n) < 4 or n in ("deposit","pos","ach","online transfer"): continue
        groups[n].append(t.dt)
    daily, weekly = [], []
    for desc, ds in groups.items():
        if len(ds) < 3: continue
        ds.sort()
        gaps = [(ds[i]-ds[i-1]).days for i in range(1, len(ds))]
        if not gaps: continue
        med = median(gaps)
        if 0.8 <= med <= 1.3: daily.append(desc)
        elif 5.5 <= med <= 8.5: weekly.append(desc)
    def topk(lst):
        lst = list(set(lst)); lst.sort(); return lst[:5]
    return topk(daily), topk(weekly)

def month_key(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"

def compute_monthly_counts_and_deposits(txns: List[Txn]) -> Tuple[Dict[str,int], Dict[str,int], Dict[str,float]]:
    debit_counts, credit_counts, monthly_deposits = defaultdict(int), defaultdict(int), defaultdict(float)
    for t in txns:
        k = month_key(t.dt)
        if t.amount < 0:
            debit_counts[k] += 1
        elif t.amount > 0:
            credit_counts[k] += 1
            if not any(x in t.desc.lower() for x in EXCLUDE_DEPOSIT_KEYWORDS):
                monthly_deposits[k] += t.amount
    return debit_counts, credit_counts, monthly_deposits

def pick_avg_revenue(monthly_deposits: Dict[str, float], state: Optional[str]) -> Optional[float]:
    if not monthly_deposits: return None
    vals = list(monthly_deposits.values())
    if state and state.upper() in ("NY","CA"):
        vals.sort(reverse=True)
        top = vals[:3] if len(vals) >= 3 else vals
        return round(sum(top)/len(top), 2) if top else None
    return round(sum(vals)/len(vals), 2)

# ---------------- Summaries ----------------
def summarize_statement(text: str, used_ocr: bool, filename: Optional[str]=None):
    period = month_from_period(text, filename=filename)
    month_label = period[0] if period else "[unknown]"

    biz, acct, bank = extract_header_info(text, filename=filename)
    txns, opening, ending = parse_transactions(text, period)
    daily = rebuild_daily_balances(txns, opening, period)

    debits, credits, monthly_deposits = compute_monthly_counts_and_deposits(txns)
    this_month = period[0] if period else (month_key(max(t.dt for t in txns)) if txns else None)

    debit_count = debits.get(this_month, 0) if this_month else 0
    credit_count = credits.get(this_month, 0) if this_month else 0
    neg_days = count_negative_days(daily)
    adb = average_daily_balance(daily)
    daily_pos, weekly_pos = detect_positions(txns)
    monthly_dep_ex_zelle = monthly_deposits.get(this_month, 0.0) if this_month else 0.0

    summary = StatementSummary(
        business_name=biz,
        account_number=acct,
        bank_name=bank,
        statement_month=month_label,
        debit_count=debit_count,
        credit_count=credit_count,
        negative_ending_days=neg_days,
        average_daily_balance=adb,
        monthly_deposits_excl_zelle=round(monthly_dep_ex_zelle, 2),
        positions_daily=daily_pos,
        positions_weekly=weekly_pos
    )
    return summary, daily, txns

def summarize_statement_from_bytes(pdf_bytes: bytes, filename: Optional[str]=None):
    text, used_ocr = read_pdf_text_from_bytes(pdf_bytes)
    return summarize_statement(text, used_ocr, filename=filename)
