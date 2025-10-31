#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re
from io import BytesIO
from datetime import datetime
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from collections import defaultdict

from flask import Flask, request, redirect, url_for, render_template_string, flash
import pdfplumber
from pdf2image import convert_from_bytes
from PIL import Image
import pytesseract
from dateutil import parser as dateparser
from rapidfuzz import fuzz
import cv2
import numpy as np

# ----------------------- Config -----------------------
ALLOWED_EXT = {"pdf"}
MAX_CONTENT_LENGTH = 35 * 1024 * 1024   # 35MB
OCR_DPI_DEFAULT = 300
TESS_OEM = 1
TESS_PSM = 4
TESS_LANG = "eng"

US_STATES = {
    "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California","CO":"Colorado","CT":"Connecticut",
    "DE":"Delaware","FL":"Florida","GA":"Georgia","HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa",
    "KS":"Kansas","KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland","MA":"Massachusetts","MI":"Michigan",
    "MN":"Minnesota","MS":"Mississippi","MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada","NH":"New Hampshire",
    "NJ":"New Jersey","NM":"New Mexico","NY":"New York","NC":"North Carolina","ND":"North Dakota","OH":"Ohio",
    "OK":"Oklahoma","OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina","SD":"South Dakota",
    "TN":"Tennessee","TX":"Texas","UT":"Utah","VT":"Vermont","VA":"Virginia","WA":"Washington","WV":"West Virginia",
    "WI":"Wisconsin","WY":"Wyoming","DC":"District of Columbia","PR":"Puerto Rico"
}
STATE_NAMES = {v.upper():k for k,v in US_STATES.items()}  # "TEXAS"->"TX"

# Expanded corp suffixes for name detection
CORP_SUFFIXES_RE = r"(,?\s+(INCORPORATED|INC\.?|LLC|L\.L\.C\.|LTD\.?|CO\.?|CO|CORP\.?|CORPORATION|PLC|LLP|L\.L\.P\.|PLLC|P\.L\.L\.C\.|PC|P\.C\.|PA|P\.A\.))+$"

INDUSTRY_KEYWORDS = {
    "Restaurants": ["restaurant","pizza","grill","diner","coffee","cafe","café","bistro","bbq","taqueria"],
    "Construction": ["construction","contractor","roof","roofing","plumbing","electric","hvac","remodel","general contractor"],
    "Retail": ["retail","boutique","store","shop","fashion","apparel","clothing","gift shop"],
    "Healthcare": ["clinic","medical","dental","dentist","orthodont","healthcare","urgent care","chiropractic","pharmacy"],
    "Transportation": ["trucking","logistics","transport","freight","dispatch","fleet","carrier"],
    "Beauty & Wellness": ["salon","spa","barber","nail","esthetic","wellness","massage"],
    "E-commerce": ["shopify","amazon","ecommerce","e-commerce","online store","woocommerce"],
    "Professional Services": ["consulting","law","legal","attorney","cpa","accounting","bookkeeping","marketing","agency"],
}

# Anchors
ANCHORS = {
    "business_name": [
        "legal business name","business legal name","business name","company name","corporate name",
        "dba","doing business as","trade name","legal name of business","name of business",
        "business d/b/a name","business dba name","business d / b / a name"
    ],
    "state": [
        "state","state/province","business address","company address","mailing address","principal address","city, state","address"
    ],
    "industry": [
        "industry","business type","naics","sic","primary industry","type of business","line of business","nature of business","sector"
    ],
    "tib": [
        "time in business","years in business","length of ownership","owner since","date business started",
        "business start date","established","since","in business since","ownership since"
    ],
    "fico": [
        "fico","credit score","personal fico","owner fico","beacon score"
    ],
}

# Regex
RE_FICO = re.compile(r"\b(3\d{2}|[4-7]\d{2}|8[0-4]\d|850)\b")
RE_ZIP5 = re.compile(r"\b\d{5}(?:-\d{4})?\b")
RE_CITY_STATE_ZIP = re.compile(r"\b([A-Za-z][A-Za-z\.\- ]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\b")
RE_DATE = re.compile(r"\b(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}|[A-Za-z]{3,9}\s+\d{1,2},\s*\d{2,4})\b")
RE_YEARS_NUM = re.compile(r"\b(\d{1,2}(?:\.\d{1,2})?)\s*(?:yrs?|years?)\b", re.I)
RE_YEARS_MONTHS = re.compile(r"\b(\d{1,2})\s*(?:yrs?|years?)\s*(\d{1,2})\s*(?:mos?|months?)\b", re.I)
RE_MONTHS_ONLY = re.compile(r"\b(\d{1,3}(?:\.\d{1,2})?)\s*(?:mos?|months?)\b", re.I)
RE_STREET = re.compile(r"^\s*(\d{1,6})\s+([A-Za-z0-9\.\-# ]+)")
RE_PHONE = re.compile(r"\b(\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b")
RE_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

# ---------- Generic helpers ----------
STOP_WORDS = set("""
application form page signature initial date phone email fax street address city state zip website ein ssn dob
""".strip().split())

def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXT

def lines_from_text(text: str) -> List[str]:
    text = text.replace("\x00"," ")
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in text.splitlines()]
    return [ln for ln in lines if ln]

def best_anchor_hits(lines_lower: List[str], anchors: List[str]):
    hits = []
    for i, ln in enumerate(lines_lower):
        for a in anchors:
            score = fuzz.partial_ratio(a, ln)
            if score >= 80:
                hits.append((i, a, score))
    hits.sort(key=lambda x: (-x[2], x[0]))
    return hits

def window(lines: List[str], idx: int, radius: int = 2) -> str:
    s = max(0, idx - radius)
    e = min(len(lines), idx + radius + 1)
    return " | ".join(lines[s:e])

def clean_business_name(name: str) -> str:
    name = re.sub(r"^(legal|company|business|corporate)\s+(name|legal\s+name)\s*[:\-]?\s*", "", name, flags=re.I)
    name = re.sub(r"^(dba|doing business as|trade name|business d/?b/?a name)\s*[:\-]?\s*", "", name, flags=re.I)
    name = re.sub(r"\s{2,}", " ", name).strip(" :-–")
    return name

def clean_and_clip(s: str) -> str:
    s = re.sub(r"\s{2,}", " ", s).strip()
    s = s.strip(":–-").strip()
    return s

def is_probably_address(s: str) -> bool:
    s_low = s.lower()
    if RE_STREET.search(s):  # starts with a street number
        return True
    if any(k in s_low for k in ["suite", "ste", "apt", "unit", "floor", "fl", "ave", "avenue", "blvd", "road", "rd", "st ", " street", "lane", "ln", "dr ", " drive"]):
        return True
    if RE_PHONE.search(s) or RE_EMAIL.search(s):
        return True
    if RE_CITY_STATE_ZIP.search(s):
        return True
    return False

def name_score(line: str) -> int:
    """Score how likely a line is a business name."""
    s = line.strip().strip(":,")
    if len(s) < 2 or len(s) > 120:
        return -999
    if is_probably_address(s):  # avoid addresses
        return -5
    low = s.lower()
    if any(w in low for w in STOP_WORDS):
        return -2
    tokens = [t for t in re.split(r"[^\w&\-.']", s) if t]
    if not tokens:
        return -2
    score = 0
    if re.search(CORP_SUFFIXES_RE, s, re.I): score += 5
    if 2 <= len(tokens) <= 6: score += 2
    cap_tokens = sum(1 for t in tokens if t[:1].isupper())
    cap_ratio = cap_tokens / max(1, len(tokens))
    if cap_ratio >= 0.7: score += 2
    if " & " in s or " - " in s: score += 1
    if sum(c.isdigit() for c in s) >= 3: score -= 2
    if s.endswith((".", ",")): score -= 1
    if low.startswith("industry "):  # prevent "Industry Automotive" false positives
        score -= 5
    return score

# ----------------------- Business Name (layout-aware) -----------------------
BN_LABELS = [
    "legal business name","business legal name","business name","company name","corporate name",
    "dba","doing business as","trade name","legal name of business","name of business",
    "business d/b/a name","business dba name","business d / b / a name"
]
BN_STOP_TOKENS = [
    "industry","naics","sic","ein","ssn","dob","address","city","state","zip","legal entity","owner","date","email","phone"
]

def extract_business_name_layout_aware(pdf_bytes: bytes) -> Optional[Tuple[str, str, float]]:
    """
    Returns (value, evidence, conf_boost) or None.
    Uses pdfplumber words to:
      1) Grab text to the RIGHT of a label on the same row (no colon required).
      2) Fallback: top-of-page largest-font candidate that looks like a name.
    """
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            if not pdf.pages:
                return None
            page = pdf.pages[0]
            words = page.extract_words(
                use_text_flow=True,
                keep_blank_chars=False,
                extra_attrs=["size", "x0", "x1", "top", "bottom"]
            )
            if not words:
                return None

            # 1) Right-of-label extraction (same visual row)
            rows = defaultdict(list)
            for w in words:
                band = int(round(w["top"]/3.0))
                rows[band].append(w)

            for band, ws in rows.items():
                row_text = " ".join(w["text"] for w in ws)
                row_low = row_text.lower()
                if any(lbl in row_low for lbl in BN_LABELS):
                    label_x1 = None
                    for w in ws:
                        if any(lbl in w["text"].lower() for lbl in BN_LABELS):
                            label_x1 = max(label_x1 or w["x1"], w["x1"])
                    if label_x1 is not None:
                        right_tokens = [w["text"] for w in ws if w["x0"] > label_x1 + 2]
                        right_line = " ".join(right_tokens)
                        # cut at stop tokens
                        low_tokens = [t.lower() for t in right_tokens]
                        stop_idx = len(right_tokens)
                        for i, t in enumerate(low_tokens):
                            if any(t.startswith(st) for st in BN_STOP_TOKENS) or any(st in t for st in BN_STOP_TOKENS):
                                stop_idx = i
                                break
                        if stop_idx > 0:
                            cand = " ".join(right_tokens[:stop_idx])
                            cand = clean_and_clip(clean_business_name(cand))
                            if 2 <= len(cand) <= 120 and name_score(cand) >= 1:
                                return (cand, f"[layout row] {row_text}", 0.18)

            # 2) Top-of-page largest font candidate (first 25% height)
            page_height = page.height or 1000.0
            cutoff_y = page_height * 0.25
            bands = defaultdict(list)
            for w in words:
                if w["top"] <= cutoff_y:
                    bands[int(round(w["top"]/2.0))].append(w)

            candidates = []
            for b, ws in bands.items():
                avg_size = sum(w.get("size", 10.0) for w in ws) / max(1, len(ws))
                text_line = clean_and_clip(" ".join(w["text"] for w in ws))
                sc = name_score(text_line)
                x_avg = sum((w["x0"]+w["x1"])/2.0 for w in ws)/len(ws)
                center_bias = -abs(x_avg - (page.width/2.0)) / (page.width/2.0)
                score = sc + (avg_size/10.0) + (0.8 + center_bias)
                candidates.append((score, text_line, f"[top largest-font] size≈{avg_size:.1f} text='{text_line[:80]}'"))

            candidates.sort(key=lambda x: -x[0])
            for score, text_line, ev in candidates[:5]:
                if name_score(text_line) >= 2:
                    return (text_line, ev, 0.10)

    except Exception:
        return None
    return None

# ----------------------- OCR Preprocessing -----------------------
def deskew(image: np.ndarray) -> np.ndarray:
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY) if len(image.shape) == 3 else image
    gray = cv2.bitwise_not(gray)
    thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)[1]
    coords = np.column_stack(np.where(thresh > 0))
    if coords.size == 0: return image
    angle = cv2.minAreaRect(coords)[-1]
    angle = -(90 + angle) if angle < -45 else -angle
    h, w = image.shape[:2]
    M = cv2.getRotationMatrix2D((w//2, h//2), angle, 1.0)
    return cv2.warpAffine(image, M, (w, h), flags=cv2.INTER_CUBIC, borderMode=cv2.BORDER_REPLICATE)

def preprocess_for_ocr(pil_img: Image.Image) -> Image.Image:
    cv_img = cv2.cvtColor(np.array(pil_img), cv2.COLOR_RGB2BGR)
    cv_img = deskew(cv_img)
    gray = cv2.cvtColor(cv_img, cv2.COLOR_BGR2GRAY)
    gray = cv2.normalize(gray, None, 0, 255, cv2.NORM_MINMAX)
    gray = cv2.medianBlur(gray, 3)
    bin_img = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                                    cv2.THRESH_BINARY, 31, 9)
    kernel = np.ones((1, 1), np.uint8)
    bin_img = cv2.morphologyEx(bin_img, cv2.MORPH_CLOSE, kernel)
    return Image.fromarray(bin_img)

def paddle_ocr_page(pil_img: Image.Image) -> str:
    try:
        from paddleocr import PaddleOCR
    except Exception:
        return ""
    if not hasattr(paddle_ocr_page, "_ocr"):
        paddle_ocr_page._ocr = PaddleOCR(use_angle_cls=True, lang='en')
    cv_img = cv2.cvtColor(np.array(pil_img), cv2.COLOR_RGB2BGR)
    result = paddle_ocr_page._ocr.ocr(cv_img, cls=True)
    lines = []
    for res in result:
        for box, (txt, conf) in res:
            if conf >= 0.5:
                lines.append(txt)
    return "\n".join(lines)

# ----------------------- Extraction Core -----------------------
@dataclass
class FieldResult:
    value: Optional[str]
    confidence: float
    evidence: Dict[str, str]

def best_anchor_hits_local(lines_lower: List[str], anchors: List[str]) -> List[Tuple[int,str,int]]:
    hits = []
    for i, ln in enumerate(lines_lower):
        for a in anchors:
            score = fuzz.partial_ratio(a, ln)
            if score >= 80:
                hits.append((i, a, score))
    hits.sort(key=lambda x: (-x[2], x[0]))
    return hits

def find_with_anchors(lines: List[str], lines_lower: List[str], field_key: str, anchors: Dict[str, List[str]], extractor_fn, conf_base: float = 0.6) -> FieldResult:
    def headtail_try():
        head = " | ".join(lines[:60])
        tail = " | ".join(lines[-60:])
        v = extractor_fn(head) or extractor_fn(tail)
        if v:
            return FieldResult(v, 0.45, {"anchor":"global-fallback","line":"(document head/tail)"})
        return FieldResult(None, 0.0, {})
    hits = best_anchor_hits_local(lines_lower, anchors[field_key])
    best = FieldResult(None, 0.0, {})
    for (idx, anchor, score) in hits[:8]:
        win = window(lines, idx, radius=2)
        v = extractor_fn(win)
        if v:
            conf = min(conf_base + (score/100.0)*0.35, 0.98)
            if conf > best.confidence:
                best = FieldResult(v, round(conf,2), {"anchor":anchor, "line":win})
    if not best.value:
        return headtail_try()
    return best

def extract_fico(win: str) -> Optional[str]:
    m = RE_FICO.search(win)
    if not m: return None
    v = int(m.group(0))
    return str(v) if 300 <= v <= 850 else None

# ---------- State extraction (multi-line aware + ZIP3 fallback) ----------
ZIP3_TO_STATE = {
    **{k:"AL" for k in range(350, 370)},
    **{k:"AK" for k in range(995, 1000)},
    **{k:"AZ" for k in range(850, 866)},
    **{k:"AR" for k in range(716, 730)},
    **{k:"CA" for k in range(900, 962)},
    **{k:"CO" for k in range(800, 816)},
    **{k:"CT" for k in range(600, 700)},
    **{k:"DC" for k in range(200, 207)},
    **{k:"DE" for k in range(197, 200)},
    **{k:"FL" for k in range(320, 350)},
    **{k:"GA" for k in range(300, 321)},
    **{k:"HI" for k in range(967, 969)},
    **{k:"IA" for k in range(500, 528)},
    **{k:"ID" for k in range(832, 839)},
    **{k:"IL" for k in range(600, 630)},
    **{k:"IN" for k in range(460, 480)},
    **{k:"KS" for k in range(660, 680)},
    **{k:"KY" for k in range(400, 428)},
    **{k:"LA" for k in range(700, 716)},
    **{k:"MA" for k in range(100, 280)},
    **{k:"MD" for k in range(206, 220)},
    **{k:"ME" for k in range(390, 400)},
    **{k:"MI" for k in range(480, 500)},
    **{k:"MN" for k in range(550, 568)},
    **{k:"MO" for k in range(630, 660)},
    **{k:"MS" for k in range(386, 400)},
    **{k:"MT" for k in range(590, 600)},
    **{k:"NC" for k in range(270, 290)},
    **{k:"ND" for k in range(580, 590)},
    **{k:"NE" for k in range(680, 700)},
    **{k:"NH" for k in range(300, 400)},
    **{k:"NJ" for k in range(700, 900)},
    **{k:"NM" for k in range(870, 885)},
    **{k:"NV" for k in range(889, 900)},
    **{k:"NY" for k in range(100, 150)},
    **{k:"OH" for k in range(430, 460)},
    **{k:"OK" for k in range(730, 750)},
    **{k:"OR" for k in range(970, 980)},
    **{k:"PA" for k in range(150, 200)},
}
ZIP3_TO_STATE.update({k:"PR" for k in range(6, 10)})   # 006–009
ZIP3_TO_STATE.update({k:"RI" for k in range(280, 291)})
ZIP3_TO_STATE.update({k:"SC" for k in range(290, 300)})
ZIP3_TO_STATE.update({k:"SD" for k in range(570, 578)})
ZIP3_TO_STATE.update({k:"TN" for k in range(370, 386)})
ZIP3_TO_STATE.update({k:"TX" for k in range(750, 800)})
ZIP3_TO_STATE.update({k:"UT" for k in range(840, 850)})
ZIP3_TO_STATE.update({k:"VA" for k in range(220, 247)})
ZIP3_TO_STATE.update({k:"VT" for k in range(50, 60)})   # 050–059
ZIP3_TO_STATE.update({k:"WA" for k in range(980, 994)})
ZIP3_TO_STATE.update({k:"WI" for k in range(530, 550)})
ZIP3_TO_STATE.update({k:"WV" for k in range(247, 269)})
ZIP3_TO_STATE.update({k:"WY" for k in range(820, 832)})

def zip_to_state(zipcode: str) -> Optional[str]:
    m = RE_ZIP5.search(zipcode)
    if not m:
        return None
    z = m.group(0)
    try:
        z3 = int(z[:3])
    except Exception:
        return None
    return ZIP3_TO_STATE.get(z3)

def collect_address_blocks(lines: List[str], max_block_lines: int = 4) -> List[str]:
    """
    Build stitched address blocks starting from address anchors or a street-number line.
    """
    blocks = []
    i = 0
    N = len(lines)
    while i < N:
        ln = lines[i]
        low = ln.lower()
        is_anchor = ("address" in low) or RE_STREET.search(ln)
        if is_anchor:
            block = [ln]
            j = i + 1
            while j < N and len(block) < max_block_lines:
                nxt = lines[j].strip()
                if not nxt:
                    break
                low_n = nxt.lower()
                if any(tok in low_n for tok in ["industry","ein","ssn","dob","owner","date","application",
                                                "business legal name","business name","company name"]):
                    break
                block.append(nxt)
                j += 1
            blocks.append(" | ".join(block))
            i = j
        else:
            i += 1
    return blocks

def extract_state_stronger_multiline(all_lines: List[str], window: str) -> Optional[Tuple[str, str]]:
    """
    Try to find state in the given window; if fail, stitch multi-line address blocks and search there.
    Returns (state_abbr, evidence).
    """
    # 1) CITY, ST ZIP in the window
    m = RE_CITY_STATE_ZIP.search(window)
    if m and m.group(2) in US_STATES:
        return m.group(2), f"[city,st,zip] {window}"

    # 2) Full state name in window
    up = window.upper()
    for full_name, abbr in STATE_NAMES.items():
        if f" {full_name} " in f" {up} ":
            return abbr, f"[state-name] {window}"

    # 3) ZIP present → infer via ZIP3
    m = RE_ZIP5.search(window)
    if m:
        abbr = zip_to_state(m.group(0))
        if abbr:
            return abbr, f"[zip-only→{abbr}] {window}"

    # 4) Search stitched address blocks across the doc
    for block in collect_address_blocks(all_lines):
        m = RE_CITY_STATE_ZIP.search(block)
        if m and m.group(2) in US_STATES:
            return m.group(2), f"[block city,st,zip] {block}"
        upb = block.upper()
        for full_name, abbr in STATE_NAMES.items():
            if f" {full_name} " in f" {upb} ":
                return abbr, f"[block state-name] {block}"
        m2 = RE_ZIP5.search(block)
        if m2:
            abbr = zip_to_state(m2.group(0))
            if abbr:
                return abbr, f"[block zip-only→{abbr}] {block}"

    # 5) Last resort: any 2-letter token that is a state, but only if 'address' is present
    if "address" in window.lower():
        for token in re.findall(r"\b[A-Z]{2}\b", window):
            if token in US_STATES:
                return token, f"[address token] {window}"

    return None

# -------- TIB parser (strict: requires tib keywords present) --------
TIB_KEYWORDS = ("time in business","years in business","length of ownership","owner since",
                "date business started","business start date","established","since","in business since","ownership since")

def parse_tib_value_strict(neigh: str) -> Optional[Tuple[float,float]]:
    low = neigh.lower()
    if not any(k in low for k in TIB_KEYWORDS):
        return None
    m = RE_YEARS_MONTHS.search(neigh)
    if m:
        yrs = int(m.group(1)); mos = int(m.group(2))
        total_m = yrs*12 + mos
        return float(total_m), round(total_m/12.0, 2)
    m = RE_MONTHS_ONLY.search(neigh)
    if m:
        mos = float(m.group(1))
        return mos, round(mos/12.0, 2)
    m = RE_YEARS_NUM.search(neigh)
    if m:
        yrs = float(m.group(1)); mos = round(yrs*12.0, 1)
        return mos, round(yrs, 2)
    dt = parse_date_candidate(neigh)
    if dt:
        delta = (datetime.now() - dt).days
        yrs = round(delta / 365.25, 2)
        mos = round(delta / 30.4375, 1)
        return mos, yrs
    m = re.search(r"(estab\w*|since|started)\s*(?:in\s*)?(\d{4})", neigh, flags=re.I)
    if m:
        year = int(m.group(2))
        if 1900 < year <= datetime.now().year:
            yrs = float(datetime.now().year - year)
            mos = round(yrs*12.0, 1)
            return mos, round(yrs, 2)
    return None

def parse_date_candidate(neigh: str) -> Optional[datetime]:
    c = []
    for m in RE_DATE.finditer(neigh):
        raw = m.group(1)
        try:
            dt = dateparser.parse(raw, dayfirst=False, fuzzy=True)
            if dt.year > 1900 and dt <= datetime.now(): c.append(dt)
        except Exception:
            pass
    if not c: return None
    c.sort()
    return c[0]

# -------- Business Name text-window extractor (handles no colon) --------
def extract_bn_from_window(win: str) -> Optional[str]:
    low = win.lower()
    pos = None
    label_hit = None
    for lbl in BN_LABELS:
        i = low.find(lbl)
        if i != -1 and (pos is None or i < pos):
            pos = i; label_hit = lbl
    if pos is None:
        return None
    after = win[pos + len(label_hit):]
    after = after.replace("|", " ")
    after = clean_and_clip(after)
    tokens = after.split()
    cut = len(tokens)
    for i, t in enumerate(tokens):
        tl = t.lower().strip(",;:")
        if tl in BN_STOP_TOKENS or tl in ("business","legal","name"):
            cut = i
            break
    if cut <= 0:
        return None
    cand = " ".join(tokens[:cut])
    cand = clean_and_clip(clean_business_name(cand))
    if 2 <= len(cand) <= 120 and name_score(cand) >= 1:
        return cand
    return None

# ----------------------- Main extraction -----------------------
def extract_fields_from_bytes(pdf_bytes: bytes, ocr_dpi: int = OCR_DPI_DEFAULT, tess_psm: int = TESS_PSM):
    # 1) Native text first
    native_text = ""
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            blocks = []
            for p in pdf.pages:
                t = p.extract_text() or ""
                if t.strip(): blocks.append(t)
            native_text = "\n".join(blocks).strip()
    except Exception:
        native_text = ""

    use_ocr = (len(native_text) < 1000)
    if use_ocr:
        pages = convert_from_bytes(pdf_bytes, dpi=ocr_dpi)
        texts = []
        tess_cfg = rf"--oem {TESS_OEM} --psm {tess_psm}"
        for img in pages:
            pre = preprocess_for_ocr(img)
            txt = pytesseract.image_to_string(pre, lang=TESS_LANG, config=tess_cfg).strip()
            if len(txt) < 100:
                alt = paddle_ocr_page(img)
                if len(alt) > len(txt): txt = alt
            txt = txt.replace("—","-").replace("•","-")
            texts.append(txt)
        text = "\n".join(texts)
        source = "ocr"
    else:
        text = native_text
        source = "native"

    lines = lines_from_text(text)
    lines_lower = [ln.lower() for ln in lines]
    out: Dict[str, FieldResult] = {}

    # -------- FICO --------
    out["FICO"] = find_with_anchors(lines, lines_lower, "fico", ANCHORS, extract_fico)

    # -------- State (address-aware) --------
    def _state(win: str) -> Optional[str]:
        res = extract_state_stronger_multiline(lines, win)
        return res[0] if res else None
    state_field = find_with_anchors(lines, lines_lower, "state", ANCHORS, _state, conf_base=0.62)
    if state_field.value:
        res = extract_state_stronger_multiline(lines, state_field.evidence.get("line",""))
        if res:
            abbr, ev = res
            state_field.evidence = {"anchor": "address-aware", "line": ev}
    out["State"] = state_field

    # -------- Business Name (layout-aware + text fallback) --------
    bn_field = find_with_anchors(lines, lines_lower, "business_name", ANCHORS, extract_bn_from_window, conf_base=0.72)

    conf_boost = 0.0
    if source == "native":
        layout_res = extract_business_name_layout_aware(pdf_bytes)
        if layout_res:
            val, ev, boost = layout_res
            conf_boost += boost
            if not bn_field.value or name_score(val) > name_score(bn_field.value):
                bn_field.value = val
                bn_field.evidence = {"anchor": "layout-aware", "line": ev}

    def extract_business_name_stronger(lines_: List[str]) -> Optional[Tuple[str, str, float]]:
        text_all = " | ".join(lines_)
        m = re.search(r"(?:legal\s+business\s+name|business\s+name|company\s+name|corporate\s+name)\s*[:\-]?\s*([^\|\n]+)", text_all, re.I)
        if m:
            cand = clean_and_clip(clean_business_name(m.group(1)))
            if 2 <= len(cand) <= 120 and name_score(cand) >= 1:
                return cand, "[anchor]", 0.10
        m = re.search(r"(?:dba|doing\s+business\s+as|trade\s+name|business d/?b/?a name)\s*[:\-]?\s*([^\|\n]+)", text_all, re.I)
        if m:
            cand = clean_and_clip(clean_business_name(m.group(1)))
            if 2 <= len(cand) <= 120 and name_score(cand) >= 1:
                return cand, "[dba]", 0.10
        addr_idx = None
        for i, ln in enumerate(lines_[:40]):
            if RE_STREET.search(ln):
                addr_idx = i
                break
        if addr_idx is not None and addr_idx-1 >= 0:
            prev = clean_and_clip(lines_[addr_idx-1])
            if 2 <= len(prev) <= 120 and name_score(prev) >= 2:
                return prev, "[above-address]", 0.08
        top = [clean_and_clip(ln) for ln in lines_[:20]]
        scored = [(name_score(ln), ln) for ln in top if ln]
        scored = [s for s in scored if s[0] >= 2]
        scored.sort(key=lambda x: -x[0])
        for sc, ln in scored[:5]:
            return ln, "[top-scored]", 0.06
        for ln in lines_[:40]:
            if re.search(CORP_SUFFIXES_RE, ln, re.I):
                cand = clean_and_clip(ln)
                if name_score(cand) >= 1:
                    return cand, "[corp-suffix-top]", 0.05
        return None

    if not bn_field.value or bn_field.confidence < 0.82:
        text_res = extract_business_name_stronger(lines)
        if text_res:
            val, ev, boost = text_res
            if not bn_field.value or name_score(val) > name_score(bn_field.value):
                bn_field.value = val
                bn_field.evidence = {"anchor": "text-heuristic", "line": ev}
            conf_boost += boost

    if bn_field.value:
        base = max(bn_field.confidence, 0.70)
        if re.search(CORP_SUFFIXES_RE, bn_field.value, re.I):
            conf_boost += 0.03
        bn_field.confidence = float(min(0.99, base + conf_boost))
    out["BusinessName"] = bn_field

    # -------- Industry --------
    def classify_industry(text_all: str) -> Optional[str]:
        m = re.search(r"(?:industry|business\s*type|naics|sic|line\s+of\s+business|nature\s+of\s+business|sector)\s*[:\-]?\s*([A-Za-z\&\-\s/]+)", text_all, flags=re.I)
        if m:
            cand = re.sub(r"\s{2,}", " ", m.group(1).split("|")[0].strip())
            if 2 <= len(cand) <= 80: return cand
        t = text_all.lower()
        best, hits = None, 0
        for label, kws in INDUSTRY_KEYWORDS.items():
            h = sum(1 for k in kws if k in t)
            if h > hits: best, hits = label, h
        return best

    out["Industry"] = find_with_anchors(lines, lines_lower, "industry", ANCHORS, lambda w: classify_industry(w), conf_base=0.55)

    # -------- Length of Ownership -> months (and years) (strict) --------
    tib = find_with_anchors(lines, lines_lower, "tib", ANCHORS, parse_tib_value_strict, conf_base=0.6)
    if isinstance(tib.value, tuple):
        mos, yrs = tib.value
        tib.value = f"{mos:.1f} months ({yrs:.2f} years)"
    out["LengthOfOwnership"] = tib

    # Confidence nudge for state when tied to ZIP
    st = out.get("State")
    if st and st.value:
        if any(tag in st.evidence.get("line","") for tag in ["city,st,zip", "zip-only", "block "]):
            st.confidence = min(0.99, max(st.confidence, 0.76) + 0.08)

    preview = f"[source={'ocr' if use_ocr else 'native'}] " + "\n".join(lines[:200])
    return out, preview

# ----------------------- Flask App + UI -----------------------
app = Flask(__name__)
app.secret_key = os.environ.get("APP_SECRET", "dev")
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH

TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Application Extractor</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root {
      --bg: #f6f7fb; --card: #ffffff; --ink: #202124; --muted: #5f6368;
      --line: #e5e7eb; --ok: #0b7a0b; --mid: #a36b00; --low: #b00020; --accent: #2d5cf6;
    }
    * { box-sizing: border-box; }
    body { margin: 0; background: var(--bg); color: var(--ink);
           font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, "Helvetica Neue", Arial, "Noto Sans", "Liberation Sans", sans-serif; }
    .wrap { max-width: 1100px; margin: 28px auto; padding: 0 16px; }
    header { display:flex; align-items:center; justify-content:space-between; margin-bottom: 18px; }
    h1 { margin: 0; font-size: 24px; letter-spacing: .2px; }
    .card { background: var(--card); border: 1px solid var(--line); border-radius: 14px; padding: 16px; box-shadow: 0 4px 16px rgba(0,0,0,.04); }
    .uploader { display:grid; grid-template-columns: 1.2fr .8fr; gap: 16px; margin-bottom: 18px; }
    @media (max-width: 900px) { .uploader { grid-template-columns: 1fr; } }
    .drop { border: 2px dashed var(--line); border-radius: 12px; padding: 24px; text-align:center; background:#fafbff; transition: border-color .2s, background .2s; }
    .drop.drag { border-color: var(--accent); background:#f0f4ff; }
    .hint { color: var(--muted); font-size: 14px; margin-top: 6px; }
    .controls { display:flex; gap:12px; align-items:center; }
    .controls label { font-size: 14px; color: var(--muted); }
    select, button { padding: 10px 12px; border-radius: 10px; border:1px solid var(--line); background:#fff; font-size:14px; }
    button[type=submit] { background: var(--accent); color:#fff; border:none; padding: 10px 16px; cursor:pointer; }
    .grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 14px; }
    .field { padding: 12px; border-radius: 12px; border: 1px solid var(--line); background:#fff; }
    .badge { display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px; }
    .ok { color: var(--ok); background:#e6f6e6; }
    .mid { color: var(--mid); background:#fff3dc; }
    .low { color: var(--low); background:#fde7ea; }
    pre { white-space: pre-wrap; word-wrap: break-word; font-size: 12.5px; line-height: 1.5; }
    .footer { margin-top: 8px; font-size: 12.5px; color: var(--muted); }
    .errors { color: var(--low); margin: 8px 0 0; }
  </style>
</head>
<body>
  <div class="wrap">
    <header><h1>Application Field Extractor</h1></header>

    <form class="card" action="{{ url_for('extract') }}" method="post" enctype="multipart/form-data" id="form">
      <div class="uploader">
        <div>
          <div id="drop" class="drop">
            <div style="font-size:16px; margin-bottom:6px;">Drag & drop your PDF here</div>
            <div class="hint">or click to choose a file (max 35MB)</div>
            <input type="file" name="file" id="file" accept=".pdf" style="display:none" required />
          </div>
          <div class="errors">
            {% with messages = get_flashed_messages() %}
              {% if messages %}
                <ul>{% for m in messages %}<li>{{ m }}</li>{% endfor %}</ul>
              {% endif %}
            {% endwith %}
          </div>
        </div>

        <div>
          <div class="controls">
            <label for="psm">PSM</label>
            <select id="psm" name="psm">
              <option value="4">4 (block of text)</option>
              <option value="6" {% if request.form.get('psm')=='6' %}selected{% endif %}>6 (uniform paragraphs)</option>
            </select>

            <label for="dpi">DPI</label>
            <select id="dpi" name="dpi">
              <option value="300">300</option>
              <option value="400" {% if request.form.get('dpi')=='400' %}selected{% endif %}>400</option>
            </select>

            <button type="submit">Extract</button>
          </div>
          <div class="hint" style="margin-top:8px;">Tip: If the scan is rough, try PSM 6 and 400 DPI.</div>
        </div>
      </div>
    </form>

    {% if results %}
      <div class="card" style="margin-top:16px;">
        <h2 style="margin-top:0;">Results</h2>
        <div class="grid">
          {% for key, fr in results.items() %}
            <div class="field">
              <div style="display:flex;justify-content:space-between;align-items:center;">
                <h3 style="margin:0 0 8px 0; font-size:16px;">{{ key }}</h3>
                {% set cls = 'ok' if fr.confidence>=0.8 else ('mid' if fr.confidence>=0.6 else 'low') %}
                <span class="badge {{ cls }}">{{ "%.2f"|format(fr.confidence) }}</span>
              </div>
              <p style="margin:.25rem 0;"><strong>Value:</strong> {{ fr.value if fr.value else "[Not Found]" }}</p>
              {% if fr.evidence and fr.evidence.line %}
                <p class="hint" style="margin:.25rem 0;"><strong>Evidence:</strong> <em>{{ fr.evidence.line }}</em></p>
              {% endif %}
            </div>
          {% endfor %}
        </div>

        <h3 style="margin-top:16px;">Parsed Text Preview</h3>
        <pre>{{ preview }}</pre>
        <div class="footer">Green = reliable, Amber = review, Red = low confidence.</div>
      </div>
    {% endif %}
  </div>

  <script>
    const drop = document.getElementById('drop');
    const fileInput = document.getElementById('file');
    drop.addEventListener('click', () => fileInput.click());
    drop.addEventListener('dragover', e => { e.preventDefault(); drop.classList.add('drag'); });
    drop.addEventListener('dragleave', e => { drop.classList.remove('drag'); });
    drop.addEventListener('drop', e => {
      e.preventDefault(); drop.classList.remove('drag');
      if (e.dataTransfer.files && e.dataTransfer.files[0]) {
        fileInput.files = e.dataTransfer.files;
        document.getElementById('form').submit();
      }
    });
  </script>
</body>
</html>
"""

@app.route("/", methods=["GET"])
def index():
    return render_template_string(TEMPLATE, results=None, preview=None)

@app.route("/extract", methods=["POST"])
def extract():
    f = request.files.get("file")
    if not f or f.filename == "":
        flash("No file selected.")
        return redirect(url_for("index"))
    ext_ok = "." in f.filename and f.filename.rsplit(".",1)[1].lower() in ALLOWED_EXT
    if not ext_ok:
        flash("Only PDF files are allowed.")
        return redirect(url_for("index"))

    try:
        pdf_bytes = f.read()
        if len(pdf_bytes) == 0:
            flash("Empty file.")
            return redirect(url_for("index"))
        psm = int(request.form.get("psm", str(TESS_PSM)))
        dpi = int(request.form.get("dpi", str(OCR_DPI_DEFAULT)))
        results, preview = extract_fields_from_bytes(pdf_bytes, ocr_dpi=dpi, tess_psm=psm)

        # Order for display
        order = ["BusinessName", "State", "Industry", "FICO", "LengthOfOwnership"]
        ordered = {k: results[k] for k in order if k in results}
        return render_template_string(TEMPLATE, results=ordered, preview=preview)
    except Exception as e:
        flash(f"Error: {e}")
        return redirect(url_for("index"))

# ----------------------- Run -----------------------
if __name__ == "__main__":
    app = Flask(__name__)
    app.secret_key = os.environ.get("APP_SECRET", "dev")
    app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH
    app.add_url_rule("/", "index", index, methods=["GET"])
    app.add_url_rule("/extract", "extract", extract, methods=["POST"])
    app.run(host="0.0.0.0", port=5000, debug=True)
