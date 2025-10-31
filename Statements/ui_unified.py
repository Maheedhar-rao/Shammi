#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Unified Extractor — Application + Bank Statements
- Single-page UI (drag/drop, preview/remove)
- Uses your Application_extractor & Statements_extractor
- Unwraps FieldResult.value; adapts tuple outputs
- Robust Time-in-Business in months (aliases, free-text, date scan)
- CA/NY 4-month nudge; ONLY eligible lenders shown
- NEW: Bank Statements — Details section (like ui_statements.py), incl. month-by-month list + overall average
"""

import os, re, tempfile, inspect, traceback
from statistics import mean
from typing import Optional, Tuple, Iterable, List, Dict, Any
from flask import Flask, request, render_template_string

# ---- Import extractors ----
try:
    import Application_extractor as appx
except Exception as e:
    raise ImportError("Application_extractor must be importable as 'Application_extractor'.") from e

stx = None
_stx_import_err = None
for mod_name in ("statements_extraction", "Statements_extractor", "statements_extractor"):
    try:
        stx = __import__(mod_name); break
    except Exception as e:
        _stx_import_err = e
if stx is None:
    raise ImportError("Statements extractor not found. Expected statements_extraction.py / Statements_extractor.py") from _stx_import_err

from lenders_rules import select_lenders
from datetime import date as _date
from dateutil import parser as dateparser

MAX_CONTENT_LENGTH = 80 * 1024 * 1024
ALLOWED = {"pdf"}

app = Flask(__name__)
app.secret_key = os.environ.get("APP_SECRET", "dev")
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH

# --------------------------- UI ---------------------------
TEMPLATE = """<!doctype html><html><head><meta charset="utf-8"/>
<title>Unified Extractor — Application + Bank Statements</title>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<style>
:root{--bg:#f6f7fb;--card:#fff;--ink:#111827;--muted:#6b7280;--line:#e5e7eb;--accent:#4f46e5;--warnbg:#fff7ed;--warnborder:#fed7aa;--warntext:#9a3412}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font-family:Inter,system-ui,-apple-system,Segoe UI,Roboto,"Helvetica Neue",Arial,"Noto Sans",sans-serif}
.wrap{max-width:1100px;margin:28px auto;padding:0 16px}header{display:flex;align-items:center;justify-content:space-between;margin-bottom:14px}
h1{margin:0;font-size:22px}.card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:16px;box-shadow:0 4px 16px rgba(0,0,0,.04);margin-bottom:14px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}@media(max-width:1000px){.grid{grid-template-columns:1fr}}
.drop{border:2px dashed var(--line);border-radius:12px;padding:18px;text-align:center;background:#fafbff;cursor:pointer}
.drop.drag{border-color:var(--accent);background:#eef2ff}input[type=file]{position:absolute;left:-9999px;width:1px;height:1px;opacity:0}
.hint{color:var(--muted);font-size:13px}.list{margin-top:10px}.file{display:flex;align-items:center;justify-content:space-between;border:1px solid var(--line);border-radius:10px;padding:8px 10px;margin-top:8px;background:#fff}
.file b{font-weight:600}.btn{background:var(--accent);color:#fff;border:none;padding:10px 16px;border-radius:10px;cursor:pointer}
.btn.small{padding:6px 10px;border-radius:8px}.btn.ghost{background:#fff;color:var(--ink);border:1px solid var(--line)}
.badge{display:inline-block;padding:2px 8px;border-radius:999px;font-size:12px;border:1px solid var(--line)}
.warn{background:var(--warnbg);border-color:var(--warnborder);color:var(--warntext)}.divider{height:1px;background:var(--line);margin:10px 0}
.row{display:grid;grid-template-columns:1fr 1fr;gap:14px}.kv{display:flex;justify-content:space-between;gap:8px}.small{font-size:12px;color:var(--muted)}
.msg{margin-top:8px;padding:8px 10px;border-radius:10px;border:1px solid var(--line);background:#fff}.err{border-color:#fecaca;background:#fff1f2;color:#991b1b}
.h2{font-size:18px;margin:0 0 8px 0}
.subtle{color:var(--muted);font-size:12px}
.item{border:1px solid var(--line);border-radius:10px;padding:10px;margin-top:10px;background:#fff}
.item .ttl{font-weight:600;margin-bottom:6px}
.kvcol{display:grid;grid-template-columns:1fr 1fr;gap:8px}
@media (max-width: 700px){.kvcol{grid-template-columns:1fr}}
.smol{font-size:12px}
</style></head><body><div class="wrap"><header><h1>Unified Extractor — Application + Bank Statements</h1></header>
<div class="card"><div class="grid"><div>
  <div id="dropApp" class="drop"><div><b>Application PDF(s)</b></div><div class="hint">Drag & drop or click to select</div>
    <input type="file" id="application_files" accept=".pdf" multiple /></div><div id="appList" class="list"></div></div>
<div><div id="dropStmt" class="drop"><div><b>Bank Statement PDF(s)</b></div><div class="hint">Upload 3–4 months (CA/NY: must be 4 months)</div>
    <input type="file" id="statement_files" accept=".pdf" multiple /></div><div id="stmtList" class="list"></div></div></div>
<div class="divider"></div><button class="btn" id="submitBtn">Extract & Match Lenders</button>
<button class="btn ghost" id="clearAll">Clear All</button><div id="clientMsg" class="msg" style="display:none;"></div></div>
<div id="results"></div></div>
<script>(function(){const a=document.getElementById('application_files'),s=document.getElementById('statement_files'),
al=document.getElementById('appList'),sl=document.getElementById('stmtList'),da=document.getElementById('dropApp'),
ds=document.getElementById('dropStmt'),c=document.getElementById('clearAll'),b=document.getElementById('submitBtn'),
m=document.getElementById('clientMsg'),r=document.getElementById('results');let af=[],sf=[];
function msg(t,e=!1){m.textContent=t;m.className='msg'+(e?' err':'');m.style.display=t?'block':'none'}
function render(list, files, isApp){list.innerHTML='';files.forEach((f,i)=>{const row=document.createElement('div');row.className='file';
const L=document.createElement('div');L.innerHTML='<b>'+f.name+'</b>';const A=document.createElement('div');
const p=document.createElement('button');p.type='button';p.className='btn small ghost';p.textContent='Preview';
p.onclick=()=>{const u=URL.createObjectURL(f);const w=window.open();if(w){w.document.write('<title>'+f.name+'</title>');
w.document.write('<embed src="'+u+'" type="application/pdf" style="width:100%;height:100vh;"/>')}else{alert('Popup blocked.')}}; 
const rm=document.createElement('button');rm.type='button';rm.className='btn small';rm.textContent='Remove';
rm.onclick=()=>{if(isApp){af.splice(i,1);render(al,af,!0)}else{sf.splice(i,1);render(sl,sf,!1)}};A.appendChild(p);A.appendChild(rm);
row.appendChild(L);row.appendChild(A);list.appendChild(row)})}
function hookDrop(box,input,isApp){box.addEventListener('click',()=>input.click());
box.addEventListener('dragover',e=>{e.preventDefault();box.classList.add('drag')});
box.addEventListener('dragleave',e=>{box.classList.remove('drag')});
box.addEventListener('drop',e=>{e.preventDefault();box.classList.remove('drag');
if(e.dataTransfer.files&&e.dataTransfer.files.length){for(const f of e.dataTransfer.files){if(f.name.toLowerCase().endsWith('.pdf'))(isApp?af:sf).push(f)}
render(isApp?al:sl,isApp?af:sf,isApp)}})}
function hookInput(input,isApp){input.addEventListener('change',()=>{for(const f of input.files){if(f.name.toLowerCase().endsWith('.pdf'))(isApp?af:sf).push(f)}
render(isApp?al:sl,isApp?af:sf,isApp);input.value=''})}
c.addEventListener('click',()=>{af=[];sf=[];render(al,af,!0);render(sl,sf,!1);msg('Cleared selected files.');r.innerHTML=''});
hookDrop(da,a,!0);hookDrop(ds,s,!1);hookInput(a,!0);hookInput(s,!1);
b.addEventListener('click',async()=>{msg('');r.innerHTML='';if(af.length===0||sf.length===0){msg('Please add at least one Application PDF and one Bank Statement PDF.',!0);return}
b.disabled=!0;b.textContent='Extracting...';const fd=new FormData();af.forEach(f=>fd.append('application_files',f,f.name));
fd.append('application_primary',af[0],af[0].name);sf.forEach(f=>fd.append('statement_files',f,f.name));
try{const resp=await fetch('{{ url_for("extract") }}',{method:'POST',body:fd,headers:{'X-Partial':'1'}});const html=await resp.text();
r.innerHTML=html;if(!resp.ok){msg('Server returned an error. See results below.',!0)}}catch(e){console.error(e);msg('Network error while uploading. Check console.',!0)}
finally{b.disabled=!1;b.textContent='Extract & Match Lenders'}});render(al,af,!0);render(sl,sf,!1)})();</script>
</body></html>"""

RESULTS_PARTIAL = """
{% if diag %}
  <div class="card"><h3 class="h2">Diagnostics</h3>
  <div class="small">Application files received: {{ diag.app_files_count }}</div>
  <div class="small">Statement files received: {{ diag.stmt_files_count }}</div>
  <div class="small">App entrypoint used: {{ diag.app_extractor_used }}</div>
  {% if diag.errors %}<details open><summary style="cursor:pointer;">Errors</summary><pre>{{ diag.errors }}</pre></details>{% endif %}
  </div>
{% endif %}

{% if error_msg %}
  <div class="card" style="border-color:#fecaca; background:#fff1f2;"><div style="color:#991b1b; white-space:pre-wrap;">{{ error_msg }}</div></div>
{% endif %}

{% if app_data %}
  <div class="card">
    <div style="display:flex; justify-content:space-between; align-items:center;">
      <h3 class="h2">Application — Parsed Fields</h3>
      {% if app_data.state in ("CA","NY") and (bank_summary and bank_summary.statements_count < 4) %}
        <span class="badge warn">CA/NY requires 4 months of statements</span>
      {% endif %}
    </div>
    <div class="row">
      <div>
        <div class="kv"><span>Business Name</span> <b>{{ app_data.business_name or "[Not Found]" }}</b></div>
        <div class="kv"><span>State</span> <b>{{ app_data.state or "[Not Found]" }}</b></div>
        <div class="kv"><span>Industry</span> <b>{{ app_data.industry or "[Not Found]" }}</b></div>
      </div>
      <div>
        <div class="kv"><span>FICO</span> <b>{{ app_data.fico or "[Not Found]" }}</b></div>
        <div class="kv"><span>Time in Business</span>
          {% if app_data.length_months is not none %}
            <b>{{ "%.1f"|format(app_data.length_months) }} months ({{ "%.2f"|format(app_data.length_months/12.0) }} years)</b>
          {% else %}<b>[Not Found]</b>{% endif %}
        </div>
      </div>
    </div>
  </div>
{% endif %}

{% if stmt_details %}
  <div class="card">
    <h3 class="h2">Bank Statements — Details</h3>
    <div class="subtle">Average Revenue uses deposits only (Zelle excluded).</div>
    {% for it in stmt_details %}
      <div class="item">
        <div class="ttl">{{ it.filename }}</div>
        <div class="kvcol">
          <div class="kv"><span>Bank Name</span> <b>{{ it.bank_name or "[Not Found]" }}</b></div>
          <div class="kv"><span>Business Name</span> <b>{{ it.business_name or "[Not Found]" }}</b></div>
          <div class="kv"><span>Account Number</span> <b>{{ it.account_number or "[Not Found]" }}</b></div>
          <div class="kv"><span>Statement Month</span> <b>{{ it.statement_month or "[unknown]" }}</b></div>
          <div class="kv"><span>Deposits excl Zelle</span> <b>{{ "%.2f"|format(it.deposits_excl_zelle) if it.deposits_excl_zelle is not none else "[N/A]" }}</b></div>
          <div class="kv"><span>Debit Count</span> <b>{{ it.debit_count if it.debit_count is not none else 0 }}</b></div>
          <div class="kv"><span>Credit Count</span> <b>{{ it.credit_count if it.credit_count is not none else 0 }}</b></div>
          <div class="kv"><span>Negative Ending Days</span> <b>{{ it.neg_days if it.neg_days is not none else 0 }}</b></div>
          <div class="kv"><span>Average Daily Balance</span> <b>{{ "%.2f"|format(it.avg_daily_balance) if it.avg_daily_balance is not none else "[N/A]" }}</b></div>
        </div>
        <div class="smol" style="margin-top:6px;">
          Positions - Daily: {{ it.positions_daily if it.positions_daily else "[none]" }}<br/>
          Positions - Weekly: {{ it.positions_weekly if it.positions_weekly else "[none]" }}
        </div>
      </div>
    {% endfor %}
  </div>

  <div class="card">
    <h3 class="h2">Average Revenue (Deposits only, excludes Zelle)</h3>
    <div class="kv"><span>State rule</span> <b>Average of all months</b></div>
    <div class="divider"></div>
    {% for m in avg_rev.months %}
      <div class="kv"><b>{{ m }}</b> <span>{{ "%.2f"|format(avg_rev.values[m]) }}</span></div>
    {% endfor %}
    <div class="divider"></div>
    <div class="kv"><b>AVERAGE</b> <span>{{ "%.2f"|format(avg_rev.average) if avg_rev.average is not none else "[N/A]" }}</span></div>
  </div>
{% endif %}

{% if bank_summary and not stmt_details %}
  <div class="card">
    <h3 class="h2">Bank Statements — Aggregates</h3>
    <div class="row">
      <div>
        <div class="kv"><span>Average Monthly Revenue (deposits only, excl. Zelle)</span> <b>{{ bank_summary.avg_revenue if bank_summary.avg_revenue is not none else "[N/A]" }}</b></div>
        <div class="kv"><span>Avg Daily Balance (most recent parsed)</span> <b>{{ bank_summary.avg_daily_balance if bank_summary.avg_daily_balance is not none else "[N/A]" }}</b></div>
      </div>
      <div>
        <div class="kv"><span>Negative Ending Days (most recent)</span> <b>{{ bank_summary.neg_days if bank_summary.neg_days is not none else "[N/A]" }}</b></div>
        <div class="kv"><span>Average Monthly Credit Count</span> <b>{{ bank_summary.deposit_freq if bank_summary.deposit_freq is not none else "[N/A]" }}</b></div>
      </div>
    </div>
    <div class="small">Statements uploaded: {{ bank_summary.statements_count }}</div>
  </div>
{% endif %}

{% if lender_results is not none %}
  <div class="card"><h3 class="h2">Eligible Lenders</h3><div class="divider"></div>
  {% if lender_results|length == 0 %}<div class="small">No eligible lenders matched the current inputs.</div>
  {% else %}{% for ln in lender_results %}<div class="kv"><b>{{ ln.name }}</b> <span>Eligible</span></div><div class="small">{{ ln.reason }}</div><div class="divider"></div>{% endfor %}{% endif %}
  </div>
{% endif %}
"""

# ----------------------- Helpers -----------------------
STATE_NAME_TO_ABBR = {
    'ALABAMA':'AL','ALASKA':'AK','ARIZONA':'AZ','ARKANSAS':'AR','CALIFORNIA':'CA','COLORADO':'CO','CONNECTICUT':'CT',
    'DELAWARE':'DE','FLORIDA':'FL','GEORGIA':'GA','HAWAII':'HI','IDAHO':'ID','ILLINOIS':'IL','INDIANA':'IN','IOWA':'IA',
    'KANSAS':'KS','KENTUCKY':'KY','LOUISIANA':'LA','MAINE':'ME','MARYLAND':'MD','MASSACHUSETTS':'MA','MICHIGAN':'MI',
    'MINNESOTA':'MN','MISSISSIPPI':'MS','MISSOURI':'MO','MONTANA':'MT','NEBRASKA':'NE','NEVADA':'NV','NEW HAMPSHIRE':'NH',
    'NEW JERSEY':'NJ','NEW MEXICO':'NM','NEW YORK':'NY','NORTH CAROLINA':'NC','NORTH DAKOTA':'ND','OHIO':'OH','OKLAHOMA':'OK',
    'OREGON':'OR','PENNSYLVANIA':'PA','RHODE ISLAND':'RI','SOUTH CAROLINA':'SC','SOUTH DAKOTA':'SD','TENNESSEE':'TN',
    'TEXAS':'TX','UTAH':'UT','VERMONT':'VT','VIRGINIA':'VA','WASHINGTON':'WA','WEST VIRGINIA':'WV','WISCONSIN':'WI','WYOMING':'WY',
    'DISTRICT OF COLUMBIA':'DC','WASHINGTON DC':'DC','WASHINGTON, DC':'DC'
}

def _allowed(fn: str) -> bool:
    return "." in fn and fn.rsplit(".", 1)[1].lower() in ALLOWED

# unwrap helpers
def _unwrap_value(v):
    if v is None or isinstance(v,(int,float,str)): return v
    if isinstance(v,dict) and "value" in v: return v.get("value")
    if hasattr(v,"value"):
        try: return getattr(v,"value")
        except: pass
    if hasattr(v,"get_value") and callable(getattr(v,"get_value")):
        try: return v.get_value()
        except: pass
    if hasattr(v,"_asdict"):
        try:
            d=v._asdict(); return d.get("value", d)
        except: pass
    return v

def _unwrap_evidence_line(v) -> Optional[str]:
    if v is None: return None
    if isinstance(v,dict):
        ev=v.get("evidence") or v.get("Evidence") or {}
        if isinstance(ev,dict):
            line=ev.get("line") or ev.get("LINE")
            if isinstance(line,str): return line
        for sub in ("line","LINE","text","Text"):
            if sub in v and isinstance(v[sub],str): return v[sub]
    if hasattr(v,"evidence"):
        try:
            ev=getattr(v,"evidence")
            if isinstance(ev,dict):
                line=ev.get("line") or ev.get("LINE")
                if isinstance(line,str): return line
        except: pass
    return None

def _pick(d: dict, *keys):
    for k in keys:
        if k in d and d[k] not in (None,""):
            return _unwrap_value(d[k])
    return None

def _to_int3(v):
    v=_unwrap_value(v)
    if v is None: return None
    s=str(v).strip()
    if s.isdigit():
        n=int(s); return n if 300<=n<=900 else None
    m=re.search(r"\b(\d{3})\b",s)
    if m:
        n=int(m.group(1)); return n if 300<=n<=900 else None
    return None

def _state_to_two_letters(v):
    v=_unwrap_value(v)
    if not v: return None
    s=str(v).strip()
    if len(s)==2 and s.isalpha(): return s.upper()
    up=s.upper()
    return STATE_NAME_TO_ABBR.get(up,None) or s

# ---- Time in Business helpers (kept) ----
TIB_MONTH_KEYS = (
    "LengthOfOwnershipMonths","TimeInBusinessMonths","MonthsInBusiness","length_months","TIB_Months",
    "Time in Business (Months)","Time in Business Months","Time-in-Business-Months","Months operating",
    "Months Active","Months of Operation","Months of Operation (Business)"
)
TIB_YEAR_KEYS = (
    "TimeInBusinessYears","YearsInBusiness","LengthOfOwnershipYears","TIB_Years",
    "Time in Business (Years)","Time in Business Years","Time-in-Business-Years","Years operating",
    "Years Active","Years of Operation","Years of Operation (Business)"
)
START_DATE_KEYS = (
    "BusinessStartDate","Business Start Date","StartDate","Established","IncorporationDate",
    "DateOpened","OpeningDate","Date Business Started","Business Inception","Business Start",
    "Founded","Date Founded","Date of Incorporation","Incorporated"
)
FREE_TEXT_TIB_KEYS = (
    "TimeInBusiness","Time in Business","TIB","Business Age","Company Age","Years in Business",
    "Length of Ownership","Ownership Length"
)
APPLICATION_DATE_KEYS = (
    "ApplicationDate","Application Date","FormDate","Date","Application Form Date","Signed Date","Signature Date"
)

DATE_PAT = re.compile(r"""
    (?: (?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\w*,?\s+ )?
    (?:
        \d{1,2}[/-]\d{1,2}[/-]\d{2,4}
        |
        \d{4}[/-]\d{1,2}[/-]\d{1,2}
        |
        (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\w*\s+\d{1,2},?\s+\d{4}
        |
        (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\w*\s+\d{4}
    )
""", re.IGNORECASE | re.VERBOSE)

START_CONTEXT = re.compile(
    r"\b(start(?:ed)?|inception|founded|establish(?:ed|ment)?|incorporat(?:ed|ion)|opened|opening)\b",
    re.IGNORECASE
)

def _parse_date_safe(s: str) -> Optional[_date]:
    try:
        return dateparser.parse(s, fuzzy=True).date()
    except Exception:
        return None

def _parse_free_text_months(s: str) -> Optional[float]:
    if not s: return None
    txt=s.strip().lower()
    m=re.search(r"(\d+(?:\.\d+)?)\s*(months|month|mos|mo)\b",txt)
    if m:
        try: return float(m.group(1))
        except: pass
    ym=re.search(r"(\d+(?:\.\d+)?)\s*(years|year|yrs|yr)\b(?:\s*(\d+(?:\.\d+)?)\s*(months|month|mos|mo))?",txt)
    if ym:
        try:
            y=float(ym.group(1)); mm=float(ym.group(3)) if ym.group(3) else 0.0
            return y*12.0+mm
        except: pass
    y=re.search(r"(\d+(?:\.\d+)?)\s*(years|year|yrs|yr)\b",txt)
    if y:
        try: return float(y.group(1))*12.0
        except: pass
    num=re.search(r"\b(\d+(?:\.\d+)?)\b",txt)
    if num:
        try:
            val=float(num.group(1))
            return val if val<=60 else val*12.0
        except: pass
    return None

def _month_diff(a: _date, b: _date) -> float:
    months=(b.year-a.year)*12+(b.month-a.month)
    if a.day>b.day: months-=1
    return float(max(0,months))

def _iterate_strings_for_date_scan(raw: dict) -> Iterable[str]:
    for _,v in raw.items():
        val=_unwrap_value(v)
        if isinstance(val,str): yield val
        line=_unwrap_evidence_line(v)
        if isinstance(line,str): yield line

def _extract_app_date(raw: dict) -> Optional[_date]:
    for key in APPLICATION_DATE_KEYS:
        if key in raw and raw[key] not in (None,""):
            d=_parse_date_safe(str(_unwrap_value(raw[key])))
            if d: return d
    for s in _iterate_strings_for_date_scan(raw):
        if "application form" in s.lower():
            m=DATE_PAT.search(s)
            if m:
                d=_parse_date_safe(m.group(0))
                if d: return d
    return None

def _extract_start_date_from_scan(raw: dict) -> Optional[_date]:
    best=None
    for s in _iterate_strings_for_date_scan(raw):
        low=s.lower()
        if "business start date" in low or "date business started" in low:
            m=DATE_PAT.search(s)
            if m:
                d=_parse_date_safe(m.group(0))
                if d: best=d; break
        if START_CONTEXT.search(s):
            m=DATE_PAT.search(s)
            if m:
                d=_parse_date_safe(m.group(0))
                if d: best=d
    return best

def _extract_months(raw: dict) -> Optional[float]:
    for key in TIB_MONTH_KEYS:
        if key in raw and raw[key] not in (None,""):
            try: return float(_unwrap_value(raw[key]))
            except: pass
    for key in TIB_YEAR_KEYS:
        if key in raw and raw[key] not in (None,""):
            try: return float(_unwrap_value(raw[key]))*12.0
            except: pass
    for key in FREE_TEXT_TIB_KEYS:
        if key in raw and raw[key] not in (None,""):
            months=_parse_free_text_months(str(_unwrap_value(raw[key])))
            if months is not None: return max(0.0,months)
    for key in START_DATE_KEYS:
        if key in raw and raw[key] not in (None,""):
            d=_parse_date_safe(str(_unwrap_value(raw[key])))
            if d:
                app_d=_extract_app_date(raw) or _date.today()
                return _month_diff(d, app_d)
    d=_extract_start_date_from_scan(raw)
    if d:
        app_d=_extract_app_date(raw) or _date.today()
        return _month_diff(d, app_d)
    return None

def _normalize_app_dict(raw: dict) -> dict:
    business=_pick(raw,"BusinessName","Business Name","Business Legal Name","business_name","Applicant Business Name","DBA","D/B/A","DoingBusinessAs")
    state=_pick(raw,"State","BusinessState","Business State","MailingState","CompanyState")
    industry=_pick(raw,"Industry","BusinessIndustry","NAICS Description","Industry Type","NAICS")
    fico=_to_int3(_pick(raw,"FICO","Fico","CreditScore","Credit Score","Score"))
    months=_extract_months(raw)
    if isinstance(business,str):
        business=business.strip().replace("Business Legal Name","").replace("Business Name","").replace("Applicant Name","").strip(" :-")
    state=_state_to_two_letters(state)
    return {"business_name":business or None,"state":state or None,"industry":industry or None,"fico":fico,"length_months":months}

_ALLOWED_TOP_FN_TOKENS=("extract_fields","summarize_application","extract_application","parse_application","process_file","process_pdf")
_BANNED_HELPER_TOKENS=("_fico","_state","preprocess_","parse_date_candidate","extract_state_stronger","extract_bn_from_window")

def _is_allowed_top_fn(name:str)->bool:
    lname=name.lower()
    if any(b in lname for b in _BANNED_HELPER_TOKENS): return False
    return any(a in lname for a in _ALLOWED_TOP_FN_TOKENS) or lname in ("extract","summarize","process","extract_fields_from_bytes")

def _call_with_bytes(fn,pdf_bytes,filename,label):
    try:
        try: out=fn(pdf_bytes, filename=filename)
        except TypeError: out=fn(pdf_bytes)
        return out,label
    except Exception:
        return None,f"{label}:\n{traceback.format_exc()}"

def _call_with_path(fn,pdf_bytes,filename,label):
    try:
        with tempfile.NamedTemporaryFile(suffix=".pdf",delete=True) as tmp:
            tmp.write(pdf_bytes); tmp.flush()
            try: out=fn(tmp.name, filename=filename)
            except TypeError: out=fn(tmp.name)
        return out,label
    except Exception:
        return None,f"{label}:\n{traceback.format_exc()}"

def _adapt_app_output(obj)->Optional[dict]:
    if isinstance(obj,dict): return obj
    if hasattr(obj,"_asdict"):
        try: return obj._asdict()
        except: pass
    if isinstance(obj,tuple):
        for el in obj:
            if isinstance(el,dict): return el
            if hasattr(el,"_asdict"):
                try: return el._asdict()
                except: pass
        if len(obj)>=5:
            try:
                b,s,i,f,m=obj[0],obj[1],obj[2],obj[3],obj[4]
                maybe={}
                if b not in (None,""): maybe["BusinessName"]=b
                if s not in (None,""): maybe["State"]=s
                if i not in (None,""): maybe["Industry"]=i
                if f not in (None,""): maybe["FICO"]=f
                if m not in (None,""): maybe["LengthOfOwnershipMonths"]=m
                return maybe or None
            except: pass
    if hasattr(obj,"__dict__"):
        d={k:getattr(obj,k) for k in dir(obj) if not k.startswith("_")}
        if any(k in d for k in ("BusinessName","State","Industry","FICO","LengthOfOwnershipMonths","TimeInBusiness","BusinessStartDate","ApplicationDate")):
            return d
    return None

def _call_app_extractor(pdf_bytes:bytes, filename:Optional[str])->Tuple[Optional[dict],str,Optional[str]]:
    errors=[]
    known=["extract_fields_from_bytes","summarize_application_from_bytes","extract_application_from_bytes",
           "summarize_application","extract_application","summarize_application_from_path",
           "process_file","process_pdf","parse_application","extract"]
    for name in known:
        fn=getattr(appx,name,None)
        if not callable(fn): continue
        lname=name.lower()
        if "bytes" in lname or "from_bytes" in lname:
            out,used=_call_with_bytes(fn,pdf_bytes,filename,f"{name}(bytes)")
        elif "path" in lname or "from_path" in lname or name in ("process_file","process_pdf","parse_application"):
            out,used=_call_with_path(fn,pdf_bytes,filename,f"{name}(path)")
        else:
            out,used=_call_with_bytes(fn,pdf_bytes,filename,f"{name}(bytes)")
            if out is None:
                errors.append(used)
                out,used=_call_with_path(fn,pdf_bytes,filename,f"{name}(path)")
        if out is not None:
            adapted=_adapt_app_output(out)
            if isinstance(adapted,dict): return adapted,used,None
            else: errors.append(f"{used}: returned non-dict ({type(out).__name__})")
    for name,fn in inspect.getmembers(appx,inspect.isfunction):
        lname=name.lower()
        if not (any(t in lname for t in _ALLOWED_TOP_FN_TOKENS) or lname in ("extract","summarize","process","extract_fields_from_bytes")): 
            continue
        if any(b in lname for b in _BANNED_HELPER_TOKENS): 
            continue
        if "bytes" in lname or "from_bytes" in lname:
            out,used=_call_with_bytes(fn,pdf_bytes,filename,f"{name}(bytes)")
        elif "path" in lname or "from_path" in lname:
            out,used=_call_with_path(fn,pdf_bytes,filename,f"{name}(path)")
        else:
            out,used=_call_with_bytes(fn,pdf_bytes,filename,f"{name}(bytes)")
            if out is None:
                errors.append(used)
                out,used=_call_with_path(fn,pdf_bytes,filename,f"{name}(path)")
        if out is not None:
            adapted=_adapt_app_output(out)
            if isinstance(adapted,dict): return adapted,used,None
            else: errors.append(f"{used}: returned non-dict ({type(out).__name__})")
    return None,"no-app-entrypoint-found","\n".join(errors) if errors else "No callable produced a dict."

def _call_statements_from_bytes(pdf_bytes:bytes, filename:Optional[str]):
    def try_bytes(fn,label):
        try:
            try: out=fn(pdf_bytes, filename=filename)
            except TypeError: out=fn(pdf_bytes)
            return out,label
        except Exception:
            return None,f"{label}:\n{traceback.format_exc()}"
    def try_path(fn,label):
        try:
            with tempfile.NamedTemporaryFile(suffix=".pdf",delete=True) as tmp:
                tmp.write(pdf_bytes); tmp.flush()
                try: out=fn(tmp.name, filename=filename)
                except TypeError: out=fn(tmp.name)
            return out,label
        except Exception:
            return None,f"{label}:\n{traceback.format_exc()}"
    known=[("summarize_statement_from_bytes","bytes"),("summarize_from_bytes","bytes"),
           ("parse_statement_from_bytes","bytes"),("summarize_statement","bytes"),
           ("summarize_statement_from_path","path"),("summarize_from_path","path"),("parse_statement","path")]
    for name,mode in known:
        fn=getattr(stx,name,None)
        if callable(fn):
            out,used=(try_bytes if mode=="bytes" else try_path)(fn,f"{name}({mode})")
            if out is not None: return out,used,None
    for name,fn in inspect.getmembers(stx,inspect.isfunction):
        lname=name.lower()
        if not any(t in lname for t in ("statement","stmt","bank","extract","summar","parse")): continue
        out,used=try_bytes(fn,f"{name}(bytes)")
        if out is not None: return out,used,None
        out,used=try_path(fn,f"{name}(path)")
        if out is not None: return out,used,None
    return None,"no-statement-entrypoint-found","No callable accepted."

# ---------------------------- Routes ----------------------------
@app.route("/", methods=["GET"])
def index():
    return render_template_string(TEMPLATE)

@app.route("/extract", methods=["POST"])
def extract():
    app_files=request.files.getlist("application_files")
    stmt_files=request.files.getlist("statement_files")
    _=request.files.get("application_primary")

    class D: pass
    diag=D(); diag.app_files_count=sum(1 for f in app_files if f and f.filename and _allowed(f.filename))
    diag.stmt_files_count=sum(1 for f in stmt_files if f and f.filename and _allowed(f.filename))
    diag.app_extractor_used="n/a"; diag.errors=""

    error_msg=None; app_data_norm=None

    # ----- Application -----
    if app_files:
        for f in app_files:
            if not f or not f.filename or not _allowed(f.filename): continue
            data=f.read()
            if not data: continue
            raw,used,err=_call_app_extractor(data,filename=f.filename)
            diag.app_extractor_used=used
            if err: diag.errors+=f"[Application] {err}\n"
            if isinstance(raw,dict):
                app_data_norm=_normalize_app_dict(raw); break
        if app_data_norm is None:
            error_msg=(error_msg or "")+"Application parsing returned no usable dict. "
    else:
        error_msg="Please upload at least one Application PDF."

    # ----- Statements (details + aggregates like ui_statements.py) -----
    per_month_deposits: Dict[str,float] = {}
    per_month_credit_counts: Dict[str,int] = {}
    months_seen = set()
    statements_count=0
    latest_month = None
    latest_adb = None
    latest_neg_days = None
    stmt_details: List[Dict[str,Any]] = []

    def _g(o, name, default=None): return getattr(o, name, default) if o is not None else default

    for f in stmt_files:
        if not f or not f.filename or not _allowed(f.filename): continue
        data=f.read()
        if not data: continue
        out,used,err=_call_statements_from_bytes(data,filename=f.filename)
        if err: diag.errors+=f"[Statements:{f.filename}] {err}\n"
        if out is None: continue
        statements_count+=1

        summary=out[0] if (isinstance(out,tuple) and len(out)>=1) else out

        m = _g(summary,"statement_month",None)
        bank_name = _g(summary,"bank_name",None)
        business_name = _g(summary,"business_name",None)
        account_number = _g(summary,"account_number",None)
        deposits_excl_zelle = _g(summary,"monthly_deposits_excl_zelle",0.0) or 0.0
        debit_count = _g(summary,"debit_count",0) or 0
        credit_count = _g(summary,"credit_count",0) or 0
        neg_days = _g(summary,"negative_ending_days",0) or 0
        avg_daily_balance = _g(summary,"average_daily_balance",None)
        positions_daily = _g(summary,"positions_daily",None)
        positions_weekly = _g(summary,"positions_weekly",None)
        # fallback: sometimes positions appear under different names
        if not positions_daily: positions_daily = _g(summary, "positions_daily_text", None)
        if not positions_weekly: positions_weekly = _g(summary, "positions_weekly_text", None)

        stmt_details.append({
            "filename": f.filename,
            "bank_name": bank_name,
            "business_name": business_name,
            "account_number": account_number,
            "statement_month": m or "[unknown]",
            "deposits_excl_zelle": float(deposits_excl_zelle) if deposits_excl_zelle is not None else None,
            "debit_count": debit_count,
            "credit_count": credit_count,
            "neg_days": neg_days,
            "avg_daily_balance": float(avg_daily_balance) if avg_daily_balance is not None else None,
            "positions_daily": positions_daily if positions_daily else None,
            "positions_weekly": positions_weekly if positions_weekly else None,
        })

        if m and m != "[unknown]":
            per_month_deposits[m] = per_month_deposits.get(m, 0.0) + (float(deposits_excl_zelle) if deposits_excl_zelle is not None else 0.0)
            per_month_credit_counts[m] = per_month_credit_counts.get(m, 0) + (int(credit_count) if credit_count is not None else 0)
            if (latest_month is None) or (m >= latest_month):
                latest_month = m
                latest_adb = avg_daily_balance if avg_daily_balance is not None else latest_adb
                latest_neg_days = neg_days if neg_days is not None else latest_neg_days
            months_seen.add(m)

    # Aggregates
    lender_results=None
    bank_summary=None
    app_data_obj=None
    avg_rev = None

    if app_data_norm:
        months_sorted = sorted(per_month_deposits.keys())
        deposits_vals=[per_month_deposits[m] for m in months_sorted]
        avg_rev_all = round(mean(deposits_vals),2) if deposits_vals else None
        credit_vals = [per_month_credit_counts[m] for m in months_sorted] if months_sorted else []
        deposit_freq = round(mean(credit_vals),1) if credit_vals else None

        bank_metrics={"avg_revenue":avg_rev_all,"avg_daily_balance":latest_adb,"neg_days":latest_neg_days,"deposit_freq":deposit_freq,"positions":0}
        all_lenders=select_lenders(app_data_norm,bank_metrics,statements_count)
        lender_results=[x for x in all_lenders if x.get("eligible")]

        class S: pass
        app_data_obj=S()
        app_data_obj.business_name=app_data_norm.get("business_name")
        app_data_obj.state=(app_data_norm.get("state") or "").strip().upper() if app_data_norm.get("state") else None
        app_data_obj.industry=app_data_norm.get("industry")
        app_data_obj.fico=app_data_norm.get("fico")
        app_data_obj.length_months=app_data_norm.get("length_months")

        bank_summary=S()
        bank_summary.avg_revenue=avg_rev_all
        bank_summary.avg_daily_balance=latest_adb
        bank_summary.neg_days=latest_neg_days
        bank_summary.deposit_freq=deposit_freq
        bank_summary.statements_count=statements_count

        # For UI section that lists months then overall average
        class AR: pass
        avg_rev=AR()
        avg_rev.months = months_sorted
        avg_rev.values = {m: per_month_deposits[m] for m in months_sorted}
        avg_rev.average = avg_rev_all

    return render_template_string(
        RESULTS_PARTIAL,
        app_data=app_data_obj,
        bank_summary=bank_summary,
        lender_results=lender_results,
        stmt_details=stmt_details if stmt_details else None,
        avg_rev=avg_rev,
        diag=diag,
        error_msg=error_msg
    )

@app.route("/healthz")
def healthz():
    return "ok", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=6060, debug=True)
