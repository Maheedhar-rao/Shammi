# viewers.py  (Python 3.9 compatible)
import os
import io
import uuid
import hashlib
from pathlib import Path
from typing import Optional, Iterable

from fastapi import FastAPI, Request, HTTPException, Depends, Query, File, UploadFile, Form
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from supabase import create_client, Client
from pikepdf import Pdf
import aiofiles

# Import your wrapper creator (same folder)
from wrappers import issue_wrapper_user_branded

# --- Optional: load environment from .env ---
try:
    from dotenv import load_dotenv  # pip install python-dotenv
    load_dotenv()
except Exception:
    pass

# ---------------- Env & App ----------------
SUPABASE_URL: Optional[str] = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE: Optional[str] = os.environ.get("SUPABASE_SERVICE_ROLE")

# Use project-local default so dev doesn't hit /var permissions
WRAPPER_STORAGE_DIR: str = os.environ.get(
    "WRAPPER_STORAGE_DIR",
    os.path.join(os.getcwd(), "uploads", "wrappers")
)

# Dev mode: relaxed auth + bypass allowlist checks + extra sandbox helpers
DEV_MODE = os.getenv("DEV_MODE", "1") == "1"  # set DEV_MODE=0 in prod
DEV_FAKE_USER_ID = os.getenv("DEV_FAKE_USER_ID")  # optional override to satisfy FK quickly

app = FastAPI(title="Secure Document Portal")

# Lazy-initialized Supabase client
_sb: Optional[Client] = None
def supabase() -> Client:
    global _sb
    if _sb is None:
        if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE:
            raise HTTPException(
                status_code=500,
                detail="Supabase env vars missing: set SUPABASE_URL and SUPABASE_SERVICE_ROLE"
            )
        _sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE)
    return _sb

# ---------------- Auth Helpers ----------------
async def get_verified_email(request: Request) -> str:
    email = request.headers.get("X-User-Email")
    if not email:
        raise HTTPException(status_code=401, detail="Login required")
    return email.lower()

async def get_relaxed_email(request: Request, as_param: Optional[str] = Query(default=None)) -> str:
    email = request.headers.get("X-User-Email") or as_param
    if email:
        return email.lower()
    if DEV_MODE:
        return "dev@example.com"
    raise HTTPException(status_code=401, detail="Login required")

# ---------------- DB Helpers ----------------
def _lookup_doc(tracking_id: str) -> dict:
    res = supabase().table("doc_tracking").select("*").eq("tracking_id", tracking_id).limit(1).execute()
    rows = getattr(res, "data", None) or []
    if not rows:
        raise HTTPException(status_code=404, detail="Document not found")
    return rows[0]

def _is_email_allowed_for_deal(email: str, deal_id: int) -> bool:
    # In dev, bypass allowlist to avoid writes to non-updatable views
    if DEV_MODE:
        return True
    res = supabase().table("deal_allowed_emails").select("email").eq("deal_id", deal_id).execute()
    rows = getattr(res, "data", None) or []
    allowed = {r["email"].lower() for r in rows}
    return email.lower() in allowed

def _ensure_user_id_for_email(sb: Client, email: str, prefer_user_id: Optional[str] = None) -> str:
    """
    Ensure we have a users.id that satisfies doc_tracking.user_id FK.

    Order of resolution (dev-friendly):
      1) If caller provided a user_id (query/env), trust it.
      2) Lookup users by email.
      3) Try to INSERT a minimal row (several payload shapes, in case of NOT NULLs).
      4) Fallback to any existing users.id.
      5) If DEV_FAKE_USER_ID env set, use that.
      6) Fail with clear message.
    """
    # 1) Explicit override
    if prefer_user_id:
        return prefer_user_id

    # 2) Lookup by email
    try:
        res = sb.table("users").select("id").eq("email", email).limit(1).execute()
        rows = getattr(res, "data", None) or []
        if rows:
            return rows[0]["id"]
    except Exception:
        pass

    # 3) Try inserts with different payloads
    new_id = str(uuid.uuid4())
    candidates = [
        {"id": new_id, "email": email},  # simplest
        {"id": new_id, "email": email, "name": "Sandbox User"},
        {"id": new_id, "email": email, "full_name": "Sandbox User"},
        {"id": new_id, "email": email, "created_at": "now()"},
    ]
    for payload in candidates:
        try:
            sb.table("users").insert(payload).execute()
            return new_id
        except Exception:
            continue

    # 4) Fallback: any existing user
    try:
        any_res = sb.table("users").select("id").limit(1).execute()
        any_rows = getattr(any_res, "data", None) or []
        if any_rows:
            return any_rows[0]["id"]
    except Exception:
        pass

    # 5) DEV override
    if DEV_MODE and DEV_FAKE_USER_ID:
        return DEV_FAKE_USER_ID

    # 6) Give a clear error
    raise HTTPException(
        status_code=500,
        detail="Sandbox needs a valid users.id. Either create a user row (id,email), "
               "set DEV_FAKE_USER_ID, or pass ?user_id=<existing-users.id>."
    )

async def _stream_file(path: str) -> Iterable[bytes]:
    async with aiofiles.open(path, "rb") as f:
        while True:
            chunk = await f.read(1024 * 1024)
            if not chunk:
                break
            yield chunk

def _extract_first_pdf_attachment(wrapper_path: str) -> bytes:
    """
    Extract the first embedded PDF from the wrapper (original statement).
    Falls back to the first embedded file if none end with .pdf.
    """
    with Pdf.open(wrapper_path) as pdf:
        names = pdf.Root.get("/Names")
        if not names or "/EmbeddedFiles" not in names:
            raise HTTPException(status_code=400, detail="Wrapper has no embedded original")
        ef = names["/EmbeddedFiles"]["/Names"]
        chosen = None
        for i in range(0, len(ef), 2):
            fname = str(ef[i])
            fs = ef[i + 1]
            data = bytes(fs["/EF"]["/F"].read_bytes())
            if fname.lower().endswith(".pdf"):
                return data
            if chosen is None:
                chosen = data
        if chosen is None:
            raise HTTPException(status_code=400, detail="No embedded files found")
        return chosen

def _log_view(tr: dict, email: str, req: Request) -> None:
    supabase().table("doc_view_log").insert({
        "tracking_id": tr["tracking_id"],
        "deal_id": tr["deal_id"],
        "business_name": tr.get("business_name"),
        "email": email,
        "ip": req.client.host if req.client else None,
        "user_agent": req.headers.get("user-agent"),
    }).execute()

def _log_download(tr: dict, email: str, req: Request) -> None:
    supabase().table("doc_download_log").insert({
        "tracking_id": tr["tracking_id"],
        "deal_id": tr["deal_id"],
        "business_name": tr.get("business_name"),
        "email": email,
        "ip": req.client.host if req.client else None,
        "user_agent": req.headers.get("user-agent"),
    }).execute()

# ---------------- Routes ----------------

@app.get("/health")
def health():
    return {"ok": True, "dev_mode": DEV_MODE, "storage": WRAPPER_STORAGE_DIR}

@app.get("/v/{tracking_id}", response_class=HTMLResponse)
async def view_page(
    tracking_id: str,
    request: Request,
    user_email: str = Depends(get_relaxed_email),
):
    tr = _lookup_doc(tracking_id)
    if not _is_email_allowed_for_deal(user_email, tr["deal_id"]):
        raise HTTPException(status_code=403, detail="Not authorized for this deal")
    biz = tr.get("business_name") or "Business"
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>{biz} — Documents</title></head>
<body style="margin:0;background:#0b0b0c">
  <div style="display:flex;gap:10px;align-items:center;padding:10px;background:#111;color:#eee;font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;">
    <div style="font-weight:600">Deal: {tr["deal_id"]}</div>
    <div>Recipient: {user_email}</div>
    <div style="margin-left:auto"></div>
    <a href="/download/{tracking_id}?as={user_email}" style="background:#2563eb;color:#fff;padding:6px 12px;border-radius:8px;text-decoration:none">Download original</a>
    <a href="/download/{tracking_id}?what=wrapper&as={user_email}" style="background:#374151;color:#fff;padding:6px 12px;border-radius:8px;text-decoration:none;margin-left:8px">Download wrapper</a>
  </div>
  <iframe src="/file/{tracking_id}?as={user_email}" style="width:100%;height:calc(100vh - 48px);border:0;background:#1a1a1b"></iframe>
</body></html>"""

@app.get("/file/{tracking_id}")
async def file_inline(
    tracking_id: str,
    request: Request,
    user_email: str = Depends(get_relaxed_email),
):
    tr = _lookup_doc(tracking_id)
    if not _is_email_allowed_for_deal(user_email, tr["deal_id"]):
        raise HTTPException(status_code=403, detail="Not authorized for this deal")
    _log_view(tr, user_email, request)
    wrapper_path = os.path.join(WRAPPER_STORAGE_DIR, tr["wrapper_filename"])
    if not os.path.exists(wrapper_path):
        raise HTTPException(status_code=404, detail="Wrapper file missing")
    return StreamingResponse(_stream_file(wrapper_path), media_type="application/pdf")

@app.get("/download/{tracking_id}")
async def download(
    tracking_id: str,
    request: Request,
    user_email: str = Depends(get_relaxed_email),
    what: str = Query(default="original", regex="^(original|wrapper)$"),
):
    tr = _lookup_doc(tracking_id)
    if not _is_email_allowed_for_deal(user_email, tr["deal_id"]):
        raise HTTPException(status_code=403, detail="Not authorized for this deal")

    wrapper_path = os.path.join(WRAPPER_STORAGE_DIR, tr["wrapper_filename"])
    if not os.path.exists(wrapper_path):
        raise HTTPException(status_code=404, detail="Wrapper file missing")

    _log_download(tr, user_email, request)

    if what == "wrapper":
        fname = f'{(tr.get("business_name") or "Business").replace(" ","_")}_Documents.pdf'
        return StreamingResponse(
            _stream_file(wrapper_path),
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{fname}"'}
        )

    # what == original
    original_bytes = _extract_first_pdf_attachment(wrapper_path)
    fname = f'{(tr.get("business_name") or "Business").replace(" ","_")}_Statements.pdf'
    return StreamingResponse(
        io.BytesIO(original_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'}
    )

@app.get("/track")
def track_json(deal_id: Optional[int] = Query(default=None), limit: int = 200):
    q = supabase().table("v_doc_status").select("*").order("issued_at", desc=True).limit(limit)
    if deal_id is not None:
        q = q.eq("deal_id", deal_id)
    rows = q.execute().data or []
    return {"items": rows, "count": len(rows)}

# ---------------- Sandbox: wrap & preview (no emails) ----------------

@app.get("/sandbox", response_class=HTMLResponse)
async def sandbox_page(
    user_email: str = Depends(get_relaxed_email),
    user_id: Optional[str] = Query(default=None),
):
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Sandbox Wrap</title></head>
<body style="font-family:system-ui;padding:24px">
  <h2>Wrap a PDF (no emails sent)</h2>
  <p>Using email: <b>{user_email}</b></p>
  <form id="f" method="post" action="/sandbox/wrap?as={user_email}" enctype="multipart/form-data">
    <div style="margin:8px 0">
      <label>PDF file:</label>
      <input type="file" name="file" accept="application/pdf" required>
    </div>
    <div style="margin:8px 0">
      <label>Funder name:</label>
      <input type="text" name="funder_name" value="Preview Funder">
    </div>
    <div style="margin:8px 0">
      <label>Deal ID (optional):</label>
      <input type="number" name="deal_id" min="0" value="0">
    </div>
    <div style="margin:8px 0">
      <label>Optional users.id (to satisfy FK):</label>
      <input type="text" name="user_id" placeholder="existing users.id UUID">
    </div>
    <button type="submit">Wrap & Get Link</button>
  </form>
  <pre id="out" style="margin-top:16px;background:#111;color:#fafafa;padding:12px;border-radius:8px;display:none"></pre>
  <script>
    const form = document.getElementById('f');
    const out = document.getElementById('out');
    form.addEventListener('submit', async (e) => {{
      e.preventDefault();
      const fd = new FormData(form);
      const res = await fetch(form.action, {{ method: 'POST', body: fd }});
      const js = await res.json();
      out.style.display = 'block';
      out.textContent = JSON.stringify(js, null, 2);
      if (js.view_url) {{
        const a = document.createElement('a');
        a.href = js.view_url + '?as={user_email}';
        a.textContent = 'Open viewer';
        a.style.display = 'inline-block';
        a.style.marginTop = '10px';
        document.body.appendChild(a);
      }}
    }});
  </script>
</body></html>"""

@app.post("/sandbox/wrap")
async def sandbox_wrap(
    request: Request,
    file: UploadFile = File(...),
    funder_name: str = Form("Preview Funder"),
    deal_id: int = Form(0),
    form_user_id: Optional[str] = Form(default=None),
    user_email: str = Depends(get_relaxed_email),
    qp_user_id: Optional[str] = Query(default=None),  # also accept ?user_id=
):
    """
    Upload a PDF, create a wrapped copy + tracking row, and return links.
    No emails and no writes to non-updatable views.
    """
    # 1) Ensure storage directory exists
    Path(WRAPPER_STORAGE_DIR).mkdir(parents=True, exist_ok=True)

    # 2) Save upload to a temp path
    tmpdir = Path("/tmp/viewer_sandbox")
    tmpdir.mkdir(parents=True, exist_ok=True)
    filename = file.filename or "upload.pdf"
    if not filename.lower().endswith(".pdf"):
        filename += ".pdf"
    in_path = tmpdir / f"{os.getpid()}_{filename}"
    async with aiofiles.open(in_path, "wb") as f:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            await f.write(chunk)

    sb = supabase()

    # 3) Resolve a users.id that satisfies FK
    prefer_user_id = form_user_id or qp_user_id or DEV_FAKE_USER_ID
    try:
        user_id = _ensure_user_id_for_email(sb, user_email, prefer_user_id=prefer_user_id)
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"User resolution failed: {e}")

    # 4) Create a sandbox deal row if none provided (writes to a REAL table)
    if deal_id == 0:
        try:
            dres = sb.table("deals").insert({
                "application_json": {"business_name": "Sandbox Business", "email": user_email}
            }).execute()
            deal_id = dres.data[0]["id"]
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to create sandbox deal: {e}")

    # 5) Call your wrapper
    try:
        result = issue_wrapper_user_branded(
            user_id=user_id,
            deal_id=deal_id,
            original_pdf_path=str(in_path),
            funder_name=funder_name,
            recipient_email=user_email,
            storage_dir=WRAPPER_STORAGE_DIR,
            supabase_url=SUPABASE_URL,
            supabase_service_key=SUPABASE_SERVICE_ROLE
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Wrapper failed: {e}")

    # 6) Return direct links
    tracking_id = result["tracking_id"]
    return JSONResponse({
        "ok": True,
        "tracking_id": tracking_id,
        "user_id": user_id,
        "view_url": f"/v/{tracking_id}",
        "download_wrapper": f"/download/{tracking_id}?what=wrapper",
        "download_original": f"/download/{tracking_id}?what=original",
        "wrapper_path": result["wrapper_path"]
    })
