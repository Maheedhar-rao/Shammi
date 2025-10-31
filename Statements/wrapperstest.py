
import os
import uuid
import hashlib
import base64
import json
import tempfile
import subprocess
from datetime import datetime, timezone
from typing import Optional, Tuple, List

from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib.utils import ImageReader

from PyPDF2 import PdfReader, PdfWriter

from pikepdf import Pdf, Name, Dictionary, Stream

from supabase import create_client, Client


# -------------------------------------------------------------------
# Helpers: hashing, embedding, branding, cover (optional), fingerprint
# -------------------------------------------------------------------
def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _embed_file(pdf: Pdf, file_bytes: bytes, filename: str, desc: str = ""):
    fs_stream = pdf.make_indirect(Stream(pdf, file_bytes))
    fs = pdf.make_indirect(Dictionary(
        Type=Name("/FileSpec"),   # FIX: leading slash
        F=filename,
        UF=filename,
        EF=Dictionary(F=fs_stream),
        Desc=desc or f"Embedded {filename}",
    ))
    if "/Names" not in pdf.Root:
        pdf.Root.Names = Dictionary()
    if "/EmbeddedFiles" not in pdf.Root.Names:
        pdf.Root.Names.EmbeddedFiles = Dictionary(Names=[])
    pdf.Root.Names.EmbeddedFiles.Names.append(filename)
    pdf.Root.Names.EmbeddedFiles.Names.append(fs)
    if "/AF" not in pdf.Root:
        pdf.Root.AF = []
    pdf.Root.AF.append(fs)


def _resolve_user_branding(sb: Client, user_id: str) -> Tuple[str, Optional[str]]:
    """
    Returns (org_name, logo_path) with graceful fallbacks:
      1) supabase.user_branding.logo_path
      2) /var/app/assets/logos/{user_id}.png
      3) ./static/logo.png
    """
    b = (
        sb.table("user_branding")
        .select("org_name,logo_path")
        .eq("user_id", user_id)
        .limit(1)
        .execute()
        .data
    )
    user_name = (b and b[0].get("org_name")) or "Pathway Catalyst"
    logo_path = (b and b[0].get("logo_path")) or None

    if not logo_path:
        candidate = f"/var/app/assets/logos/{user_id}.png"
        if os.path.exists(candidate):
            logo_path = candidate

    if not logo_path:
        fallback = os.path.abspath(os.path.join("static", "logo.png"))
        if os.path.exists(fallback):
            logo_path = fallback

    return user_name, logo_path


def _make_cover(cover_path: str, *, logo_path: Optional[str], user_name: str,
                user_tag: str, funder_name: str, deal_id: str,
                business_name: str, recipient_email: str, tracking_id: str):
    """
    Optional cover generator (not required for overlay wrapping).
    Kept for compatibility; you can prepend if you ever want a front page.
    """
    w, h = letter
    c = canvas.Canvas(cover_path, pagesize=letter)

    if logo_path and os.path.exists(logo_path):
        try:
            c.drawImage(ImageReader(logo_path), 0.75*inch, h-1.6*inch,
                        width=1.5*inch, height=1.0*inch, preserveAspectRatio=True, mask='auto')
        except Exception:
            pass

    c.setFont("Helvetica-Bold", 18)
    c.drawString(2.5*inch, h-1.0*inch, f"{user_name} Submission")

    c.setFont("Helvetica-Bold", 16)
    c.drawString(0.75*inch, h-2.0*inch, f"Funder: {funder_name}")
    c.setFont("Helvetica", 12)
    c.drawString(0.75*inch, h-2.35*inch, f"Deal ID: {deal_id}")
    c.drawString(0.75*inch, h-2.55*inch, f"Business: {business_name}")
    c.drawString(0.75*inch, h-2.75*inch, f"Recipient: {recipient_email}")
    c.drawString(0.75*inch, h-2.95*inch, f"Tracking ID: {tracking_id}")
    c.drawString(0.75*inch, h-3.15*inch, f"Issued: {datetime.now(timezone.utc).isoformat()}")

    c.saveState()
    c.setFont("Helvetica-Bold", 50)
    c.setFillGray(0.90)
    c.translate(w/2, h/2); c.rotate(45)
    c.drawCentredString(0, 0, f"{user_name.upper()} • {user_tag}")
    c.restoreState()

    c.setFont("Helvetica", 9)
    c.drawString(0.75*inch, 0.75*inch, f"Issued by: {user_name} • {user_tag} • {tracking_id[:8]}")
    c.showPage(); c.save()


def _apply_invisible_fingerprint(pdf: Pdf, *, fingerprint_id: str,
                                 recipient_email: str, deal_id: str, user_id: str):
    info = pdf.docinfo or Dictionary()
    info[Name("/PCP_Fingerprint")] = fingerprint_id
    info[Name("/PCP_Recipient")] = recipient_email
    info[Name("/PCP_Deal")] = str(deal_id)
    info[Name("/PCP_UserId")] = str(user_id)
    info[Name("/PCP_IssuedAt")] = datetime.now(timezone.utc).isoformat()
    pdf.docinfo = info

    payload = {
        "fp": fingerprint_id,
        "r": recipient_email,
        "d": str(deal_id),
        "u": str(user_id),
        "t": datetime.now(timezone.utc).isoformat(),
    }
    raw = base64.b64encode(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    _ = pdf.make_indirect(Stream(pdf, raw))  # inert payload object


# ----------------------------------------------------------
# Overlay building & composition (wrap every page with logo)
# ----------------------------------------------------------
def _probe_page_sizes(input_pdf: str) -> List[Tuple[float, float]]:
    r = PdfReader(input_pdf)
    sizes = []
    for p in r.pages:
        mb = p.mediabox
        w = float(mb.right) - float(mb.left)
        h = float(mb.top) - float(mb.bottom)
        sizes.append((w, h))
    return sizes


def _build_multi_page_overlay(
    overlay_path: str,
    page_sizes: List[Tuple[float, float]],
    logo_path: Optional[str],
    footer_text: Optional[str] = None,
    *,
    LOGO_MAX_IN: float = 0.9,      # <= make this smaller for a smaller logo (inches)
    LOGO_MAX_PCT: float = 0.12,    # <= or shrink this to cap at e.g. 8–10% of page width
    MARGIN_IN: float = 0.4,        # corner margin in inches
) -> None:
    """
    Create a multi-page PDF overlay that draws a small logo in the top-right
    and optional footer text. Logo scales to page size with aspect preserved.
    """
    c = None
    img_reader = None
    img_w = img_h = None
    if logo_path and os.path.exists(logo_path):
        try:
            img_reader = ImageReader(logo_path)
            img_w, img_h = img_reader.getSize()  # pixels; ratio only matters
        except Exception:
            img_reader = None

    for (w, h) in page_sizes:
        if c is None:
            c = canvas.Canvas(overlay_path, pagesize=(w, h))
        else:
            c.setPageSize((w, h))

        # --- top-right logo sized by page width & max inches ---
        if img_reader:
            try:
                # compute target width in points (1 inch = 72 pt)
                max_width_pts = min(LOGO_MAX_IN * inch, w * LOGO_MAX_PCT)
                # preserve aspect ratio using source image ratio
                # (if source dims unknown, fall back to a sensible box)
                if img_w and img_h and img_w > 0:
                    target_w = max_width_pts
                    target_h = target_w * (img_h / img_w)
                else:
                    # fallback box
                    target_w = max_width_pts
                    target_h = max_width_pts * 0.7

                margin = MARGIN_IN * inch
                x = w - target_w - margin
                y = h - target_h - margin

                c.drawImage(
                    img_reader,
                    x=x,
                    y=y,
                    width=target_w,
                    height=target_h,
                    preserveAspectRatio=True,
                    mask="auto",
                )
            except Exception:
                pass

        # --- footer text (optional) ---
        if footer_text:
            c.setFont("Helvetica", 9)
            c.drawString(0.6 * inch, 0.6 * inch, footer_text)

        c.showPage()

    if c:
        c.save()



def _overlay_with_qpdf(input_pdf: str, overlay_pdf: str, output_pdf: str) -> None:
    """
    Preferred path: qpdf overlays per-page content streams without rasterizing.
    """
    cmd = ["qpdf", "--overlay", overlay_pdf, "--", input_pdf, output_pdf]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.returncode != 0:
        raise RuntimeError(f"qpdf overlay failed: {proc.stderr.decode('utf-8', 'ignore')}")


def _overlay_fallback_python(input_pdf: str, overlay_pdf: str, output_pdf: str) -> None:
    """
    Fallback when qpdf is not available. Uses PyPDF2.merge_page which keeps
    vector content. This still preserves layout, though qpdf is more robust.
    """
    base = PdfReader(input_pdf)
    over = PdfReader(overlay_pdf)
    writer = PdfWriter()

    multi = (len(over.pages) == len(base.pages))
    over0 = over.pages[0] if len(over.pages) else None

    for i, page in enumerate(base.pages):
        if multi:
            page.merge_page(over.pages[i])
        elif over0:
            page.merge_page(over0)
        writer.add_page(page)

    with open(output_pdf, "wb") as f:
        writer.write(f)


def _wrap_whole_pdf_with_logo_and_metadata(*,
    sb: Client,
    user_id: str,
    original_pdf_path: str,
    out_path: str,
    funder_name: str,
    recipient_email: str,
    deal_id: str,
    fingerprint_id: str
) -> None:
    """
    Compose overlay (logo + footer) on EVERY page of the original,
    then apply invisible fingerprint and embed the ORIGINAL bytes.
    """
    # 1) Build overlay matching page sizes
    sizes = _probe_page_sizes(original_pdf_path)
    _, logo_path = _resolve_user_branding(sb, user_id)
    footer_text = f"For: {funder_name} • {recipient_email} • Track: {fingerprint_id}"

    with tempfile.TemporaryDirectory() as td:
        overlay_path = os.path.join(td, "overlay.pdf")
        _build_multi_page_overlay(overlay_path, sizes, logo_path, footer_text)

        # 2) Compose with qpdf; fallback to PyPDF2 if needed
        composed_path = os.path.join(td, "composed.pdf")
        try:
            _overlay_with_qpdf(original_pdf_path, overlay_path, composed_path)
        except Exception:
            _overlay_fallback_python(original_pdf_path, overlay_path, composed_path)

        # 3) Fingerprint & embed original into the composed wrapper
        pdf = Pdf.open(composed_path)
        _apply_invisible_fingerprint(
            pdf,
            fingerprint_id=fingerprint_id,
            recipient_email=recipient_email,
            deal_id=str(deal_id),
            user_id=str(user_id),
        )
        with open(original_pdf_path, "rb") as f:
            _embed_file(pdf, f.read(), os.path.basename(original_pdf_path), desc="Original (untouched)")
        pdf.save(out_path)


# ----------------------------------------------------------
# Main entry used by your server code
# ----------------------------------------------------------
def issue_wrapper_user_branded(*,
    user_id: str,            # auth.users(id)
    deal_id: int,            # deals.id
    original_pdf_path: str,
    funder_name: str,
    recipient_email: str,
    storage_dir: str,
    supabase_url: str,
    supabase_service_key: str
) -> dict:
    """
    Create a user-branded wrapper tied to a deal:
    - overlays logo + footer on EVERY page (non-raster, non-corrupting)
    - embeds ORIGINAL PDF unchanged inside the result
    - invisible fingerprint ties to user+deal+recipient
    - inserts a row in doc_tracking (trigger fills business_name/message_id)
    """
    os.makedirs(storage_dir, exist_ok=True)
    sb: Client = create_client(supabase_url, supabase_service_key)

    # Business name for filename/UI
    d = sb.table("deals").select("application_json").eq("id", deal_id).limit(1).execute().data
    if not d:
        raise ValueError(f"deal {deal_id} not found")
    business_name = (d[0]["application_json"] or {}).get("business_name") or "Business"

    user_name, _logo_path = _resolve_user_branding(sb, user_id)
    user_tag = f"UID-{user_id[:8]}"

    tracking_id = str(uuid.uuid4())
    sha256_hex = _sha256_file(original_pdf_path)
    fingerprint_id = hashlib.sha256(f"{tracking_id}:{recipient_email}:{deal_id}:{user_id}".encode()).hexdigest()[:16]

    # Output naming
    safe_biz = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in business_name)
    out_name = f"{deal_id}_{safe_biz}_{fingerprint_id}.wrapper.pdf"
    out_path = os.path.join(storage_dir, out_name)

    # Compose overlay on every page, then fingerprint + embed original
    _wrap_whole_pdf_with_logo_and_metadata(
        sb=sb,
        user_id=user_id,
        original_pdf_path=original_pdf_path,
        out_path=out_path,
        funder_name=funder_name,
        recipient_email=recipient_email,
        deal_id=str(deal_id),
        fingerprint_id=fingerprint_id,
    )

    # Optional post-optimize using qpdf (compress + linearize) if available
    try:
        linearized = os.path.join(storage_dir, f"{deal_id}_{safe_biz}_{fingerprint_id}.linear.pdf")
        cmd = ["qpdf", "--object-streams=generate", "--compress-streams=y", "--linearize", out_path, linearized]
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if proc.returncode == 0 and os.path.exists(linearized):
            # replace original with linearized
            os.replace(linearized, out_path)
    except Exception:
        pass

    # Insert tracking row (mirrors filled by trigger)
    row = {
        "deal_id": deal_id,
        "user_id": user_id,
        "tracking_id": tracking_id,
        "recipient": recipient_email.lower(),
        "funder_name": funder_name,
        "sha256": sha256_hex,
        "wrapper_filename": out_name,
        "fingerprint_id": fingerprint_id
    }
    resp = sb.table("doc_tracking").insert(row).execute()

    return {
        "tracking_id": tracking_id,
        "fingerprint_id": fingerprint_id,
        "sha256": sha256_hex,
        "wrapper_filename": out_name,
        "wrapper_path": out_path,
        "business_name": business_name,
        "user_name": user_name,
        "logo_path": _logo_path,
        "supabase_row": getattr(resp, "data", None)
    }
