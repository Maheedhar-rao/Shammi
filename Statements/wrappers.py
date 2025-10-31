# server/wrappers.py
# -*- coding: utf-8 -*-


import os
import uuid
import json
import base64
import hashlib
import tempfile
import subprocess
from datetime import datetime, timezone
from typing import Optional, Tuple, List, Dict, Any

from reportlab.pdfgen import canvas
from reportlab.lib.units import inch
from reportlab.lib.utils import ImageReader

from PyPDF2 import PdfReader, PdfWriter, Transformation

from pikepdf import Pdf, Name, Dictionary, Stream
from supabase import create_client, Client



def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _embed_file(pdf: Pdf, file_bytes: bytes, filename: str, desc: str = "") -> None:
    """
    Embed a file payload into the PDF (as an attachment) and add to /AF.
    """
    fs_stream = pdf.make_indirect(Stream(pdf, file_bytes))
    fs = pdf.make_indirect(Dictionary(
        Type=Name("/FileSpec"),  
        F=filename,
        UF=filename,
        EF=Dictionary(F=fs_stream),
        Desc=desc or f"Embedded {filename}",
    ))

    root = pdf.Root
    if "/Names" not in root:
        root.Names = Dictionary()
    if "/EmbeddedFiles" not in root.Names:
        root.Names.EmbeddedFiles = Dictionary(Names=[])
    root.Names.EmbeddedFiles.Names.append(filename)
    root.Names.EmbeddedFiles.Names.append(fs)

    if "/AF" not in root:
        root.AF = []
    root.AF.append(fs)


def _resolve_user_branding(sb: Optional[Client], user_id: Optional[str]) -> Tuple[str, Optional[str]]:
    """
    Returns (org_name, logo_path) with fallbacks:
      1) supabase.user_branding.logo_path
      2) /var/app/assets/logos/{user_id}.png
      3) Statements/static/logo.png (alongside your repo)
    """
    user_name = "Pathway Catalyst"
    logo_path: Optional[str] = None

    if sb and user_id:
        try:
            rec = (
                sb.table("user_branding")
                .select("org_name,logo_path")
                .eq("user_id", user_id)
                .limit(1)
                .execute()
                .data
            )
            if rec:
                user_name = rec[0].get("org_name") or user_name
                logo_path = rec[0].get("logo_path") or None
        except Exception:
            pass

    if not logo_path and user_id:
        candidate = f"/var/app/assets/logos/{user_id}.png"
        if os.path.exists(candidate):
            logo_path = candidate

    if not logo_path:
        fallback = os.path.abspath(os.path.join("Statements", "static", "logo.png"))
        if os.path.exists(fallback):
            logo_path = fallback

    return user_name, logo_path


def _resolve_lender_branding(
    sb: Client,
    *,
    lender_id: Optional[str] = None,
    funder_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Returns lender branding overrides if present. Expected schema (customize to your DB):
      lenders:          id (uuid), name (text)
      lender_branding:  lender_id (uuid FK), logo_path (text), watermark_text (text),
                        footer_template (text), logo_max_in (numeric), logo_max_pct (numeric)
    """
    try:
        if lender_id:
            lb = (
                sb.table("lender_branding")
                .select("logo_path,watermark_text,footer_template,logo_max_in,logo_max_pct,lender_id")
                .eq("lender_id", lender_id)
                .limit(1)
                .execute()
                .data
            )
            return lb[0] if lb else {}
        if funder_name:
            lenders = sb.table("lenders").select("id").eq("name", funder_name).limit(1).execute().data
            if lenders:
                lid = lenders[0]["id"]
                lb = (
                    sb.table("lender_branding")
                    .select("logo_path,watermark_text,footer_template,logo_max_in,logo_max_pct,lender_id")
                    .eq("lender_id", lid)
                    .limit(1)
                    .execute()
                    .data
                )
                return lb[0] if lb else {}
    except Exception:
        pass
    return {}


def _probe_page_sizes(input_pdf: str) -> List[Tuple[float, float]]:
    """Read page sizes (width, height) in points from the input PDF."""
    r = PdfReader(input_pdf)
    sizes: List[Tuple[float, float]] = []
    for p in r.pages:
        mb = p.mediabox
        w = float(mb.right) - float(mb.left)
        h = float(mb.top) - float(mb.bottom)
        sizes.append((w, h))
    return sizes


# ---------- wrappers.py: fixed overlay builder ----------
def _build_multi_page_overlay(
    out_path: str,
    page_sizes: list,                 # [(width_pts, height_pts), ...] from _probe_page_sizes
    logo_path: str = None,
    footer_text: str = None,          # e.g. "Submitted via Pathway Catalyst"
    LOGO_MAX_IN: float = 0.9,
    LOGO_MAX_PCT: float = 0.18,
    WATERMARK_TEXT: str = None        # e.g. "WRAPPED"
):
    """
    Builds a VALID overlay PDF with SAME page count as src.
    No unreplaced tokens. Uses ReportLab to draw.
    """
    from reportlab.pdfgen import canvas
    from reportlab.lib.units import inch
    from reportlab.lib.pagesizes import portrait, landscape
    from reportlab.lib.utils import ImageReader
    import os, io

    total_pages = len(page_sizes)
    if total_pages == 0:
        raise ValueError("page_sizes is empty")

    # Prepare output
    c = canvas.Canvas(out_path)

    # Preload logo if provided and exists
    logo_img = None
    if logo_path and os.path.exists(logo_path):
        try:
            logo_img = ImageReader(logo_path)
        except Exception:
            logo_img = None  # draw nothing if unreadable

    for i, (w, h) in enumerate(page_sizes):
        # Set page size exactly to source page
        if w >= h:
            c.setPageSize(landscape((w, h)))
        else:
            c.setPageSize(portrait((w, h)))

        # Optional diagonal watermark behind content when used as underlay
        if WATERMARK_TEXT:
            c.saveState()
            c.translate(w * 0.5, h * 0.5)
            c.rotate(45)
            c.setFillGray(0.85)                 # light gray
            c.setFont("Helvetica-Bold", max(24, min(w, h) * 0.06))
            text_wm = WATERMARK_TEXT
            c.drawCentredString(0, 0, text_wm)
            c.restoreState()

        # Logo in the top-left margin
        if logo_img:
            c.saveState()
            # Compute max logo box
            max_w_pts = min(w * LOGO_MAX_PCT, LOGO_MAX_IN * inch)
            # Keep aspect ratio using image native size
            try:
                iw, ih = logo_img.getSize()
                ar = iw / float(ih or 1)
                draw_w = max_w_pts
                draw_h = draw_w / ar
                # position: small margin from top-left
                margin = max(12, 0.015 * min(w, h))
                x = margin
                y = h - margin - draw_h
                c.drawImage(logo_img, x, y, width=draw_w, height=draw_h, mask='auto')
            except Exception:
                pass
            c.restoreState()

        # Footer line (no placeholders)
        if footer_text:
            c.saveState()
            c.setFont("Helvetica", max(8, min(w, h) * 0.018))
            c.setFillGray(0.2)
            margin = max(14, 0.02 * min(w, h))
            footer = f"{footer_text}  •  Page {i+1} of {total_pages}"
            c.drawString(margin, margin, footer)
            c.restoreState()

        c.showPage()

    c.save()


# ---------- wrappers.py: pure-Python fallback, rotation-safe ----------
def _overlay_python_rotation_safe(src_path: str, overlay_path: str, out_path: str):
    """
    Pure-Python merge using pypdf or PyPDF2 (whichever is installed).
    Keeps page sizes; overlay count must match source count.
    """
    # Try pypdf first, then PyPDF2
    try:
        from pypdf import PdfReader, PdfWriter
        HAVE_PYPDF2 = False
    except Exception:
        from PyPDF2 import PdfReader, PdfWriter
        HAVE_PYPDF2 = True  # API is compatible for what we use here

    src = PdfReader(src_path)
    ovl = PdfReader(overlay_path)

    if len(src.pages) != len(ovl.pages):
        raise ValueError("overlay page count must match source page count")

    writer = PdfWriter()
    for i in range(len(src.pages)):
        base_page = src.pages[i]
        overlay_page = ovl.pages[i]

        # Ensure mediabox matches
        overlay_page.mediabox = base_page.mediabox

        # Merge: draw base, then overlay on top
        # (Both pypdf and PyPDF2 support merge_page)
        base_page.merge_page(overlay_page)
        writer.add_page(base_page)

    with open(out_path, "wb") as f:
        writer.write(f)


def _apply_invisible_fingerprint(pdf: Pdf, *, fingerprint_id: str,
                                 recipient_email: str, deal_id: str, user_id: str) -> None:
    """
    Add custom docinfo keys (non-visible) for traceability.
    """
    info = pdf.docinfo or Dictionary()
    info[Name("/PCP_Fingerprint")] = fingerprint_id
    info[Name("/PCP_Recipient")] = recipient_email
    info[Name("/PCP_Deal")] = str(deal_id)
    info[Name("/PCP_UserId")] = str(user_id)
    info[Name("/PCP_IssuedAt")] = datetime.now(timezone.utc).isoformat()
    pdf.docinfo = info

    # Optional inert payload object (base64'd JSON) to deter trivial stripping
    payload = {
        "fp": fingerprint_id,
        "r": recipient_email,
        "d": str(deal_id),
        "u": str(user_id),
        "t": datetime.now(timezone.utc).isoformat(),
    }
    raw = base64.b64encode(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    _ = pdf.make_indirect(Stream(pdf, raw))


def _wrap_whole_pdf_with_logo_and_metadata(
    *,
    sb: Client,
    user_id: str,
    original_pdf_path: str,
    out_path: str,
    funder_name: str,
    recipient_email: str,
    deal_id: str,
    fingerprint_id: str,
    user_logo_path: Optional[str],
    lender_overrides: Dict[str, Any],
) -> None:
    """
    Compose overlay (logo + watermark + footer) on EVERY page of the original,
    then apply invisible fingerprint and embed the ORIGINAL bytes.
    """
    sizes = _probe_page_sizes(original_pdf_path)

    # Effective logo & sizing (lender overrides win if present)
    effective_logo = lender_overrides.get("logo_path") or user_logo_path
    logo_max_in = float(lender_overrides.get("logo_max_in") or 0.7)
    logo_max_pct = float(lender_overrides.get("logo_max_pct") or 0.10)
    watermark_text = lender_overrides.get("watermark_text")

    # Footer template with tokens
    footer_tpl = lender_overrides.get("footer_template") or "For: {funder} • {recipient} • Track: {tracking} • FP: {fp}"
    footer_text = footer_tpl.format(
        funder=funder_name,
        recipient=recipient_email,
        deal=deal_id,
        fp=fingerprint_id,
        tracking=tracking_id, 
    )

    wm_tpl = force_watermark_text or (lender_overrides or {}).get("watermark_text")
    watermark_text = wm_tpl.format(
        funder=funder_name,
        recipient=recipient_email,
        deal=deal_id,
        fp=fingerprint_id,
        tracking=tracking_id,
        ) if wm_tpl else None

    with tempfile.TemporaryDirectory() as td:
        overlay_path = os.path.join(td, "overlay.pdf")
        _build_multi_page_overlay(
            overlay_path,
            sizes,
            effective_logo,
            footer_text,
            LOGO_MAX_IN=logo_max_in,
            LOGO_MAX_PCT=logo_max_pct,
            WATERMARK_TEXT=watermark_text,
        )

        composed_path = os.path.join(td, "composed.pdf")

        # Choose overlay engine:
        # - If DISABLE_QPDF=1 -> always use rotation-safe PyPDF2
        # - Else: try qpdf first, fall back to rotation-safe PyPDF2
        disable_qpdf = os.environ.get("DISABLE_QPDF") in ("1", "true", "TRUE", "yes", "YES")
        if disable_qpdf:
            _overlay_python_rotation_safe(original_pdf_path, overlay_path, composed_path)
        else:
            try:
                _overlay_with_qpdf(original_pdf_path, overlay_path, composed_path)
            except Exception:
                _overlay_python_rotation_safe(original_pdf_path, overlay_path, composed_path)

        # Fingerprint & embed original into the composed wrapper
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

# ---------- wrappers.py: qpdf overlay/underlay with linearize ----------
def _overlay_with_qpdf(src: str, overlay: str, dst: str, *, underlay: bool = False, linearize: bool = True):
    """
    Uses qpdf to merge overlay.
    underlay=True puts overlay behind content (good for big watermarks).
    Also linearizes to satisfy Gmail/Browser previewers.
    """
    import subprocess, os, tempfile, shutil

    if not shutil.which("qpdf"):
        raise RuntimeError("qpdf not found on PATH")

    mode = "--underlay" if underlay else "--overlay"
    tmp_out = dst + ".tmp.pdf"

    # 1) Merge
    subprocess.check_call(["qpdf", src, mode, overlay, "--", tmp_out])

    # 2) Linearize for better viewer compatibility
    if linearize:
        tmp_lin = dst + ".lin.pdf"
        subprocess.check_call(["qpdf", "--linearize", tmp_out, tmp_lin])
        os.replace(tmp_lin, dst)
        os.remove(tmp_out)
    else:
        os.replace(tmp_out, dst)





def issue_wrapper_user_branded(
    *,
    user_id: str,                # users.id (FK)
    deal_id: int,                # deals.id
    original_pdf_path: str,
    funder_name: str,            # human-readable lender name
    recipient_email: str,        # who receives/views this
    storage_dir: str,
    supabase_url: str,
    supabase_service_key: str,
    lender_id: Optional[str] = None,  # optional explicit lender id

    # -------- Force-overrides you can pass from email path (optional) --------
    force_watermark_text: Optional[str] = None,
    force_footer_template: Optional[str] = None,
    force_logo_path: Optional[str] = None,
    force_logo_max_in: Optional[float] = None,
    force_logo_max_pct: Optional[float] = None,

    force_tracking_id: Optional[str] = None,
) -> dict:
    """
    Create a user-branded wrapper tied to a deal:
      - overlay logo + optional watermark + footer on EVERY page (non-raster)
      - embed ORIGINAL PDF unchanged inside the result
      - insert fingerprint metadata into docinfo
      - insert a row in doc_tracking (DB triggers can fill extra fields)
    Returns info for downstream usage (paths, IDs, etc.).
    """
    os.makedirs(storage_dir, exist_ok=True)
    sb: Client = create_client(supabase_url, supabase_service_key)

    # Business name for filename/UI
    d = sb.table("deals").select("application_json").eq("id", deal_id).limit(1).execute().data
    if not d:
        raise ValueError(f"deal {deal_id} not found")
    business_name = (d[0]["application_json"] or {}).get("business_name") or "Business"

    # Branding from user + lender
    user_name, user_logo_path = _resolve_user_branding(sb, user_id)
    lender_overrides = _resolve_lender_branding(sb, lender_id=lender_id, funder_name=funder_name)

    # ---- Force overrides (take precedence over DB) ----
    if force_logo_path:
        lender_overrides["logo_path"] = force_logo_path
    if force_logo_max_in is not None:
        lender_overrides["logo_max_in"] = force_logo_max_in
    if force_logo_max_pct is not None:
        lender_overrides["logo_max_pct"] = force_logo_max_pct
    if force_watermark_text is not None:
        lender_overrides["watermark_text"] = force_watermark_text
    if force_footer_template is not None:
        lender_overrides["footer_template"] = force_footer_template

    # Identifiers
    tracking_id = force_tracking_id or str(uuid.uuid4())
    sha256_hex = _sha256_file(original_pdf_path)
    fingerprint_id = hashlib.sha256(
        f"{tracking_id}:{recipient_email}:{deal_id}:{user_id}".encode()
    ).hexdigest()[:16]

    # Output path
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
        user_logo_path=user_logo_path,
        lender_overrides=lender_overrides,
    )

    # Optional: optimize output with qpdf (object streams + linearize) if available & not disabled
    try:
        disable_qpdf = os.environ.get("DISABLE_QPDF") in ("1", "true", "TRUE", "yes", "YES")
        if not disable_qpdf:
            linearized = os.path.join(storage_dir, f"{deal_id}_{safe_biz}_{fingerprint_id}.linear.pdf")
            cmd = ["qpdf", "--object-streams=generate", "--compress-streams=y", "--linearize", out_path, linearized]
            proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if proc.returncode == 0 and os.path.exists(linearized):
                os.replace(linearized, out_path)
    except Exception:
        pass

    # Insert tracking row (respect FKs/RLS in your DB)
    row = {
        "deal_id": deal_id,
        "user_id": user_id,
        "tracking_id": tracking_id,
        "recipient": recipient_email.lower(),
        "funder_name": funder_name,
        "sha256": sha256_hex,
        "wrapper_filename": out_name,
        "fingerprint_id": fingerprint_id,
        # "lender_id": lender_id,  # enable if needed
    }
    resp = sb.table("doc_tracking").insert(row).execute()

    # Diagnostics to quickly see what actually got drawn
    effective_logo_path = lender_overrides.get("logo_path") or user_logo_path
    try:
        qpdf_present = (subprocess.run(["which", "qpdf"], stdout=subprocess.PIPE, stderr=subprocess.PIPE).returncode == 0)
    except Exception:
        qpdf_present = False

    diagnostics = {
        "overlay": True,
        "watermark_used": bool(lender_overrides.get("watermark_text")),
        "footer_used": bool((lender_overrides.get("footer_template") or "").strip()),
        "logo_used_path": effective_logo_path if (effective_logo_path and os.path.exists(effective_logo_path)) else None,
        "qpdf_present": qpdf_present and not (os.environ.get("DISABLE_QPDF") in ("1","true","TRUE","yes","YES")),
    }

    return {
        "tracking_id": tracking_id,
        "fingerprint_id": fingerprint_id,
        "sha256": sha256_hex,
        "wrapper_filename": out_name,
        "wrapper_path": out_path,
        "business_name": business_name,
        "user_name": user_name,
        "logo_path": effective_logo_path,
        "lender_overrides": lender_overrides or None,
        "supabase_row": getattr(resp, "data", None),
        "diagnostics": diagnostics,
    }

# ---------- wrappers.py: wrapper shim tying it together ----------
def wrap_pdf_with_logo(input_path: str, logo_path: str, output_dir: str = None,
                       footer_text: str = None, watermark_text: str = None) -> str:
    import os, tempfile
    from pathlib import Path

    output_dir = output_dir or os.path.dirname(input_path)
    base = Path(input_path).stem
    out_path = os.path.join(output_dir, f"{base}.wrapped.pdf")

    sizes = _probe_page_sizes(input_path)
    if not sizes:
        raise ValueError("cannot probe page sizes")

    with tempfile.TemporaryDirectory() as td:
        overlay_pdf = os.path.join(td, "overlay.pdf")
        _build_multi_page_overlay(
            out_path=overlay_pdf,
            page_sizes=sizes,
            logo_path=logo_path,
            footer_text=footer_text,
            LOGO_MAX_IN=0.9,
            LOGO_MAX_PCT=0.18,
            WATERMARK_TEXT=watermark_text
        )

        # >>> Always use pure-Python merge (no qpdf needed)
        _overlay_python_rotation_safe(input_path, overlay_pdf, out_path)

    return out_path

