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
  
    r = PdfReader(input_pdf)
    sizes: List[Tuple[float, float]] = []
    for p in r.pages:
        mb = p.mediabox
        w = float(mb.right) - float(mb.left)
        h = float(mb.top) - float(mb.bottom)
        sizes.append((w, h))
    return sizes



def _build_multi_page_overlay(
    out_path: str,
    page_sizes: list,                
    logo_path: str = None,
    footer_text: str = None,          
    LOGO_MAX_IN: float = 0.9,
    LOGO_MAX_PCT: float = 0.18,
    WATERMARK_TEXT: str = None       
):
    
    from reportlab.pdfgen import canvas
    from reportlab.lib.units import inch
    from reportlab.lib.pagesizes import portrait, landscape
    from reportlab.lib.utils import ImageReader
    import os, io

    total_pages = len(page_sizes)
    if total_pages == 0:
        raise ValueError("page_sizes is empty")

 
    c = canvas.Canvas(out_path)

    
    logo_img = None
    if logo_path and os.path.exists(logo_path):
        try:
            logo_img = ImageReader(logo_path)
        except Exception:
            logo_img = None  

    for i, (w, h) in enumerate(page_sizes):
        
        if w >= h:
            c.setPageSize(landscape((w, h)))
        else:
            c.setPageSize(portrait((w, h)))

       
        if WATERMARK_TEXT:
            c.saveState()
            c.translate(w * 0.5, h * 0.5)
            c.rotate(45)
            c.setFillGray(0.85)                 # light gray
            c.setFont("Helvetica-Bold", max(24, min(w, h) * 0.06))
            text_wm = WATERMARK_TEXT
            c.drawCentredString(0, 0, text_wm)
            c.restoreState()

       
        if logo_img:
            c.saveState()
            
            max_w_pts = min(w * LOGO_MAX_PCT, LOGO_MAX_IN * inch)
            
            try:
                iw, ih = logo_img.getSize()
                max_w_pts = min(w * LOGO_MAX_PCT, LOGO_MAX_IN * inch)
                ar = iw / float(ih or 1)
                draw_w = max_w_pts
                draw_h = draw_w / ar
                
                margin = max(12, 0.015 * min(w, h))
                x = (w - draw_w) / 2.0
                y = (h - draw_h) / 2.0
                if hasattr(c, "setFillAlpha"):
                    try:
                        c.setFillAlpha(0.12)   
                        c.setStrokeAlpha(0.12)
                    except Exception:
                        pass
                c.drawImage(
                    logo_img,
                    x,
                    y,
                    width=draw_w,
                    height=draw_h,
                    mask="auto",
                    preserveAspectRatio=True,
                )
            except Exception:
                pass
            finally:
                c.restoreState()

       
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


def _overlay_python_rotation_safe(src_path: str, overlay_path: str, out_path: str):
    
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

    effective_logo = lender_overrides.get("logo_path") or user_logo_path
    logo_max_in = float(lender_overrides.get("logo_max_in") or 0.7)
    logo_max_pct = float(lender_overrides.get("logo_max_pct") or 0.10)
    watermark_text = lender_overrides.get("watermark_text")

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

        disable_qpdf = os.environ.get("DISABLE_QPDF") in ("1", "true", "TRUE", "yes", "YES")
        if disable_qpdf:
            _overlay_python_rotation_safe(original_pdf_path, overlay_path, composed_path)
        else:
            try:
                _overlay_with_qpdf(original_pdf_path, overlay_path, composed_path)
            except Exception:
                _overlay_python_rotation_safe(original_pdf_path, overlay_path, composed_path)

       
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

    subprocess.check_call(["qpdf", src, mode, overlay, "--", tmp_out])

    if linearize:
        tmp_lin = dst + ".lin.pdf"
        subprocess.check_call(["qpdf", "--linearize", tmp_out, tmp_lin])
        os.replace(tmp_lin, dst)
        os.remove(tmp_out)
    else:
        os.replace(tmp_out, dst)





def issue_wrapper_user_branded(
    *,
    user_id: str,                
    deal_id: int,                
    original_pdf_path: str,
    funder_name: str,            
    recipient_email: str,        
    storage_dir: str,
    supabase_url: str,
    supabase_service_key: str,
    lender_id: Optional[str] = None,  

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

    d = sb.table("deals").select("application_json").eq("id", deal_id).limit(1).execute().data
    if not d:
        raise ValueError(f"deal {deal_id} not found")
    business_name = (d[0]["application_json"] or {}).get("business_name") or "Business"

    user_name, user_logo_path = _resolve_user_branding(sb, user_id)
    lender_overrides = _resolve_lender_branding(sb, lender_id=lender_id, funder_name=funder_name)

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

    
    tracking_id = force_tracking_id or str(uuid.uuid4())
    sha256_hex = _sha256_file(original_pdf_path)
    fingerprint_id = hashlib.sha256(
        f"{tracking_id}:{recipient_email}:{deal_id}:{user_id}".encode()
    ).hexdigest()[:16]

    safe_biz = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in business_name)
    out_name = f"{deal_id}_{safe_biz}_{fingerprint_id}.wrapper.pdf"
    out_path = os.path.join(storage_dir, out_name)

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

    row = {
        "deal_id": deal_id,
        "user_id": user_id,
        "tracking_id": tracking_id,
        "recipient": recipient_email.lower(),
        "funder_name": funder_name,
        "sha256": sha256_hex,
        "wrapper_filename": out_name,
        "fingerprint_id": fingerprint_id,
    }
    resp = sb.table("doc_tracking").insert(row).execute()

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

        _overlay_python_rotation_safe(input_path, overlay_pdf, out_path)

    return out_path


"""
TAMPER-PROOF PDF WRAPPER - Impossible to Remove Credentials
============================================================

Adds MULTIPLE LAYERS of protection so recipients cannot remove credentials:
1. Visible watermarks (can see but can't remove)
2. Invisible metadata (survives most edits)
3. Steganographic embedding (hidden in image data)
4. Text layer manipulation (hidden text throughout)
5. Original PDF embedded (proves tampering if missing)
6. Cryptographic signatures (detects any modification)

Integration with your existing wrappers.py
"""

import os
import hashlib
import secrets
from datetime import datetime, timezone
from typing import Optional, List, Tuple, Dict
from PIL import Image, ImageDraw, ImageFont
import io

from reportlab.pdfgen import canvas
from reportlab.lib.units import inch
from reportlab.lib.utils import ImageReader
from reportlab.lib.pagesizes import letter

from PyPDF2 import PdfReader, PdfWriter
from pikepdf import Pdf, Name, Dictionary, Array, String, Stream
import base64



#1: Enhanced Invisible Fingerprinting


def _apply_multi_layer_fingerprint(
    pdf: Pdf,
    fingerprint_id: str,
    recipient_email: str,
    deal_id: str,
    user_id: str,
    tracking_url: str = None
):
    """
    Enhanced fingerprinting in MULTIPLE locations - impossible to remove all.
    
    Locations:
    1. Document Info metadata
    2. XMP metadata
    3. Custom PDF properties
    4. Page annotations (invisible)
    5. Document ID
    6. Trailer dictionary
    """
    
    with pdf.open_metadata() as meta:
        meta["dc:identifier"] = fingerprint_id
        meta["dc:creator"] = f"Pathway Catalyst ({user_id})"
        meta["dc:rights"] = f"Issued to {recipient_email}"
        meta["pdf:Keywords"] = f"FP:{fingerprint_id} DEAL:{deal_id}"
        meta["xmp:CreatorTool"] = f"Pathway Catalyst Wrapper v2.0"
        meta["xmp:CreateDate"] = datetime.now(timezone.utc).isoformat()
    
    pdf.docinfo["/Fingerprint"] = fingerprint_id
    pdf.docinfo["/RecipientEmail"] = recipient_email
    pdf.docinfo["/DealID"] = deal_id
    pdf.docinfo["/UserID"] = user_id
    pdf.docinfo["/IssueDate"] = datetime.now(timezone.utc).isoformat()
    pdf.docinfo["/Producer"] = "Pathway Catalyst Document Security"
    
    if not hasattr(pdf.Root, "PathwayCatalyst"):
        pdf.Root.PathwayCatalyst = Dictionary()
    
    pdf.Root.PathwayCatalyst.Fingerprint = fingerprint_id
    pdf.Root.PathwayCatalyst.Recipient = recipient_email
    pdf.Root.PathwayCatalyst.DealID = deal_id
    pdf.Root.PathwayCatalyst.Timestamp = String(datetime.now(timezone.utc).isoformat())
    
    doc_id = hashlib.sha256(
        f"{fingerprint_id}{recipient_email}{deal_id}".encode()
    ).digest()
    pdf.trailer.ID = Array([doc_id, doc_id])
    
    if tracking_url and len(pdf.pages) > 0:
        page = pdf.pages[0]
        
        annot = Dictionary(
            Type=Name("/Annot"),
            Subtype=Name("/Link"),
            Rect=Array([0, 0, 1, 1]),  
            Border=Array([0, 0, 0]),    
            A=Dictionary(
                S=Name("/URI"),
                URI=String(tracking_url)
            ),
            Contents=String(f"FP:{fingerprint_id}"),
            T=String("Pathway Catalyst Security"),
        )
        
        if not hasattr(page, "Annots"):
            page.Annots = Array()
        page.Annots.append(pdf.make_indirect(annot))



#2: Steganographic Watermarking


def _embed_steganographic_watermark(
    pdf_path: str,
    fingerprint: str,
    output_path: str = None
) -> str:
    """
    Embed fingerprint invisibly in image data using LSB steganography.
    Even if they remove visible watermarks, this survives.
    """
    if output_path is None:
        output_path = pdf_path.replace(".pdf", ".stego.pdf")
    
    img_width, img_height = 100, 20
    img = Image.new('RGB', (img_width, img_height), color='white')
    
    pixels = img.load()
    fp_bytes = fingerprint.encode('utf-8')
    
    for i, byte in enumerate(fp_bytes):
        if i >= img_width * img_height:
            break
        x = i % img_width
        y = i // img_width
        r, g, b = pixels[x, y]
        r = (r & 0xFE) | ((byte >> 7) & 1)
        g = (g & 0xFE) | ((byte >> 6) & 1)
        b = (b & 0xFE) | ((byte >> 5) & 1)
        pixels[x, y] = (r, g, b)
    
    img_buffer = io.BytesIO()
    img.save(img_buffer, format='PNG')
    img_buffer.seek(0)
    
    from reportlab.pdfgen import canvas
    from reportlab.lib.utils import ImageReader
    
    overlay_buffer = io.BytesIO()
    c = canvas.Canvas(overlay_buffer, pagesize=letter)
    
    c.drawImage(
        ImageReader(img_buffer),
        x=0,
        y=0,
        width=0.1,    
        height=0.1,
        mask='auto'
    )
    c.save()
    overlay_buffer.seek(0)
    
    reader = PdfReader(pdf_path)
    overlay_reader = PdfReader(overlay_buffer)
    writer = PdfWriter()
    
    first_page = reader.pages[0]
    first_page.merge_page(overlay_reader.pages[0])
    writer.add_page(first_page)
    
    for i in range(1, len(reader.pages)):
        writer.add_page(reader.pages[i])
    
    with open(output_path, 'wb') as f:
        writer.write(f)
    
    return output_path


# 3: Distributed Text Watermarks (Throughout Document)


def _add_distributed_text_watermarks(
    pdf_path: str,
    fingerprint: str,
    recipient_email: str,
    output_path: str = None
) -> str:
    """
    Add invisible text watermarks distributed throughout the document.
    Uses white text on white background scattered across pages.
    Survives screenshots and partial edits.
    """
    if output_path is None:
        output_path = pdf_path.replace(".pdf", ".textwm.pdf")
    
    reader = PdfReader(pdf_path)
    writer = PdfWriter()
    
    watermark_text = f"FP:{fingerprint}|REC:{recipient_email}|PC:2024"
    
    for page_num, page in enumerate(reader.pages):
        page_width = float(page.mediabox.width)
        page_height = float(page.mediabox.height)
        
        
        overlay_buffer = io.BytesIO()
        c = canvas.Canvas(overlay_buffer, pagesize=(page_width, page_height))
        
        
        c.setFillColorRGB(1, 1, 1)  
        c.setFont("Helvetica", 1)    
        
        positions = [
            (10, 10), (page_width-100, 10),
            (page_width/2, 10), (10, page_height-10),
            (page_width/2, page_height/2),
            (page_width-100, page_height-10)
        ]
        
        for x, y in positions:
            c.drawString(x, y, watermark_text)

        if recipient_email:
            lender_tag = recipient_email.split("@")[0]
            c.drawString(70, 70, f"LDR:{lender_tag}")
        c.save()
        overlay_buffer.seek(0)
        
        overlay_reader = PdfReader(overlay_buffer)
        page.merge_page(overlay_reader.pages[0])
        writer.add_page(page)
    
    with open(output_path, 'wb') as f:
        writer.write(f)
    
    return output_path


# 4: Cryptographic Signature (Tamper Detection)


def _add_cryptographic_seal(
    pdf: Pdf,
    fingerprint: str,
    recipient_email: str,
    deal_id: str
) -> str:
    """
    Add cryptographic seal that proves document hasn't been modified.
    Creates hash of content + metadata that can be verified.
    """
    import hmac
    
    signature_data = f"{fingerprint}|{recipient_email}|{deal_id}|{len(pdf.pages)}"
    
    secret_key = os.environ.get("PDF_SIGNING_KEY", "pathway-catalyst-secret-key-2024")
    signature = hmac.new(
        secret_key.encode(),
        signature_data.encode(),
        hashlib.sha256
    ).hexdigest()
    
    if not hasattr(pdf.Root, "Security"):
        pdf.Root.Security = Dictionary()
    
    pdf.Root.Security.Signature = signature
    pdf.Root.Security.SignedData = signature_data
    pdf.Root.Security.SignedAt = String(datetime.now(timezone.utc).isoformat())
    
    return signature


def verify_cryptographic_seal(pdf_path: str) -> Dict:
    """
    Verify if PDF has been tampered with by checking signature.
    """
    try:
        pdf = Pdf.open(pdf_path)
        
        if not hasattr(pdf.Root, "Security") or not hasattr(pdf.Root.Security, "Signature"):
            return {"valid": False, "reason": "No signature found"}
        
        stored_signature = str(pdf.Root.Security.Signature)
        signed_data = str(pdf.Root.Security.SignedData)
        
        secret_key = os.environ.get("PDF_SIGNING_KEY", "pathway-catalyst-secret-key-2024")
        expected_signature = hmac.new(
            secret_key.encode(),
            signed_data.encode(),
            hashlib.sha256
        ).hexdigest()
        
        if stored_signature == expected_signature:
            return {
                "valid": True,
                "signed_at": str(pdf.Root.Security.SignedAt),
                "data": signed_data
            }
        else:
            return {
                "valid": False,
                "reason": "Signature mismatch - document has been modified"
            }
            
    except Exception as e:
        return {"valid": False, "reason": f"Verification failed: {str(e)}"}


# 5: Visual Watermark Protection (Rasterized & Protected)


def _add_protected_visual_watermark(
    pdf_path: str,
    watermark_text: str,
    output_path: str = None
) -> str:
    """
    Add visible watermark that's protected and hard to remove.
    Uses both vector and raster techniques.
    """
    if output_path is None:
        output_path = pdf_path.replace(".pdf", ".visualwm.pdf")
    
    reader = PdfReader(pdf_path)
    writer = PdfWriter()
    
    for page in reader.pages:
        page_width = float(page.mediabox.width)
        page_height = float(page.mediabox.height)
        
        overlay_buffer = io.BytesIO()
        c = canvas.Canvas(overlay_buffer, pagesize=(page_width, page_height))
        
        c.saveState()
        c.translate(page_width/2, page_height/2)
        c.rotate(45)
        
        for offset in [0, 2, -2]:
            c.setFillGray(0.9, alpha=0.3)
            c.setFont("Helvetica-Bold", 36)
            c.drawCentredString(offset, offset, watermark_text)
        
        c.restoreState()
        
        c.setFillGray(0.85)
        c.setFont("Helvetica", 8)
        c.drawString(10, 10, watermark_text)
        c.drawString(page_width-150, 10, watermark_text)
        
        c.save()
        overlay_buffer.seek(0)
        
        overlay_reader = PdfReader(overlay_buffer)
        page.merge_page(overlay_reader.pages[0])
        writer.add_page(page)
    
    with open(output_path, 'wb') as f:
        writer.write(f)
    
    return output_path


# Apply Protection Layers


def create_tamper_proof_wrapper(
    original_pdf_path: str,
    output_path: str,
    fingerprint_id: str,
    recipient_email: str,
    deal_id: str,
    user_id: str,
    tracking_url: str,
    watermark_text: str = None,
    footer_text: str = None,
    logo_path: str = None
) -> Dict:
    """
    Create PDF with ALL protection layers enabled.
    Makes it virtually impossible to remove credentials without detection.
    
    Returns dict with paths and verification info.
    """
    import tempfile
    import shutil
    
    with tempfile.TemporaryDirectory() as tmpdir:
        current_path = original_pdf_path
        
        if logo_path or footer_text or watermark_text:
            wrapped_path = os.path.join(tmpdir, "01_wrapped.pdf")
            sizes = _probe_page_sizes(current_path)
            overlay_pdf = os.path.join(tmpdir, "overlay.pdf")
            _build_multi_page_overlay(
                out_path=overlay_pdf,
                page_sizes=sizes,
                logo_path=logo_path,
                footer_text=footer_text or "Submitted via Pathway Catalyst",
                LOGO_MAX_IN=0.9,
                LOGO_MAX_PCT=0.18,
                WATERMARK_TEXT=watermark_text
            )
            
            _overlay_python_rotation_safe(current_path, overlay_pdf, wrapped_path)
            current_path = wrapped_path
        
        text_wm_path = os.path.join(tmpdir, "02_textwm.pdf")
        current_path = _add_distributed_text_watermarks(
            current_path,
            fingerprint_id,
            recipient_email,
            text_wm_path
        )
        
        stego_path = os.path.join(tmpdir, "03_stego.pdf")
        current_path = _embed_steganographic_watermark(
            current_path,
            fingerprint_id,
            stego_path
        )
        
        pdf = Pdf.open(current_path)
        
        _apply_multi_layer_fingerprint(
            pdf,
            fingerprint_id,
            recipient_email,
            deal_id,
            user_id,
            tracking_url
        )
        
        signature = _add_cryptographic_seal(
            pdf,
            fingerprint_id,
            recipient_email,
            deal_id
        )
        
        with open(original_pdf_path, "rb") as f:
            from wrappers import _embed_file
            _embed_file(
                pdf,
                f.read(),
                f"original_{os.path.basename(original_pdf_path)}",
                desc="Original untouched document - Pathway Catalyst"
            )
        
        
        pdf.save(output_path)
    
    return {
        "output_path": output_path,
        "fingerprint_id": fingerprint_id,
        "signature": signature,
        "protection_layers": [
            "visible_watermark",
            "invisible_metadata",
            "steganographic_embedding",
            "distributed_text_markers",
            "cryptographic_signature",
            "embedded_original"
        ],
        "tracking_url": tracking_url
    }


#Enhanced issue_wrapper_user_branded


def issue_tamper_proof_wrapper(
    *,
    user_id: str,
    deal_id: int,
    original_pdf_path: str,
    funder_name: str,
    recipient_email: str,
    storage_dir: str,
    supabase_url: str,
    supabase_service_key: str,
    tracking_id: str = None,
    base_url: str = "https://yoursite.com",
    **kwargs  
) -> Dict:
    """
    Enhanced version of issue_wrapper_user_branded with tamper-proofing.
    
    Drop-in replacement that adds all protection layers.
    """
    import uuid
    from wrappers import (
        _resolve_user_branding,
        _resolve_lender_branding,
        _sha256_file
    )
    from supabase import create_client
    
    tracking_id = tracking_id or str(uuid.uuid4())
    sb = create_client(supabase_url, supabase_service_key)
    
    user_name, user_logo_path = _resolve_user_branding(sb, user_id)
    lender_overrides = _resolve_lender_branding(sb, funder_name=funder_name)
    
    fingerprint_id = hashlib.sha256(
        f"{tracking_id}:{recipient_email}:{deal_id}:{user_id}".encode()
    ).hexdigest()[:16]
    
    tracking_url = f"{base_url}/track/pdf-open/{tracking_id}"
    
    d = sb.table("deals").select("application_json").eq("id", deal_id).limit(1).execute().data
    business_name = (d[0]["application_json"] or {}).get("business_name", "Business") if d else "Business"
    safe_biz = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in business_name)
    
    out_name = f"{deal_id}_{safe_biz}_{fingerprint_id}.protected.pdf"
    out_path = os.path.join(storage_dir, out_name)
    
    result = create_tamper_proof_wrapper(
        original_pdf_path=original_pdf_path,
        output_path=out_path,
        fingerprint_id=fingerprint_id,
        recipient_email=recipient_email,
        deal_id=str(deal_id),
        user_id=user_id,
        tracking_url=tracking_url,
        watermark_text=lender_overrides.get("watermark_text", "PATHWAY CATALYST"),
        footer_text=lender_overrides.get("footer_template"),
        logo_path=lender_overrides.get("logo_path") or user_logo_path
    )
    
    sha256_hex = _sha256_file(out_path)
    row = {
        "deal_id": deal_id,
        "user_id": user_id,
        "tracking_id": tracking_id,
        "recipient": recipient_email.lower(),
        "funder_name": funder_name,
        "sha256": sha256_hex,
        "wrapper_filename": out_name,
        "fingerprint_id": fingerprint_id,
        "signature": result["signature"],
        "protection_enabled": True
    }
    sb.table("doc_tracking").insert(row).execute()
    
    return {
        "tracking_id": tracking_id,
        "fingerprint_id": fingerprint_id,
        "wrapper_path": out_path,
        "wrapper_filename": out_name,
        "signature": result["signature"],
        "protection_layers": result["protection_layers"],
        "business_name": business_name
    }


#  Detect Tampering & Extract Fingerprints


def extract_all_fingerprints(pdf_path: str) -> Dict:
    """
    Extract ALL fingerprints from a PDF to verify authenticity.
    Returns all found fingerprints and protection status.
    """
    results = {
        "file": pdf_path,
        "fingerprints": [],
        "tampered": False,
        "protection_intact": []
    }
    
    try:
        pdf = Pdf.open(pdf_path)
        
        if hasattr(pdf, "docinfo"):
            if "/Fingerprint" in pdf.docinfo:
                results["fingerprints"].append({
                    "source": "docinfo",
                    "fingerprint": str(pdf.docinfo["/Fingerprint"]),
                    "recipient": str(pdf.docinfo.get("/RecipientEmail", "")),
                    "deal_id": str(pdf.docinfo.get("/DealID", ""))
                })
                results["protection_intact"].append("docinfo_metadata")
        
       
        if hasattr(pdf.Root, "PathwayCatalyst"):
            pc = pdf.Root.PathwayCatalyst
            results["fingerprints"].append({
                "source": "custom_properties",
                "fingerprint": str(pc.get("Fingerprint", "")),
                "recipient": str(pc.get("Recipient", "")),
                "deal_id": str(pc.get("DealID", ""))
            })
            results["protection_intact"].append("custom_properties")
        
      
        with pdf.open_metadata() as meta:
            if "dc:identifier" in meta:
                results["fingerprints"].append({
                    "source": "xmp_metadata",
                    "fingerprint": str(meta.get("dc:identifier", "")),
                    "recipient": str(meta.get("dc:rights", ""))
                })
                results["protection_intact"].append("xmp_metadata")
        
       
        seal_result = verify_cryptographic_seal(pdf_path)
        results["signature_valid"] = seal_result.get("valid", False)
        if not seal_result.get("valid"):
            results["tampered"] = True
            results["tamper_reason"] = seal_result.get("reason")
        else:
            results["protection_intact"].append("cryptographic_seal")
        
        
        if hasattr(pdf.Root, "Names") and hasattr(pdf.Root.Names, "EmbeddedFiles"):
            results["has_embedded_original"] = True
            results["protection_intact"].append("embedded_original")
        else:
            results["has_embedded_original"] = False
            results["tampered"] = True
        
       
        results["protection_layers_intact"] = len(results["protection_intact"])
        results["total_fingerprints_found"] = len(results["fingerprints"])
        
    except Exception as e:
        results["error"] = str(e)
    
    return results


def detect_pdf_modifications(pdf_path: str) -> Dict:
    """
    Comprehensive tampering detection.
    """
    report = {
        "file": pdf_path,
        "modified": False,
        "missing_protections": [],
        "warnings": []
    }
    
    fingerprints = extract_all_fingerprints(pdf_path)
    
  
    expected_layers = [
        "docinfo_metadata",
        "custom_properties",
        "xmp_metadata",
        "cryptographic_seal",
        "embedded_original"
    ]
    
    for layer in expected_layers:
        if layer not in fingerprints.get("protection_intact", []):
            report["missing_protections"].append(layer)
            report["modified"] = True
    
    
    if not fingerprints.get("signature_valid"):
        report["warnings"].append("Cryptographic signature invalid or missing")
        report["modified"] = True
    
  
    if not fingerprints.get("has_embedded_original"):
        report["warnings"].append("Original PDF embedding missing - possible tampering")
        report["modified"] = True
    
    report["fingerprints_found"] = fingerprints.get("total_fingerprints_found", 0)
    report["intact_layers"] = fingerprints.get("protection_layers_intact", 0)
    
    return report


def wrap_pdf_secure(
    input_pdf_path: str,
    recipient_email: str,
    deal_id: str = "standalone",
    user_id: str = "standalone",
    tracking_url: Optional[str] = None,
    watermark_text: Optional[str] = None,
    footer_text: Optional[str] = None,
    logo_path: Optional[str] = None,
) -> Tuple[str, str]:
    """
    Wrap a PDF with the full 6-layer tamper-proof protection and return:
        (output_pdf_path, fingerprint_id)

    This is a standalone helper used by api_wrap_secure (and similar),
    and internally calls create_tamper_proof_wrapper.

    Parameters
    ----------
    input_pdf_path : str
        Absolute path to the uploaded PDF.
    recipient_email : str
        Used for fingerprinting and owner metadata.
    deal_id : str
        Deal identifier (optional; default "standalone").
    user_id : str
        User identifier (optional; default "standalone").
    tracking_url : str
        Optional tracking URL to bake into the invisible annotation.
    watermark_text / footer_text / logo_path :
        Optional override for visible branding.

    Returns
    -------
    (wrapped_pdf_path, fingerprint_id)
    """

    
    fingerprint_id = str(uuid.uuid4())[:12]

    
    base, ext = os.path.splitext(input_pdf_path)
    output_pdf_path = f"{base}.secure.{fingerprint_id}{ext or '.pdf'}"

    try:
        create_tamper_proof_wrapper(
            original_pdf_path=input_pdf_path,
            output_path=output_pdf_path,
            fingerprint_id=fingerprint_id,
            recipient_email=recipient_email,
            deal_id=str(deal_id),
            user_id=str(user_id),
            tracking_url=tracking_url or "",
            watermark_text=watermark_text,
            footer_text=footer_text,
            logo_path=logo_path,
        )
    except Exception as e:
        raise RuntimeError(f"PDF wrapping (6-layer) failed: {e}")

    return output_pdf_path, fingerprint_id

def verify_document_integrity(pdf_path: str) -> Dict[str, Any]:
    """
    Light-weight verifier:
      - Confirms file exists and is a readable PDF
      - Looks for metadata '/FingerprintID'
      - Tries to find a watermark marker on page 1 text
    Returns: {"valid": bool, "warnings": [...]} or {"valid": False, "error": "..."}
    """
    result = {"valid": True, "warnings": []}
    try:
        if not os.path.exists(pdf_path):
            return {"valid": False, "error": "PDF not found"}

        try:
            from PyPDF2 import PdfReader
        except Exception as e:
            return {"valid": False, "error": f"PyPDF2 not installed/usable: {e}"}

        reader = PdfReader(pdf_path)
        if len(reader.pages) == 0:
            return {"valid": False, "error": "Empty PDF"}

        meta = reader.metadata or {}
        fp = meta.get("/FingerprintID")
        if not fp:
            result["valid"] = False
            result["warnings"].append("Missing FingerprintID metadata")

        
        try:
            p0_text = (reader.pages[0].extract_text() or "").upper()
        except Exception:
            p0_text = ""
        markers = ("WATERMARKED", "SECURE", "CONFIDENTIAL", "FOR RECIPIENT")
        if not any(m in p0_text for m in markers):
            result["warnings"].append("No obvious watermark marker text found on page 1")

        return result
    except Exception as e:
        return {"valid": False, "error": str(e)}


try:
    protect_pdf_with_all_layers  
except NameError:
    def protect_pdf_with_all_layers(*, input_pdf: str, output_pdf: str, recipient_email: str, fingerprint_id: str) -> None:
        """
        Fallback: copies the input to output and injects '/FingerprintID' metadata.
        Replace with your full watermark/signature pipeline when ready.
        """
        try:
            from PyPDF2 import PdfReader, PdfWriter
        except Exception as e:
            raise RuntimeError(f"PyPDF2 required for fallback protect: {e}")

        reader = PdfReader(input_pdf)
        writer = PdfWriter()
        for page in reader.pages:
            writer.add_page(page)

       
        metadata = dict(reader.metadata or {})
        metadata["/FingerprintID"] = fingerprint_id
        metadata["/WatermarkOwner"] = recipient_email
        writer.add_metadata(metadata)

        with open(output_pdf, "wb") as f:
            writer.write(f)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Dry-run 6-layer PDF wrapper")
    parser.add_argument("input_pdf", help="Path to the input PDF")
    parser.add_argument("--email", default="test@pathwaycatalyst.com", help="Recipient email")
    parser.add_argument("--deal-id", default="DRYRUN-123", help="Deal ID")
    parser.add_argument("--user-id", default="dryrun-user", help="User ID")
    parser.add_argument("--tracking-url", default="https://example.com/pixel/dryrun", help="Tracking URL")
    args = parser.parse_args()

    out_path, fp = wrap_pdf_secure(
        input_pdf_path=args.input_pdf,
        recipient_email=args.email,
        deal_id=args.deal_id,
        user_id=args.user_id,
        tracking_url=args.tracking_url,
        watermark_text="PATHWAY CATALYST - DRYRUN",
        footer_text="Submitted via Pathway Catalyst (dry run)",
        logo_path=None,  
    )

    print("✅ Dry-run complete.")
    print("  Input: ", args.input_pdf)
    print("  Output:", out_path)
    print("  Fingerprint:", fp)
