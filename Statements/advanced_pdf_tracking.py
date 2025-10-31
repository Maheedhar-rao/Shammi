"""
advanced_pdf_tracking.py
Server-side tracking and verification for secure PDF views.
Logs every access, detects forwarding, and verifies tamper protection.
"""

import datetime
import hashlib
import json
import os
import socket
from supabase import create_client
from wrappers import verify_document_integrity


# ------------------------------------------------------------
# Supabase setup (reuse same keys as underwrite.py)
# ------------------------------------------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ------------------------------------------------------------
# 1. Log each document view
# ------------------------------------------------------------
def log_view_event(tracking_id, request):
    """
    Record a view of a secure PDF link.
    Stores IP, User-Agent, timestamp, and basic fingerprint data.
    """
    ip = request.headers.get("X-Forwarded-For", request.remote_addr)
    agent = request.headers.get("User-Agent", "unknown")

    # Very light device fingerprint (hash of agent+ip)
    fingerprint = hashlib.sha256(f"{ip}-{agent}".encode()).hexdigest()[:16]

    record = {
        "tracking_id": tracking_id,
        "ip": ip,
        "user_agent": agent,
        "fingerprint": fingerprint,
        "viewed_at": datetime.datetime.utcnow().isoformat(),
    }

    try:
        supabase.table("pdf_views").insert(record).execute()
    except Exception as e:
        print(f"[WARN] log_view_event failed: {e}")

    return record


# ------------------------------------------------------------
# 2. Get analytics summary
# ------------------------------------------------------------
def get_document_analytics(tracking_id):
    """
    Aggregate view data for a given tracking_id.
    Returns counts, unique visitors, timestamps, and origin countries if available.
    """
    try:
        result = supabase.table("pdf_views").select("*").eq("tracking_id", tracking_id).execute()
        views = result.data or []
    except Exception as e:
        print(f"[ERROR] Supabase fetch failed: {e}")
        views = []

    total_views = len(views)
    unique_fingerprints = {v["fingerprint"] for v in views} if views else set()
    first_viewed = views[0]["viewed_at"] if views else None
    last_viewed = views[-1]["viewed_at"] if views else None

    analytics = {
        "tracking_id": tracking_id,
        "total_views": total_views,
        "unique_visitors": len(unique_fingerprints),
        "first_viewed": first_viewed,
        "last_viewed": last_viewed,
        "views": views,
    }
    return analytics


# ------------------------------------------------------------
# 3. Verify PDF integrity (tamper detection)
# ------------------------------------------------------------
def verify_pdf_integrity(pdf_path):
    """
    Run tamper-proof verification on a PDF using the wrapper's verifier.
    Returns structured result with validity and warnings.
    """
    try:
        result = verify_document_integrity(pdf_path)
    except Exception as e:
        result = {"valid": False, "error": str(e)}

    return result


# ------------------------------------------------------------
# 4. Trace possible leak source
# ------------------------------------------------------------
def trace_leak_source(pdf_path, known_fingerprints):
    """
    Compare extracted fingerprint from a PDF with known recipient fingerprints.
    Returns matched email or 'unknown'.
    """
    try:
        # Simple extraction from metadata (same logic as wrapper)
        from PyPDF2 import PdfReader
        reader = PdfReader(pdf_path)
        metadata = reader.metadata or {}
        embedded_fp = metadata.get("/FingerprintID")

        if not embedded_fp:
            return {"status": "unknown", "reason": "No fingerprint found"}

        match = next((k for k, v in known_fingerprints.items() if v == embedded_fp), None)
        if match:
            return {"status": "match", "recipient": match, "fingerprint": embedded_fp}
        return {"status": "unknown", "fingerprint": embedded_fp}

    except Exception as e:
        return {"status": "error", "error": str(e)}


# ------------------------------------------------------------
# 5. Utility: export analytics report (optional)
# ------------------------------------------------------------
def export_analytics_report(tracking_id, output_path="analytics_report.json"):
    """
    Dump analytics summary for a given tracking_id into a JSON file.
    """
    analytics = get_document_analytics(tracking_id)
    with open(output_path, "w") as f:
        json.dump(analytics, f, indent=2)
    return output_path
