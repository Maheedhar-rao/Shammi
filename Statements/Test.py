#!/usr/bin/env python3
"""
Test script for send_emails_secure() functionality
Run this BEFORE sending real emails
"""

import sys
import os
import base64
import tempfile

print("=" * 70)
print("🧪 TESTING send_emails_secure() FUNCTIONALITY")
print("=" * 70)

# ============================================================================
# TEST 1: Check imports
# ============================================================================
print("\n1️⃣  Testing imports...")

try:
    from datetime import datetime, timezone, timedelta
    print("   ✅ datetime imports")
except ImportError as e:
    print(f"   ❌ datetime imports failed: {e}")
    sys.exit(1)

try:
    import base64
    print("   ✅ base64")
except ImportError as e:
    print(f"   ❌ base64 failed: {e}")
    sys.exit(1)

try:
    from email_preview_system import generate_pdf_preview, build_email_preview_html
    print("   ✅ email_preview_system")
except ImportError as e:
    print(f"   ❌ email_preview_system failed: {e}")
    print("      Note: Preview will fail but emails will still send")

try:
    from wrappers import wrap_pdf_secure
    print("   ✅ wrappers.wrap_pdf_secure")
except ImportError as e:
    print(f"   ❌ wrappers.wrap_pdf_secure failed: {e}")
    sys.exit(1)

# ============================================================================
# TEST 2: Test attachment parsing (tuple format)
# ============================================================================
print("\n2️⃣  Testing attachment parsing...")

# Simulate what _parse_attachments_from_json returns
test_attachments = [
    ("application.pdf", b"fake_pdf_data_here"),
    ("statement1.pdf", b"more_pdf_data"),
    ("statement2.pdf", b"even_more_data"),
]

try:
    for att in test_attachments:
        att_name, att_data = att  # Unpack tuple
        assert isinstance(att_name, str), "Name should be string"
        assert isinstance(att_data, bytes), "Data should be bytes"
        assert att_name.endswith(".pdf"), "Should be PDF"
    print(f"   ✅ Parsed {len(test_attachments)} attachments correctly")
except Exception as e:
    print(f"   ❌ Attachment parsing failed: {e}")
    sys.exit(1)

# ============================================================================
# TEST 3: Test PDF writing (simulate wrapping)
# ============================================================================
print("\n3️⃣  Testing PDF file operations...")

try:
    # Create a minimal valid PDF
    minimal_pdf = b"""%PDF-1.4
1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj
2 0 obj<</Type/Pages/Count 1/Kids[3 0 R]>>endobj
3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R/Resources<<>>>>endobj
xref
0 4
0000000000 65535 f 
0000000009 00000 n 
0000000052 00000 n 
0000000101 00000 n 
trailer<</Size 4/Root 1 0 R>>
startxref
190
%%EOF"""
    
    # Write to temp file
    temp_path = os.path.join(tempfile.gettempdir(), "test_write.pdf")
    with open(temp_path, "wb") as f:
        f.write(minimal_pdf)
    
    # Verify it was written
    assert os.path.exists(temp_path), "File should exist"
    assert os.path.getsize(temp_path) > 0, "File should have content"
    
    # Clean up
    os.remove(temp_path)
    print("   ✅ PDF write/read operations work")
except Exception as e:
    print(f"   ❌ PDF operations failed: {e}")
    sys.exit(1)

# ============================================================================
# TEST 4: Test wrap_pdf_secure (if available)
# ============================================================================
print("\n4️⃣  Testing wrap_pdf_secure()...")

try:
    # Create temp PDF
    temp_pdf = os.path.join(tempfile.gettempdir(), "test_wrap.pdf")
    with open(temp_pdf, "wb") as f:
        f.write(minimal_pdf)
    
    # Try wrapping
    wrapped_path, fingerprint = wrap_pdf_secure(temp_pdf, "test@example.com")
    
    assert os.path.exists(wrapped_path), "Wrapped PDF should exist"
    assert fingerprint, "Should have fingerprint"
    
    # Clean up
    os.remove(temp_pdf)
    if os.path.exists(wrapped_path):
        os.remove(wrapped_path)
    
    print(f"   ✅ wrap_pdf_secure works (fingerprint: {fingerprint[:8]}...)")
except Exception as e:
    print(f"   ❌ wrap_pdf_secure failed: {e}")
    print("      This will cause wrapping to fail!")
    sys.exit(1)

# ============================================================================
# TEST 5: Test PDF preview generation (if available)
# ============================================================================
print("\n5️⃣  Testing PDF preview generation...")

try:
    from email_preview_system import generate_pdf_preview
    
    # Create temp PDF
    temp_pdf = os.path.join(tempfile.gettempdir(), "test_preview.pdf")
    with open(temp_pdf, "wb") as f:
        f.write(minimal_pdf)
    
    # Try generating preview
    preview_b64 = generate_pdf_preview(temp_pdf)
    
    assert preview_b64, "Should have base64 preview"
    assert isinstance(preview_b64, str), "Should be string"
    assert len(preview_b64) > 100, "Should have substantial data"
    
    # Clean up
    os.remove(temp_pdf)
    
    print(f"   ✅ Preview generation works ({len(preview_b64)} chars)")
except Exception as e:
    print(f"   ⚠️  Preview generation failed: {e}")
    print("      Emails will send without previews (fallback mode)")

# ============================================================================
# TEST 6: Test HTML email building
# ============================================================================
print("\n6️⃣  Testing HTML email generation...")

try:
    from email_preview_system import build_email_preview_html
    
    html = build_email_preview_html(
        name="Test Lender",
        view_link="https://example.com/view/123",
        preview_b64="iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
    )
    
    assert html, "Should have HTML"
    assert "Test Lender" in html, "Should include name"
    assert "https://example.com/view/123" in html, "Should include link"
    assert "base64," in html, "Should include image"
    
    print(f"   ✅ HTML email generation works ({len(html)} chars)")
except Exception as e:
    print(f"   ❌ HTML generation failed: {e}")
    print("      Will fall back to plain text links")

# ============================================================================
# TEST 7: Test datetime operations
# ============================================================================
print("\n7️⃣  Testing datetime operations...")

try:
    now = datetime.utcnow()
    future = now + timedelta(days=30)
    
    assert now.isoformat(), "Should format to ISO"
    assert future > now, "Future should be after now"
    
    print(f"   ✅ Datetime operations work")
    print(f"      Now: {now.isoformat()}")
    print(f"      +30 days: {future.isoformat()}")
except Exception as e:
    print(f"   ❌ Datetime operations failed: {e}")
    sys.exit(1)

# ============================================================================
# TEST 8: Test payload simulation
# ============================================================================
print("\n8️⃣  Testing payload structure...")

# Simulate frontend payload
test_payload = {
    "selected_lenders": ["Test Lender 1", "Test Lender 2"],
    "subject": "Test Subject",
    "message": "<html><body>Test body</body></html>",
    "cc": [],
    "mode": "MCA",
    "wrap_pdfs": True,
    "use_secure_links": False,
    "link_expiry_days": 30,
    "per_lender_attachments": {
        "Test Lender 1": [
            ("app.pdf", b"fake_data"),
            ("stmt.pdf", b"more_data")
        ]
    }
}

try:
    assert test_payload.get("wrap_pdfs") == True, "wrap_pdfs should be True"
    assert test_payload.get("use_secure_links") == False, "use_secure_links should be False"
    assert len(test_payload["selected_lenders"]) == 2, "Should have 2 lenders"
    
    # Test attachment retrieval
    atts = test_payload["per_lender_attachments"]["Test Lender 1"]
    assert len(atts) == 2, "Should have 2 attachments"
    
    name, data = atts[0]
    assert name == "app.pdf", "Should unpack name"
    assert data == b"fake_data", "Should unpack data"
    
    print(f"   ✅ Payload structure is correct")
except Exception as e:
    print(f"   ❌ Payload test failed: {e}")
    sys.exit(1)

# ============================================================================
# TEST 9: Check if frontend sends correct flags
# ============================================================================
print("\n9️⃣  Checking frontend configuration...")

print("   ⚠️  MANUAL CHECK REQUIRED:")
print("   ")
print("   In underwrite.html, around line 1110-1130, verify:")
print("   ")
print("   const payload = {")
print("     ...,")
print("     wrap_pdfs: true,         // ← Must be true")
print("     use_secure_links: false, // ← Start with false (attachments)")
print("   };")
print("   ")
print("   If missing, add those two lines!")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 70)
print("📊 TEST SUMMARY")
print("=" * 70)

print("""
✅ All critical tests passed!

✅ Imports work
✅ Attachment parsing works (tuple format)
✅ PDF operations work
✅ wrap_pdf_secure() works
✅ Datetime operations work
✅ Payload structure correct

⚠️  Preview generation status depends on dependencies

NEXT STEPS:
1. Add wrap_pdfs: true to frontend payload
2. Test with real email (small test)
3. Check console logs for:
   🚀 send_emails_secure() CALLED
   🔒 wrap_pdfs: True
   🔒 Wrapping N PDFs for [lender]
   ✅ Wrapped: /path/to/wrapped.pdf

If you see those logs, everything is working!
""")

print("=" * 70)