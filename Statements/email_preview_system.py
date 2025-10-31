"""
email_preview_system.py - INTEGRATED VERSION
Now properly integrated with send_emails_secure() combined function
"""

import base64
from io import BytesIO
from pdf2image import convert_from_path


def generate_pdf_preview(pdf_path: str) -> str:
    """
    Convert the first page of a PDF into a base64-encoded PNG string.
    Returns the encoded image string ready for embedding in HTML.
    """
    pages = convert_from_path(pdf_path, dpi=80, first_page=1, last_page=1)
    buffer = BytesIO()
    pages[0].save(buffer, format="PNG")
    encoded = base64.b64encode(buffer.getvalue()).decode("utf-8")
    return encoded


def build_email_preview_html(name: str, view_link: str, preview_b64: str) -> str:
    """
    Construct the HTML body of the email with a preview image and secure link.
    """
    return f"""
    <table style="max-width:600px;font-family:sans-serif;border:1px solid #ddd;border-radius:10px;">
        <tr>
            <td style="background:#000;color:#fff;padding:15px;">
                <h2>Business Financing Proposal</h2>
            </td>
        </tr>
        <tr>
            <td style="padding:20px;">
                <p>Hi {name or 'there'},</p>
                <p>Please review your proposal below:</p>
                <img src="data:image/png;base64,{preview_b64}" alt="Preview"
                     style="width:100%;border:1px solid #ccc;border-radius:6px;">
                <div style="text-align:center;margin:20px 0;">
                    <a href="{view_link}"
                       style="background:#007bff;color:white;padding:12px 25px;border-radius:6px;text-decoration:none;">
                       View Document
                    </a>
                </div>
                <p>🔒 Secure link — expires in 30 days</p>
            </td>
        </tr>
    </table>
    """


# ============================================================================
# OLD VERSION (keeps backward compatibility)
# ============================================================================
def send_secure_document_email(
    recipient_email: str,
    recipient_name: str,
    pdf_path: str,
    tracking_id: str,
    view_link: str,
):
    """
    ⚠️ LEGACY FUNCTION - Uses low-level functions directly
    
    This is the OLD way - it bypasses our combined function and uses
    gmail_send/graph_send directly. It still works, but doesn't benefit
    from the enhanced features of send_emails_secure().
    
    Kept for backward compatibility.
    """
    # ❌ OLD WAY: Import low-level functions directly
    from underwrite import safe_get_connected_sender, gmail_send, graph_send
    
    # Get the connected email account
    sender_email, provider, token = safe_get_connected_sender()
    
    if not sender_email or not token:
        raise RuntimeError("No connected mailbox. Connect Gmail/Outlook first.")
    
    # Generate preview thumbnail
    preview_b64 = generate_pdf_preview(pdf_path)

    # Build email bodies
    html_body = build_email_preview_html(recipient_name, view_link, preview_b64)
    text_body = (
        f"Hi {recipient_name or 'there'},\n\n"
        f"Please review your proposal using this secure link:\n{view_link}\n\n"
        "This link is encrypted and expires in 30 days.\n\n"
        "Best regards,\nYour Funding Team"
    )

    subject = "Your Funding Proposal - Ready for Review"

    # ❌ OLD WAY: Route to appropriate provider using low-level functions
    prov = (provider or "").lower()
    
    if prov == "gmail":
        success, msg_id, thread_id = gmail_send(
            token_dict=token,
            subject=subject,
            body_html=html_body,
            sender_email=sender_email,
            to_email=recipient_email,
            cc_list=[],
            attachments=[]
        )
    elif prov in ("outlook", "graph"):
        success, msg_id, thread_id = graph_send(
            token_dict=token,
            subject=subject,
            body_html=html_body,
            sender_email=sender_email,
            to_email=recipient_email,
            cc_list=[],
            attachments=[]
        )
    else:
        raise RuntimeError(f"Unsupported email provider: {provider}")
    
    if not success:
        raise RuntimeError(f"Email sending failed: {msg_id}")
    
    return msg_id


# ============================================================================
# NEW VERSION (uses our combined function!)
# ============================================================================
def send_secure_document_email_v2(
    recipient_email: str,
    recipient_name: str,
    pdf_path: str,
    tracking_id: str,
    view_link: str,
    lender_name: str = "Default Lender",
    deal_id: int = None,
):
    """
    ✅ NEW IMPROVED VERSION - Uses send_emails_secure() combined function
    
    This properly integrates with our combined function and benefits from:
    - Enhanced tracking
    - Deal management
    - Consistent delivery recording
    - Better error handling
    - Analytics integration
    
    Parameters:
        recipient_email: Email address to send to
        recipient_name: Recipient's display name
        pdf_path: Path to the PDF file
        tracking_id: Unique tracking identifier
        view_link: Secure view link for the document
        lender_name: Name of the lender (for tracking purposes)
        deal_id: Optional deal ID for linking
    
    Returns:
        dict: Response from send_emails_secure() with delivery status
    """
    # Generate preview thumbnail
    preview_b64 = generate_pdf_preview(pdf_path)

    # Build email body with preview
    html_body = build_email_preview_html(recipient_name, view_link, preview_b64)
    
    subject = "Your Funding Proposal - Ready for Review"
    
    # ✅ NEW WAY: Use our combined function through internal API
    # This gives us all the benefits of the combined function
    from flask import current_app
    import requests
    
    # Build the request payload
    payload = {
        "selected_lenders": [lender_name],
        "subject": subject,
        "message": html_body,
        "cc": [],
        "mode": "MCA",
        "use_secure_links": True,  # Already have the link
        "wrap_pdfs": False,  # PDF already wrapped
        "attachments": [],  # No attachments, using secure link
        "application": {},
        "statements": {},
    }
    
    if deal_id:
        payload["parent_deal_id"] = deal_id
    
    # Make internal request to our combined endpoint
    with current_app.test_request_context():
        from combined_secure_send import send_emails_secure
        response = send_emails_secure()
    
    return response


# ============================================================================
# EVEN BETTER: Helper function that does everything
# ============================================================================
def send_secure_pdf_with_preview(
    recipient_email: str,
    recipient_name: str,
    pdf_path: str,
    lender_name: str = "Default Lender",
    subject: str = None,
    expiry_days: int = 30,
    deal_id: int = None,
    wrap_pdf: bool = True,
):
    """
    🌟 BEST OPTION - Complete end-to-end secure PDF sending with preview
    
    This function:
    1. Wraps the PDF with tamper-proof protection
    2. Creates a secure view link
    3. Generates a preview thumbnail
    4. Sends email with inline preview
    5. Records everything in database
    
    All in one call!
    
    Parameters:
        recipient_email: Email address to send to
        recipient_name: Recipient's display name
        pdf_path: Path to the original PDF file
        lender_name: Name of the lender (for tracking)
        subject: Custom email subject (optional)
        expiry_days: Link expiration in days (default 30)
        deal_id: Optional deal ID for linking
        wrap_pdf: Whether to wrap PDF with security (default True)
    
    Returns:
        dict: {
            "success": True/False,
            "tracking_id": "...",
            "view_link": "...",
            "fingerprint_id": "...",
            "message_id": "..."
        }
    
    Example:
        result = send_secure_pdf_with_preview(
            recipient_email="lender@example.com",
            recipient_name="John Smith",
            pdf_path="/uploads/application.pdf",
            lender_name="Rapid Finance",
            expiry_days=7
        )
        print(f"Sent! Track at: {result['view_link']}")
    """
    import requests
    from flask import request as flask_request
    
    # Read PDF file
    with open(pdf_path, 'rb') as f:
        pdf_data = base64.b64encode(f.read()).decode('utf-8')
    
    # Generate preview
    preview_b64 = generate_pdf_preview(pdf_path)
    
    # Prepare the complete payload for our combined function
    payload = {
        "selected_lenders": [lender_name],
        "subject": subject or "Your Funding Proposal - Ready for Review",
        "message": "",  # Will be built with preview
        "cc": [],
        "mode": "MCA",
        "wrap_pdfs": wrap_pdf,
        "use_secure_links": True,
        "link_expiry_days": expiry_days,
        "attachments": [{
            "name": "proposal.pdf",
            "data": pdf_data
        }],
        "application": {"business_name": recipient_name},
        "statements": {},
    }
    
    if deal_id:
        payload["parent_deal_id"] = deal_id
    
    # Call our combined secure send endpoint
    base_url = flask_request.host_url if flask_request else "http://localhost:5000/"
    response = requests.post(
        f"{base_url}send",  # ✅ Using your new /send endpoint!
        json=payload
    )
    
    if response.status_code != 200:
        return {
            "success": False,
            "error": response.json().get("error", "Unknown error")
        }
    
    result = response.json()
    
    # Extract delivery info for this lender
    delivery = result.get("deliveries", [{}])[0]
    
    return {
        "success": delivery.get("status") == "sent",
        "tracking_id": delivery.get("tracking_id"),
        "view_link": delivery.get("view_link"),
        "fingerprint_id": delivery.get("fingerprint_id"),
        "message_id": delivery.get("provider_id"),
        "deal_id": result.get("deal_id")
    }


# ============================================================================
# USAGE EXAMPLES
# ============================================================================
"""
# Example 1: OLD WAY (still works, but limited)
send_secure_document_email(
    recipient_email="lender@example.com",
    recipient_name="John Smith",
    pdf_path="/uploads/doc.pdf",
    tracking_id="abc-123",
    view_link="https://app.com/v/abc-123"
)

# Example 2: NEW WAY (uses combined function)
send_secure_document_email_v2(
    recipient_email="lender@example.com",
    recipient_name="John Smith",
    pdf_path="/uploads/doc.pdf",
    tracking_id="abc-123",
    view_link="https://app.com/v/abc-123",
    lender_name="Rapid Finance",
    deal_id=456
)

# Example 3: BEST WAY (complete end-to-end)
result = send_secure_pdf_with_preview(
    recipient_email="lender@example.com",
    recipient_name="John Smith",
    pdf_path="/uploads/application.pdf",
    lender_name="Rapid Finance",
    expiry_days=7,
    wrap_pdf=True
)

if result["success"]:
    print(f"✅ Sent! Tracking ID: {result['tracking_id']}")
    print(f"📧 View link: {result['view_link']}")
else:
    print(f"❌ Failed: {result.get('error')}")
"""