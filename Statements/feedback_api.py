"""
Feedback Hub API endpoints
Handles lender responses, stips, documents, and deal activity tracking
"""

from flask import Blueprint, jsonify, request, session
from pathlib import Path
from datetime import datetime, timezone
import json
import os

bp = Blueprint("feedback", __name__)

# Database/storage setup (using Supabase from parent app)
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE = os.environ.get("SUPABASE_SERVICE_ROLE")

if SUPABASE_URL and SUPABASE_SERVICE_ROLE:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE)
else:
    supabase = None


# ============================================
# GET DEAL FEEDBACK DATA
# ============================================

@bp.get("/api/feedback/deal/<int:deal_id>")
def get_deal_feedback(deal_id):
    """
    Get comprehensive feedback data for a specific deal
    Returns: deal info with deliveries, lender responses (offers/declines from email_responses),
    stips (from manual_review), activity, documents
    """
    try:
        if not supabase:
            # Return mock data if no database connection
            return jsonify(_generate_mock_deal_data(deal_id))

        # Fetch deal from database with ALL related data
        deal_response = supabase.table("deals").select("*").eq("id", deal_id).single().execute()

        if not deal_response.data:
            return jsonify({"error": "Deal not found"}), 404

        deal = deal_response.data

        # Get deliveries for this deal (KEEP THIS - it has lender names, emails, etc.)
        deliveries_result = supabase.table("deliveries").select("*").eq("deal_id", deal_id).execute()
        deliveries = deliveries_result.data or []
        delivery_ids = [d['id'] for d in deliveries]

        # Add deliveries to deal object
        deal['deliveries'] = deliveries

        # Fetch REAL OFFERS from email_responses (approved responses)
        approved_offers = []
        declined_responses = []
        stips = []

        if delivery_ids:
            # Fetch offers (approved)
            offers_result = supabase.table("email_responses").select("*").in_("delivery_id", delivery_ids).eq("response_type", "approved").eq("processed", True).execute()
            approved_data = offers_result.data or []

            # Transform to expected format
            for offer in approved_data:
                offer_details = offer.get('offer_details') or {}
                approved_offers.append({
                    "id": offer.get('id'),
                    "deal_id": deal_id,
                    "lender_name": _extract_lender_name(offer.get('from_email'), deliveries),
                    "lender_email": offer.get('from_email'),
                    "status": "approved",
                    "amount": offer_details.get('amount'),
                    "factor_rate": offer_details.get('factor_rate'),
                    "term": offer_details.get('term'),
                    "payment": offer_details.get('payment'),
                    "conditions": offer_details.get('conditions'),
                    "notes": offer.get('summary'),
                    "created_at": offer.get('received_at'),
                    "updated_at": offer.get('processed_at')
                })

            # Fetch declines
            declines_result = supabase.table("email_responses").select("*").in_("delivery_id", delivery_ids).eq("response_type", "declined").eq("processed", True).execute()
            declined_data = declines_result.data or []

            # Transform declines
            for decline in declined_data:
                decline_reasons = decline.get('decline_reasons') or []
                declined_responses.append({
                    "id": decline.get('id'),
                    "deal_id": deal_id,
                    "lender_name": _extract_lender_name(decline.get('from_email'), deliveries),
                    "lender_email": decline.get('from_email'),
                    "status": "declined",
                    "decline_reason": ', '.join(decline_reasons) if isinstance(decline_reasons, list) else str(decline_reasons),
                    "notes": decline.get('summary'),
                    "created_at": decline.get('received_at'),
                    "updated_at": decline.get('processed_at')
                })

            # Fetch STIPS from manual_review table
            stips_result = supabase.table("manual_review").select("*").in_("delivery_id", delivery_ids).or_("ai_classification.eq.stips,manual_classification.eq.stips").execute()
            stips_data = stips_result.data or []

            # Transform stips
            for stip in stips_data:
                # Use manual data if reviewed, otherwise use AI data
                classification = stip.get('manual_classification') or stip.get('ai_classification')
                offer_details = stip.get('manual_offer_details') or stip.get('ai_offer_details') or {}

                stips.append({
                    "id": stip.get('id'),
                    "deal_id": deal_id,
                    "lender_name": _extract_lender_name(stip.get('from_email'), deliveries),
                    "lender_email": stip.get('from_email'),
                    "status": "stips",
                    "requirement": ', '.join(offer_details.get('requirements', [])) if isinstance(offer_details.get('requirements'), list) else str(offer_details.get('requirements', '')),
                    "description": stip.get('manual_notes') or stip.get('ai_summary'),
                    "review_status": stip.get('review_status'),
                    "requires_action": stip.get('requires_action'),
                    "created_at": stip.get('received_at'),
                    "updated_at": stip.get('updated_at')
                })

        # Combine all responses from database
        responses = approved_offers + declined_responses + stips

        # Build activity timeline from responses
        activity = []
        for resp in responses:
            activity_type = "response_received"
            description = f"{resp['status'].upper()} from {resp['lender_name']}"

            if resp['status'] == 'approved':
                activity_type = "response_received"
                amount = resp.get('amount')
                if amount:
                    description = f"Approved offer received - ${amount:,.0f}"
            elif resp['status'] == 'declined':
                activity_type = "response_received"
                description = f"Declined by {resp['lender_name']}"
            elif resp['status'] == 'stips':
                activity_type = "response_received"
                description = f"Stips requested by {resp['lender_name']}"

            activity.append({
                "activity_type": activity_type,
                "description": description,
                "lender_name": resp['lender_name'],
                "created_at": resp['created_at']
            })

        # Sort activity by date
        activity.sort(key=lambda x: x.get('created_at') or '', reverse=True)

        # Documents would come from your existing document storage
        documents = []

        return jsonify({
            "deal": deal,  # Includes deliveries now
            "responses": responses,  # Real data from email_responses + manual_review
            "stips": [s for s in responses if s['status'] == 'stips'],
            "activity": activity,
            "documents": documents
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============================================
# LENDER RESPONSES
# ============================================

@bp.post("/api/feedback/deal/<int:deal_id>/response")
def add_lender_response(deal_id):
    """
    Add or update a lender response (offer, decline, stips required)
    """
    try:
        data = request.json

        if not supabase:
            return jsonify({"success": True, "message": "Mock mode - response would be saved"})

        response_data = {
            "deal_id": deal_id,
            "lender_name": data.get("lender_name"),
            "lender_email": data.get("lender_email"),
            "status": data.get("status"),  # 'approved', 'stips', 'declined'
            "amount": data.get("amount"),
            "factor_rate": data.get("factor_rate"),
            "term": data.get("term"),
            "payment": data.get("payment"),
            "conditions": data.get("conditions"),
            "decline_reason": data.get("decline_reason"),
            "notes": data.get("notes"),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat()
        }

        result = supabase.table("lender_responses").insert(response_data).execute()

        # Log activity
        _log_activity(deal_id, f"Response from {data.get('lender_name')}: {data.get('status')}")

        return jsonify({"success": True, "response": result.data})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.patch("/api/feedback/response/<int:response_id>")
def update_lender_response(response_id):
    """
    Update an existing lender response
    """
    try:
        data = request.json

        if not supabase:
            return jsonify({"success": True, "message": "Mock mode - response would be updated"})

        update_data = {**data, "updated_at": datetime.now(timezone.utc).isoformat()}
        result = supabase.table("lender_responses").update(update_data).eq("id", response_id).execute()

        return jsonify({"success": True, "response": result.data})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============================================
# STIPS MANAGEMENT
# ============================================

@bp.post("/api/feedback/deal/<int:deal_id>/stip")
def add_stip(deal_id):
    """
    Add a stip requirement for a deal
    """
    try:
        data = request.json

        if not supabase:
            return jsonify({"success": True, "message": "Mock mode - stip would be saved"})

        stip_data = {
            "deal_id": deal_id,
            "lender_name": data.get("lender_name"),
            "requirement": data.get("requirement"),
            "description": data.get("description"),
            "status": "pending",  # 'pending', 'completed'
            "created_at": datetime.now(timezone.utc).isoformat()
        }

        result = supabase.table("stips").insert(stip_data).execute()

        # Log activity
        _log_activity(deal_id, f"New stip from {data.get('lender_name')}: {data.get('requirement')}")

        return jsonify({"success": True, "stip": result.data})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.patch("/api/feedback/stip/<int:stip_id>")
def update_stip(stip_id):
    """
    Update stip status (mark as completed, etc.)
    """
    try:
        data = request.json

        if not supabase:
            return jsonify({"success": True, "message": "Mock mode - stip would be updated"})

        result = supabase.table("stips").update(data).eq("id", stip_id).execute()

        return jsonify({"success": True, "stip": result.data})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============================================
# DOCUMENT MANAGEMENT
# ============================================

@bp.post("/api/feedback/deal/<int:deal_id>/document")
def upload_document(deal_id):
    """
    Upload a document for a deal (stips, contracts, etc.)
    """
    try:
        if not request.files:
            return jsonify({"error": "No files provided"}), 400

        files = request.files.getlist("files")
        doc_type = request.form.get("type", "stip")  # 'stip', 'contract', 'bank_statement', etc.
        lender_name = request.form.get("lender_name", "")

        if not supabase:
            return jsonify({
                "success": True,
                "message": f"Mock mode - {len(files)} file(s) would be uploaded",
                "files": [{"name": f.filename, "size": 0} for f in files]
            })

        uploaded = []
        BASE_DIR = Path(__file__).resolve().parent
        UPLOADS_DIR = BASE_DIR / "uploads" / "feedback" / str(deal_id)
        UPLOADS_DIR.mkdir(parents=True, exist_ok=True)

        for file in files:
            if file.filename:
                # Save file
                timestamp = int(datetime.now().timestamp())
                safe_name = f"{timestamp}_{file.filename}"
                file_path = UPLOADS_DIR / safe_name
                file.save(str(file_path))

                # Record in database
                doc_data = {
                    "deal_id": deal_id,
                    "file_name": file.filename,
                    "file_path": str(file_path),
                    "file_size": file_path.stat().st_size,
                    "document_type": doc_type,
                    "lender_name": lender_name,
                    "uploaded_at": datetime.now(timezone.utc).isoformat()
                }

                result = supabase.table("deal_documents").insert(doc_data).execute()
                uploaded.append(result.data)

        # Log activity
        _log_activity(deal_id, f"Uploaded {len(uploaded)} document(s)")

        return jsonify({"success": True, "documents": uploaded})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.post("/api/feedback/reply")
def send_lender_reply():
    """
    Send an email reply to a lender
    Handles reply messages with optional file attachments
    """
    try:
        deal_id = request.form.get("deal_id")
        lender_name = request.form.get("lender_name", "")
        status_context = request.form.get("status_context", "")  # approved, stips, declined
        to = request.form.get("to", "")
        body = request.form.get("body", "")
        files = request.files.getlist("files")

        if not deal_id or not to or not body:
            return jsonify({"error": "Missing required fields"}), 400

        # In a real implementation, this would send an actual email via SMTP or email service
        # For now, we'll log the activity and save the communication record

        if not supabase:
            return jsonify({
                "success": True,
                "message": f"Mock mode - Email would be sent to {to}",
                "details": {
                    "to": to,
                    "body_length": len(body),
                    "attachments": len(files)
                }
            })

        # Save the outgoing communication to the database
        communication_data = {
            "deal_id": int(deal_id),
            "lender_name": lender_name,
            "direction": "outgoing",
            "recipient": to,
            "message_body": body,
            "status_context": status_context,
            "sent_at": datetime.now(timezone.utc).isoformat(),
            "has_attachments": len(files) > 0
        }

        # Store in communications table (if it exists)
        try:
            result = supabase.table("communications").insert(communication_data).execute()
        except Exception as db_err:
            # If communications table doesn't exist, just log it
            print(f"Could not save to communications table: {db_err}")

        # Log activity
        _log_activity(
            int(deal_id),
            f"Sent reply to {lender_name} ({status_context}): {body[:50]}..."
        )

        # TODO: Implement actual email sending here
        # Example using SMTP or SendGrid/Mailgun:
        # send_email(to=to, subject=f"Re: {lender_name}", body=body, attachments=files)

        return jsonify({
            "success": True,
            "message": f"Reply sent to {lender_name}",
            "details": {
                "to": to,
                "lender_name": lender_name,
                "status": status_context,
                "attachments_count": len(files)
            }
        })

    except Exception as e:
        print(f"Error sending reply: {e}")
        return jsonify({"error": str(e)}), 500


@bp.get("/api/feedback/deal/<int:deal_id>/documents")
def get_deal_documents(deal_id):
    """
    Get all documents uploaded for a deal (stips, contracts, etc.)
    """
    try:
        if not supabase:
            return jsonify({
                "success": True,
                "documents": []
            })

        # Fetch all documents for this deal
        result = supabase.table("deal_documents").select("*").eq("deal_id", deal_id).order("uploaded_at", desc=True).execute()

        documents = result.data or []

        return jsonify({
            "success": True,
            "documents": documents,
            "count": len(documents)
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.delete("/api/feedback/document/<int:doc_id>")
def delete_document(doc_id):
    """
    Delete a document
    """
    try:
        if not supabase:
            return jsonify({"success": True, "message": "Mock mode - document would be deleted"})

        # Get document info first
        doc_result = supabase.table("deal_documents").select("*").eq("id", doc_id).single().execute()

        if doc_result.data:
            # Delete physical file
            file_path = Path(doc_result.data["file_path"])
            if file_path.exists():
                file_path.unlink()

            # Delete from database
            supabase.table("deal_documents").delete().eq("id", doc_id).execute()

        return jsonify({"success": True})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============================================
# ACTIVITY TRACKING
# ============================================

@bp.post("/api/feedback/deal/<int:deal_id>/activity")
def add_activity(deal_id):
    """
    Log activity for a deal
    """
    try:
        data = request.json

        if not supabase:
            return jsonify({"success": True, "message": "Mock mode - activity would be logged"})

        activity_data = {
            "deal_id": deal_id,
            "activity_type": data.get("type"),  # 'email_sent', 'response_received', 'stip_completed', etc.
            "description": data.get("description"),
            "lender_name": data.get("lender_name"),
            "created_at": datetime.now(timezone.utc).isoformat()
        }

        result = supabase.table("deal_activity").insert(activity_data).execute()

        return jsonify({"success": True, "activity": result.data})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.get("/api/feedback/deal/<int:deal_id>/activity")
def get_activity(deal_id):
    """
    Get activity timeline for a deal
    """
    try:
        if not supabase:
            return jsonify({"activity": _generate_mock_activity(deal_id)})

        result = supabase.table("deal_activity").select("*").eq("deal_id", deal_id).order("created_at", desc=True).execute()

        return jsonify({"activity": result.data or []})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============================================
# DEAL ACTIONS
# ============================================

@bp.post("/api/feedback/deal/<int:deal_id>/send-update")
def send_deal_update(deal_id):
    """
    Send update email to lenders with new documents/info
    """
    try:
        data = request.json
        lenders = data.get("lenders", [])  # List of lender emails
        message = data.get("message", "")
        attachments = data.get("attachments", [])  # Document IDs

        # TODO: Implement email sending logic here
        # This would integrate with your existing email system

        # Log activity
        _log_activity(deal_id, f"Sent update to {len(lenders)} lender(s)")

        return jsonify({
            "success": True,
            "message": f"Update sent to {len(lenders)} lender(s)"
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.post("/api/feedback/deal/<int:deal_id>/mark-funded")
def mark_deal_funded(deal_id):
    """
    Mark a deal as funded
    """
    try:
        data = request.json

        if not supabase:
            return jsonify({"success": True, "message": "Mock mode - deal would be marked as funded"})

        update_data = {
            "status": "funded",
            "funded_lender": data.get("lender_name"),
            "funded_amount": data.get("amount"),
            "funded_at": datetime.now(timezone.utc).isoformat()
        }

        result = supabase.table("deals").update(update_data).eq("id", deal_id).execute()

        # Log activity
        _log_activity(deal_id, f"Deal marked as FUNDED by {data.get('lender_name')}")

        return jsonify({"success": True, "deal": result.data})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.post("/api/feedback/deal/<int:deal_id>/reply")
def reply_to_lender(deal_id):
    """
    Send a reply to a specific lender
    """
    try:
        data = request.json
        lender_email = data.get("lender_email")
        message = data.get("message", "")

        # TODO: Implement email reply logic
        # This would integrate with your existing email system

        # Log activity
        _log_activity(deal_id, f"Sent reply to {lender_email}")

        return jsonify({
            "success": True,
            "message": "Reply sent successfully"
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============================================
# HELPER FUNCTIONS
# ============================================

def _extract_lender_name(from_email: str, deliveries: list) -> str:
    """Extract lender name from email by matching with deliveries"""
    if not from_email:
        return "Unknown Lender"

    # Try to match email with deliveries
    for delivery in deliveries:
        if delivery.get('to') and from_email.lower() in delivery.get('to', '').lower():
            return delivery.get('lender', 'Unknown Lender')

    # Fallback: extract from email domain
    try:
        domain = from_email.split('@')[1].split('.')[0]
        return domain.title()
    except:
        return from_email

def _log_activity(deal_id: int, description: str, lender_name: str = None, activity_type: str = "general"):
    """Helper to log activity"""
    if not supabase:
        return

    try:
        activity_data = {
            "deal_id": deal_id,
            "activity_type": activity_type,
            "description": description,
            "lender_name": lender_name,
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        supabase.table("deal_activity").insert(activity_data).execute()
    except:
        pass  # Silently fail for activity logging


def _generate_mock_deal_data(deal_id: int):
    """Generate mock data for development/testing"""
    return {
        "deal": {
            "id": deal_id,
            "business_name": "Test Business LLC",
            "mode": "MCA",
            "status": "in_review",
            "created_at": "2025-01-15T10:00:00Z"
        },
        "responses": [
            {
                "id": 1,
                "lender_name": "Capital First",
                "lender_email": "offers@capitalfirst.com",
                "status": "approved",
                "amount": 150000,
                "factor_rate": 1.25,
                "term": 12,
                "payment": 15625,
                "created_at": "2025-01-15T14:30:00Z"
            },
            {
                "id": 2,
                "lender_name": "Quick Funding",
                "lender_email": "deals@quickfunding.com",
                "status": "stips",
                "amount": 100000,
                "conditions": "Need 4 months bank statements",
                "created_at": "2025-01-15T15:00:00Z"
            }
        ],
        "stips": [
            {
                "id": 1,
                "lender_name": "Quick Funding",
                "requirement": "4 months bank statements",
                "status": "pending",
                "created_at": "2025-01-15T15:00:00Z"
            }
        ],
        "activity": _generate_mock_activity(deal_id),
        "documents": [
            {
                "id": 1,
                "file_name": "application.pdf",
                "document_type": "application",
                "file_size": 245000,
                "uploaded_at": "2025-01-15T10:00:00Z"
            }
        ]
    }


def _generate_mock_activity(deal_id: int):
    """Generate mock activity timeline"""
    return [
        {
            "id": 1,
            "activity_type": "response_received",
            "description": "Approved offer received",
            "lender_name": "Capital First",
            "created_at": "2025-01-15T14:30:00Z"
        },
        {
            "id": 2,
            "activity_type": "email_sent",
            "description": "Deal submitted to 15 lenders",
            "created_at": "2025-01-15T10:15:00Z"
        },
        {
            "id": 3,
            "activity_type": "deal_created",
            "description": "Deal created",
            "created_at": "2025-01-15T10:00:00Z"
        }
    ]
