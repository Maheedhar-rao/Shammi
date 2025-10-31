# Feedback Hub API Documentation

Complete REST API for managing lender responses, stips, documents, and deal activity.

## Base URL
All endpoints are prefixed with `/api/feedback`

---

## Endpoints

### 📊 Get Deal Feedback Data

**GET** `/api/feedback/deal/<deal_id>`

Get comprehensive feedback data for a specific deal including responses, stips, activity, and documents.

**Response:**
```json
{
  "deal": {
    "id": 123,
    "business_name": "ABC Corp",
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
    }
  ],
  "stips": [...],
  "activity": [...],
  "documents": [...]
}
```

---

### 📝 Add Lender Response

**POST** `/api/feedback/deal/<deal_id>/response`

Add or update a lender response (offer, decline, or stips required).

**Request Body:**
```json
{
  "lender_name": "Quick Funding",
  "lender_email": "deals@quickfunding.com",
  "status": "approved",  // 'approved', 'stips', 'declined'
  "amount": 100000,
  "factor_rate": 1.30,
  "term": 12,
  "payment": 10833,
  "conditions": "Subject to final approval",
  "decline_reason": null,
  "notes": "Fast approval"
}
```

**Response:**
```json
{
  "success": true,
  "response": { /* response object */ }
}
```

---

### ✏️ Update Lender Response

**PATCH** `/api/feedback/response/<response_id>`

Update an existing lender response.

**Request Body:**
```json
{
  "status": "approved",
  "amount": 125000,
  "notes": "Increased offer amount"
}
```

---

### 📋 Add Stip

**POST** `/api/feedback/deal/<deal_id>/stip`

Add a stip requirement for a deal.

**Request Body:**
```json
{
  "lender_name": "Quick Funding",
  "requirement": "4 months bank statements",
  "description": "Need complete statements with all pages"
}
```

**Response:**
```json
{
  "success": true,
  "stip": {
    "id": 5,
    "deal_id": 123,
    "lender_name": "Quick Funding",
    "requirement": "4 months bank statements",
    "status": "pending",
    "created_at": "2025-01-15T15:00:00Z"
  }
}
```

---

### ✅ Update Stip

**PATCH** `/api/feedback/stip/<stip_id>`

Update stip status (mark as completed, etc.).

**Request Body:**
```json
{
  "status": "completed",
  "completed_at": "2025-01-16T10:30:00Z"
}
```

---

### 📎 Upload Document

**POST** `/api/feedback/deal/<deal_id>/document`

Upload documents for a deal (stips, contracts, bank statements).

**Request:** `multipart/form-data`
- `files`: File(s) to upload (multiple allowed)
- `type`: Document type ('stip', 'contract', 'bank_statement', 'application', 'other')
- `lender_name`: Associated lender (optional)

**Response:**
```json
{
  "success": true,
  "documents": [
    {
      "id": 10,
      "deal_id": 123,
      "file_name": "bank_statements_jan.pdf",
      "file_path": "/uploads/feedback/123/1705415000_bank_statements_jan.pdf",
      "file_size": 2450000,
      "document_type": "bank_statement",
      "uploaded_at": "2025-01-16T11:00:00Z"
    }
  ]
}
```

---

### 🗑️ Delete Document

**DELETE** `/api/feedback/document/<doc_id>`

Delete a document from a deal.

**Response:**
```json
{
  "success": true
}
```

---

### 📝 Add Activity

**POST** `/api/feedback/deal/<deal_id>/activity`

Log activity for a deal.

**Request Body:**
```json
{
  "type": "response_received",  // 'email_sent', 'response_received', 'stip_completed', etc.
  "description": "Approved offer received from Capital First",
  "lender_name": "Capital First"
}
```

---

### 📅 Get Activity Timeline

**GET** `/api/feedback/deal/<deal_id>/activity`

Get activity timeline for a deal.

**Response:**
```json
{
  "activity": [
    {
      "id": 15,
      "activity_type": "response_received",
      "description": "Approved offer received",
      "lender_name": "Capital First",
      "created_at": "2025-01-15T14:30:00Z"
    },
    {
      "id": 14,
      "activity_type": "email_sent",
      "description": "Deal submitted to 15 lenders",
      "created_at": "2025-01-15T10:15:00Z"
    }
  ]
}
```

---

### 📧 Send Deal Update

**POST** `/api/feedback/deal/<deal_id>/send-update`

Send update email to lenders with new documents/info.

**Request Body:**
```json
{
  "lenders": ["lender1@email.com", "lender2@email.com"],
  "message": "Please find attached updated bank statements",
  "attachments": [10, 11, 12]  // Document IDs
}
```

**Response:**
```json
{
  "success": true,
  "message": "Update sent to 2 lender(s)"
}
```

---

### 💰 Mark Deal as Funded

**POST** `/api/feedback/deal/<deal_id>/mark-funded`

Mark a deal as funded.

**Request Body:**
```json
{
  "lender_name": "Capital First",
  "amount": 150000
}
```

**Response:**
```json
{
  "success": true,
  "deal": {
    "id": 123,
    "status": "funded",
    "funded_lender": "Capital First",
    "funded_amount": 150000,
    "funded_at": "2025-01-16T14:00:00Z"
  }
}
```

---

### 💬 Reply to Lender

**POST** `/api/feedback/deal/<deal_id>/reply`

Send a reply to a specific lender.

**Request Body:**
```json
{
  "lender_email": "offers@capitalfirst.com",
  "message": "Thank you for the offer. Can you provide more details on the terms?"
}
```

**Response:**
```json
{
  "success": true,
  "message": "Reply sent successfully"
}
```

---

## Response Status Codes

- `200` - Success
- `400` - Bad Request (invalid data)
- `404` - Not Found (deal/resource doesn't exist)
- `500` - Internal Server Error

---

## Database Schema

See `feedback_schema.sql` for the complete database schema including:
- `lender_responses` - Lender offers, declines, stips
- `stips` - Stipulation requirements
- `deal_documents` - Uploaded files
- `deal_activity` - Activity timeline

---

## Integration with Frontend

The frontend feedback hub ([pub/js/components/feedback-hub.js](pub/js/components/feedback-hub.js)) automatically calls these endpoints when:

1. Opening a deal → `GET /api/feedback/deal/<id>`
2. Uploading stips → `POST /api/feedback/deal/<id>/document`
3. Sending updates → `POST /api/feedback/deal/<id>/send-update`
4. Marking as funded → `POST /api/feedback/deal/<id>/mark-funded`

---

## Mock Mode

When `SUPABASE_URL` or `SUPABASE_SERVICE_ROLE` environment variables are not set, the API runs in **mock mode** and returns sample data without requiring a database. This is useful for development and testing.

---

## Next Steps

1. **Run the SQL schema**: Execute `feedback_schema.sql` in your Supabase database
2. **Test with mock data**: The API returns mock data when database is not configured
3. **Integrate email sending**: Implement email logic in `send_deal_update()` and `reply_to_lender()`
4. **Add authentication**: Endpoints inherit authentication from `app.before_request` in app1.py

---

## Example Usage

```javascript
// Get deal feedback data
const response = await fetch('/api/feedback/deal/123');
const data = await response.json();

// Upload stips
const formData = new FormData();
formData.append('files', file1);
formData.append('files', file2);
formData.append('type', 'stip');
formData.append('lender_name', 'Quick Funding');

await fetch('/api/feedback/deal/123/document', {
  method: 'POST',
  body: formData,
  credentials: 'include'
});

// Mark as funded
await fetch('/api/feedback/deal/123/mark-funded', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    lender_name: 'Capital First',
    amount: 150000
  }),
  credentials: 'include'
});
```
