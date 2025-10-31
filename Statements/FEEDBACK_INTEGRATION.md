# Feedback Hub Integration Summary

## ✅ What's Wired Up (REAL Data)

### 1. Deal ID
- **Source**: Passed from dashboard when clicking deal row
- **Usage**: Used throughout the feedback hub to identify the deal
- **Flow**: Dashboard click → `openFeedbackHub(dealId)` → API call to `/api/underwrite/deal/${dealId}`

### 2. Business Name
- **Source**: `deal.application.business_name` from deal API
- **Fallback**: `deal.subject` if business name not available
- **Display**: Shows in feedback hub title as "Deal #123 - ABC Corporation"
- **Usage**: Also used in activity timeline descriptions

### 3. Lender Names
- **Source**: `deal.deliveries[].lender` from deal API
- **Display**: Used to generate mock responses with REAL lender names
- **Usage**:
  - Shows actual lenders the deal was submitted to
  - Each lender gets a mock status (approved/stips/declined/pending)
  - Appears in all tabs (Offers, Stips, Declines, Activity)

### 4. Lender Emails
- **Source**: `deal.deliveries[].to_email` from deal API
- **Display**: Shows actual email addresses in lender cards
- **Usage**: Ready for Reply and Send Update functionality

---

## 🎭 What's Mocked (For Now)

### Response Data
- **Offer amounts** - Random amounts ($45k-$60k)
- **Factor rates** - Mock rates (1.20-1.26)
- **Terms** - Mock terms (6 months)
- **Conditions** - Mock conditions ("COJ required", "None")
- **Response status** - Algorithmically distributed:
  - First lender: **Approved**
  - Second lender: **Approved** (if 4+ lenders)
  - Third lender: **Stips** (if 6+ lenders)
  - Last lender: **Declined** (if 5+ lenders)
  - Rest: **Pending**

### Stip Requirements
- Mock stip text: "Most recent bank statement, Photo ID, Voided check"
- Will be replaced with real data when lenders respond

### Activity Timeline
- Mock timestamps
- Generated based on real business name and lender count
- Activities include: submission, approvals, stips requests

### Documents
- Mock document list (Application.pdf, statements)
- Will be replaced with actual uploaded documents

---

## 📊 Mock Response Logic

```javascript
// Real lender names from deliveries
lenderNames = ["Capital First", "Quick Funding", "Smart Step", ...]

// Generated responses
- Capital First → APPROVED ($45,000 @ 1.20)
- Quick Funding → APPROVED ($50,000 @ 1.23) [if 4+ lenders]
- Smart Step → STIPS REQUIRED [if 6+ lenders]
- Last lender → DECLINED [if 5+ lenders]
- Others → PENDING (awaiting response)
```

---

## 🔌 Backend API Endpoints (Ready)

All endpoints in [feedback_api.py](feedback_api.py) are registered and ready:

- `GET /api/feedback/deal/<id>` - Get feedback data
- `POST /api/feedback/deal/<id>/response` - Add lender response
- `POST /api/feedback/deal/<id>/stip` - Add stip requirement
- `POST /api/feedback/deal/<id>/document` - Upload documents
- `POST /api/feedback/deal/<id>/activity` - Log activity
- `POST /api/feedback/deal/<id>/send-update` - Send email update
- `POST /api/feedback/deal/<id>/mark-funded` - Mark as funded
- `POST /api/feedback/deal/<id>/reply` - Reply to lender

**Current Mode**: Mock mode (returns sample data)
**To Enable**: Run `feedback_schema.sql` in Supabase

---

## 🎯 What Happens When You Click a Deal

1. **User clicks deal row** in Dashboard
2. **Console logs**:
   ```
   🖱️ Deal row clicked, ID: 123
   🔄 Loading deal data for ID: 123
   ✅ Deal loaded: { id: 123, business_name: "ABC Corp", ... }
   📊 Business: ABC Corporation
   📧 Lenders submitted to: ["Capital First", "Quick Funding", ...]
   📋 Generated mock responses for real lenders: [...]
   📅 Generated mock activity: [...]
   ```

3. **Feedback hub opens** with:
   - Title: "Deal #123 - ABC Corporation" (REAL)
   - Lender cards with REAL names and emails
   - Mock offer amounts, factors, and terms
   - Activity timeline with REAL business name

4. **All tabs work**:
   - **Overview**: Progress, stats, recent activity
   - **Offers**: Approved offers with amounts/terms
   - **Stips**: Lenders requesting documents
   - **Declines**: Declined with reasons
   - **Activity**: Timeline of all events
   - **Documents**: Original submission files

---

## 🚀 Next Steps to Make it Real

1. **Run Database Schema**:
   ```bash
   # In Supabase SQL editor
   Run: feedback_schema.sql
   ```

2. **Connect Frontend to Real API**:
   - Currently: Frontend generates mock responses
   - Future: Call `/api/feedback/deal/<id>` for real data

3. **Implement Email Integration**:
   - Send Update button → Actual email sending
   - Reply button → Email reply functionality

4. **Document Upload**:
   - Upload stips button → Save to `/uploads/feedback/<deal_id>/`
   - Track in `deal_documents` table

5. **Real Lender Responses**:
   - Parse incoming emails for lender responses
   - Extract offers, stips, declines
   - Store in `lender_responses` table

---

## 📝 Testing

1. **Go to Dashboard** (http://localhost:5056)
2. **Click any deal row**
3. **Feedback hub opens** with:
   - Real deal ID
   - Real business name
   - Real lender names (from deliveries)
   - Mock responses based on those real lenders

4. **Check browser console** to see:
   - Deal data being loaded
   - Real lender names extracted
   - Mock responses generated

---

## 🎨 Visual Demo

The feedback hub shows:
- **Green cards** = Approved offers
- **Yellow cards** = Stips required
- **Red cards** = Declined
- **Gray cards** = Pending (no response yet)

Each card displays the **actual lender name** and **email address** from your deal deliveries!

---

## 💡 Key Points

✅ **Real Data Being Used**:
- Deal ID
- Business Name
- Lender Names
- Lender Emails
- Delivery Count

🎭 **Mocked Until Backend Ready**:
- Offer amounts
- Factor rates
- Response statuses
- Stip requirements
- Activity timestamps

🔌 **Backend API**:
- All endpoints implemented
- Mock mode active (no database required)
- Ready to switch to real data when schema is deployed
