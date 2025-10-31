-- ============================================
-- FEEDBACK HUB DATABASE SCHEMA
-- Tables for storing lender responses, stips, and deal activity
-- ============================================

-- Lender Responses Table
-- Stores offers, declines, and stip requirements from lenders
CREATE TABLE IF NOT EXISTS lender_responses (
    id BIGSERIAL PRIMARY KEY,
    deal_id BIGINT NOT NULL,
    lender_name TEXT NOT NULL,
    lender_email TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('approved', 'stips', 'declined', 'pending')),

    -- Offer details (for approved status)
    amount DECIMAL(12, 2),
    factor_rate DECIMAL(4, 3),
    term INTEGER,  -- months
    payment DECIMAL(12, 2),
    conditions TEXT,

    -- Decline details (for declined status)
    decline_reason TEXT,

    -- Additional notes
    notes TEXT,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    FOREIGN KEY (deal_id) REFERENCES deals(id) ON DELETE CASCADE
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_lender_responses_deal_id ON lender_responses(deal_id);
CREATE INDEX IF NOT EXISTS idx_lender_responses_status ON lender_responses(status);


-- Stips Table
-- Tracks stipulation requirements from lenders
CREATE TABLE IF NOT EXISTS stips (
    id BIGSERIAL PRIMARY KEY,
    deal_id BIGINT NOT NULL,
    lender_name TEXT NOT NULL,
    requirement TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'completed', 'in_progress')),
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),

    FOREIGN KEY (deal_id) REFERENCES deals(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_stips_deal_id ON stips(deal_id);
CREATE INDEX IF NOT EXISTS idx_stips_status ON stips(status);


-- Deal Documents Table
-- Stores uploaded documents (stips, contracts, bank statements, etc.)
CREATE TABLE IF NOT EXISTS deal_documents (
    id BIGSERIAL PRIMARY KEY,
    deal_id BIGINT NOT NULL,
    file_name TEXT NOT NULL,
    file_path TEXT NOT NULL,
    file_size BIGINT,
    document_type TEXT NOT NULL DEFAULT 'stip' CHECK (document_type IN ('stip', 'contract', 'bank_statement', 'application', 'other')),
    lender_name TEXT,
    uploaded_by TEXT,
    uploaded_at TIMESTAMPTZ DEFAULT NOW(),

    FOREIGN KEY (deal_id) REFERENCES deals(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_deal_documents_deal_id ON deal_documents(deal_id);
CREATE INDEX IF NOT EXISTS idx_deal_documents_type ON deal_documents(document_type);


-- Deal Activity Table
-- Activity timeline for tracking all actions on a deal
CREATE TABLE IF NOT EXISTS deal_activity (
    id BIGSERIAL PRIMARY KEY,
    deal_id BIGINT NOT NULL,
    activity_type TEXT NOT NULL CHECK (activity_type IN ('deal_created', 'email_sent', 'response_received', 'stip_completed', 'document_uploaded', 'status_changed', 'funded', 'general')),
    description TEXT NOT NULL,
    lender_name TEXT,
    metadata JSONB,  -- Additional data in JSON format
    created_at TIMESTAMPTZ DEFAULT NOW(),

    FOREIGN KEY (deal_id) REFERENCES deals(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_deal_activity_deal_id ON deal_activity(deal_id);
CREATE INDEX IF NOT EXISTS idx_deal_activity_type ON deal_activity(activity_type);
CREATE INDEX IF NOT EXISTS idx_deal_activity_created_at ON deal_activity(created_at DESC);


-- Update existing deals table to add funding tracking
-- (Only if the deals table doesn't already have these columns)
ALTER TABLE deals ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'pending';
ALTER TABLE deals ADD COLUMN IF NOT EXISTS funded_lender TEXT;
ALTER TABLE deals ADD COLUMN IF NOT EXISTS funded_amount DECIMAL(12, 2);
ALTER TABLE deals ADD COLUMN IF NOT EXISTS funded_at TIMESTAMPTZ;


-- ============================================
-- USEFUL QUERIES
-- ============================================

-- Get all responses for a deal with stats
-- SELECT
--   COUNT(*) FILTER (WHERE status = 'approved') as approved_count,
--   COUNT(*) FILTER (WHERE status = 'declined') as declined_count,
--   COUNT(*) FILTER (WHERE status = 'stips') as stips_count
-- FROM lender_responses WHERE deal_id = ?;

-- Get pending stips for a deal
-- SELECT * FROM stips WHERE deal_id = ? AND status = 'pending' ORDER BY created_at DESC;

-- Get recent activity for a deal
-- SELECT * FROM deal_activity WHERE deal_id = ? ORDER BY created_at DESC LIMIT 20;

-- Get all documents for a deal grouped by type
-- SELECT document_type, COUNT(*), SUM(file_size) as total_size
-- FROM deal_documents WHERE deal_id = ? GROUP BY document_type;
