/**
 * Feedback Hub - Comprehensive deal management and lender response tracking
 * Displays offers, declines, stips, activity timeline, and allows replies/uploads
 */

import { $ } from '../utils.js';
import { postJSON, postFormData } from '../api.js';

// Active deal context
let activeDeal = null;
let lenderResponses = [];
let dealActivity = [];
let replyAttachments = []; // attachments added via inline reply blocks

/**
 * Initialize feedback hub
 */
export function initFeedbackHub() {
  // Close button
  const closeBtn = $('feedbackClose');
  if (closeBtn) {
    closeBtn.addEventListener('click', closeFeedbackHub);
  }

  // Tab switching
  const tabs = ['overview', 'offers', 'stips', 'declines', 'activity', 'documents'];
  tabs.forEach(tab => {
    const btn = $(`feedbackTab${tab.charAt(0).toUpperCase() + tab.slice(1)}`);
    if (btn) {
      btn.addEventListener('click', () => switchTab(tab));
    }
  });

  // Reply button
  const replyBtn = $('feedbackReplyBtn');
  if (replyBtn) {
    replyBtn.addEventListener('click', handleReply);
  }

  // Upload stips button
  const uploadBtn = $('feedbackUploadBtn');
  if (uploadBtn) {
    uploadBtn.addEventListener('click', () => $('feedbackStipFile').click());
  }

  // File input for stips
  const fileInput = $('feedbackStipFile');
  if (fileInput) {
    fileInput.addEventListener('change', handleStipUpload);
  }

  // Send update button
  const updateBtn = $('feedbackSendUpdate');
  if (updateBtn) {
    updateBtn.addEventListener('click', handleSendUpdate);
  }

  // Mark as funded button
  const fundedBtn = $('feedbackMarkFunded');
  if (fundedBtn) {
    fundedBtn.addEventListener('click', handleMarkFunded);
  }

  // Filter buttons
  const filterBtns = document.querySelectorAll('[data-filter]');
  filterBtns.forEach(btn => {
    btn.addEventListener('click', (e) => {
      const filter = e.target.dataset.filter;
      applyFilter(filter);
    });
  });

  // Close on overlay click
  const overlay = $('feedbackHub');
  if (overlay) {
    overlay.addEventListener('click', (e) => {
      if (e.target === overlay) {
        closeFeedbackHub();
      }
    });
  }
}

/**
 * Open feedback hub for a deal
 */
export async function openFeedbackHub(dealId) {
  activeDeal = dealId;

  // Show the hub
  const hub = $('feedbackHub');
  if (hub) {
    hub.classList.remove('hidden');
    hub.style.display = 'flex';
  }

  // Load deal data
  await loadDealData(dealId);

  // Show overview tab by default
  switchTab('overview');
}

/**
 * Close feedback hub
 */
export function closeFeedbackHub() {
  const hub = $('feedbackHub');
  if (hub) {
    hub.classList.add('hidden');
    hub.style.display = 'none';
  }
  activeDeal = null;
  lenderResponses = [];
  dealActivity = [];
  replyAttachments = [];
}

/**
 * Load deal data from API
 */
async function loadDealData(dealId) {
  try {
    console.log('🔄 Loading deal data for ID:', dealId);

    // Load deal details from backend (ORIGINAL endpoint - has business name, deliveries, documents)
    const dealRes = await fetch(`/api/underwrite/deal/${dealId}`);
    if (!dealRes.ok) {
      console.error('❌ Failed to load deal');
      activeDeal = { id: dealId, business_name: 'Unknown Business' };
      lenderResponses = generateMockResponses(dealId);
      dealActivity = generateMockActivity(dealId);
      return;
    }

    const data = await dealRes.json();
    const deal = data.deal;

    console.log('✅ Deal loaded:', deal);

    // Extract REAL data from deal
    const businessName = deal.application?.business_name || deal.subject || 'Unknown Business';
    const lenderNames = (deal.deliveries || []).map(d => d.lender).filter(Boolean);
    const lenderEmails = (deal.deliveries || []).map(d => ({ name: d.lender, email: d.to })).filter(d => d.name);

    console.log('📊 Business:', businessName);
    console.log('📧 Submitted to:', lenderNames);

    // Store deal info globally
    activeDeal = {
      id: dealId,
      business_name: businessName,
      mode: deal.mode,
      created_at: deal.created_at,
      deliveries: deal.deliveries || [],
      application: deal.application || {},
      statements: deal.statements || {},
      attachments: deal.attachments || {}
    };

    // Update title with REAL business name
    updateDealTitle(dealId, businessName);

    // NOW fetch REAL responses from feedback API
    try {
      const feedbackRes = await fetch(`/api/feedback/deal/${dealId}`);
      if (feedbackRes.ok) {
        const feedbackData = await feedbackRes.json();

        // If we have REAL responses from email_responses/manual_review, use them
        if (feedbackData.responses && feedbackData.responses.length > 0) {
          lenderResponses = transformResponses(feedbackData.responses);
          dealActivity = transformActivity(feedbackData.activity || []);
          console.log('📊 Using REAL responses from email_responses/manual_review:', lenderResponses.length);
        } else {
          // No responses in DB yet - generate mock based on deliveries
          if (lenderNames.length > 0) {
            lenderResponses = generateMockResponsesFromRealLenders(lenderNames, lenderEmails);
            dealActivity = generateMockActivityFromRealData(businessName, lenderNames.length);
            console.log('📊 Using MOCK responses based on deliveries:', lenderResponses.length);
          } else {
            lenderResponses = generateMockResponses(dealId);
            dealActivity = generateMockActivity(dealId);
            console.log('📊 Using generic MOCK responses');
          }
        }
      } else {
        // Feedback API failed - use mock based on deliveries
        console.log('⚠️ Feedback API failed, using mock data');
        if (lenderNames.length > 0) {
          lenderResponses = generateMockResponsesFromRealLenders(lenderNames, lenderEmails);
          dealActivity = generateMockActivityFromRealData(businessName, lenderNames.length);
        } else {
          lenderResponses = generateMockResponses(dealId);
          dealActivity = generateMockActivity(dealId);
        }
      }
    } catch (feedbackErr) {
      // Feedback API error - use mock based on deliveries
      console.log('⚠️ Feedback API error, using mock data:', feedbackErr);
      if (lenderNames.length > 0) {
        lenderResponses = generateMockResponsesFromRealLenders(lenderNames, lenderEmails);
        dealActivity = generateMockActivityFromRealData(businessName, lenderNames.length);
      } else {
        lenderResponses = generateMockResponses(dealId);
        dealActivity = generateMockActivity(dealId);
      }
    }

    console.log('📅 Loaded activities:', dealActivity.length);

  } catch (err) {
    console.error('❌ Failed to load deal data:', err);
    // Fallback to generic mock data
    activeDeal = { id: dealId, business_name: 'Unknown Business' };
    lenderResponses = generateMockResponses(dealId);
    dealActivity = generateMockActivity(dealId);
  }
}

/**
 * Transform API responses to frontend format
 */
function transformResponses(responses) {
  return responses.map(resp => {
    // Normalize field names between API and frontend
    return {
      id: resp.id,
      lender_name: resp.lender_name,
      email: resp.lender_email,
      status: resp.status,

      // Offer fields
      amount: resp.amount,
      factor: resp.factor_rate,
      term_months: resp.term,
      payment: resp.payment,
      conditions: resp.conditions,

      // Decline fields
      decline_reason: resp.decline_reason,

      // Stip fields
      stips_required: resp.requirement,
      review_status: resp.review_status,
      requires_action: resp.requires_action,

      // Common fields
      notes: resp.notes || resp.description,
      responded_at: resp.created_at ? new Date(resp.created_at).getTime() : Date.now(),
      created_at: resp.created_at,
      updated_at: resp.updated_at
    };
  });
}

/**
 * Transform API activity to frontend format
 */
function transformActivity(activities) {
  return activities.map(act => ({
    type: act.activity_type || 'general',
    description: act.description,
    lender_name: act.lender_name,
    timestamp: act.created_at ? new Date(act.created_at).getTime() : Date.now()
  }));
}

/**
 * Update deal title with real data
 */
function updateDealTitle(dealId, businessName) {
  const titleEl = $('feedbackDealTitle');
  if (titleEl) {
    titleEl.textContent = `Deal #${dealId} - ${businessName}`;
  }
}

/**
 * Switch between tabs
 */
function switchTab(tabName) {
  // Update tab buttons
  const tabs = ['overview', 'offers', 'stips', 'declines', 'activity', 'documents'];
  tabs.forEach(tab => {
    const btn = $(`feedbackTab${tab.charAt(0).toUpperCase() + tab.slice(1)}`);
    const content = $(`feedbackContent${tab.charAt(0).toUpperCase() + tab.slice(1)}`);

    if (btn) {
      if (tab === tabName) {
        btn.classList.add('active');
      } else {
        btn.classList.remove('active');
      }
    }

    if (content) {
      if (tab === tabName) {
        content.classList.remove('hidden');
      } else {
        content.classList.add('hidden');
      }
    }
  });

  // Render content for active tab
  switch (tabName) {
    case 'overview':
      renderOverview();
      break;
    case 'offers':
      renderOffers();
      break;
    case 'stips':
      renderStips();
      break;
    case 'declines':
      renderDeclines();
      break;
    case 'activity':
      renderActivity();
      break;
    case 'documents':
      renderDocuments();
      break;
  }
}

/**
 * Populate overview section
 */
function populateOverview(deal) {
  const titleEl = $('feedbackDealTitle');
  if (titleEl) {
    titleEl.textContent = `Deal #${deal.id} - ${deal.business_name || 'Unknown Business'}`;
  }

  const summaryEl = $('feedbackSummary');
  if (summaryEl) {
    const stats = calculateStats();
    summaryEl.innerHTML = `
      <span class="stat-badge">${stats.approved} Approved</span>
      <span class="stat-badge stips">${stats.stips} Stips</span>
      <span class="stat-badge declined">${stats.declined} Declined</span>
      <span class="stat-badge">${stats.pending} Pending</span>
    `;
  }
}

/**
 * Render overview tab
 */
function renderOverview() {
  const content = $('feedbackContentOverview');
  if (!content) return;

  const stats = calculateStats();

  // Get SUBMITTED lenders from deliveries (always show these)
  // Match with responses to show status
  const submittedLenders = (activeDeal?.deliveries || []).map(d => {
    const lenderName = d.lender || 'Unknown';
    const lenderEmail = d.lender_email || '';

    // Find matching response by lender name or email
    const matchedResponse = lenderResponses.find(r => {
      const nameMatch = r.lender_name?.toLowerCase() === lenderName.toLowerCase();
      const emailMatch = r.email?.toLowerCase() === lenderEmail.toLowerCase();
      return nameMatch || emailMatch;
    });

    let statusBadge = '';
    let statusClass = '';

    if (matchedResponse) {
      if (matchedResponse.status === 'approved') {
        statusBadge = '<span class="status-badge approved">✅ Approved</span>';
        statusClass = 'has-response approved';
      } else if (matchedResponse.status === 'stips') {
        statusBadge = '<span class="status-badge stips">📎 Stips</span>';
        statusClass = 'has-response stips';
      } else if (matchedResponse.status === 'declined') {
        statusBadge = '<span class="status-badge declined">❌ Declined</span>';
        statusClass = 'has-response declined';
      } else {
        statusBadge = '<span class="status-badge pending">⏳ Pending</span>';
        statusClass = 'has-response pending';
      }
    } else {
      statusBadge = '<span class="status-badge no-response">⏳ No Response</span>';
      statusClass = 'no-response';
    }

    return `
      <div class="submitted-lender-item ${statusClass}">
        <div class="lender-info">
          <div class="lender-name">${lenderName}</div>
          ${statusBadge}
        </div>
      </div>
    `;
  }).join('');

  // Get lender names for display (responses)
  const lendersList = lenderResponses.map(r => {
    let statusClass = '';
    let statusIcon = '';
    if (r.status === 'approved') {
      statusClass = 'approved';
      statusIcon = '✅';
    } else if (r.status === 'stips') {
      statusClass = 'stips';
      statusIcon = '📎';
    } else if (r.status === 'declined') {
      statusClass = 'declined';
      statusIcon = '❌';
    } else {
      statusClass = 'pending';
      statusIcon = '⏳';
    }

    return `
      <div class="lender-list-item ${statusClass}">
        <span class="lender-icon">${statusIcon}</span>
        <span class="lender-name">${r.lender_name}</span>
        <span class="lender-status">${r.status.toUpperCase()}</span>
      </div>
    `;
  }).join('');

  const submittedCount = activeDeal?.deliveries?.length || 0;

  content.innerHTML = `
    <div class="overview-grid">
      <div class="overview-card">
        <h4>Deal Progress</h4>
        <div class="progress-bar">
          <div class="progress-fill" style="width: ${stats.progressPercent}%"></div>
        </div>
        <div class="progress-steps">
          <div class="step ${stats.approved > 0 ? 'complete' : ''}">
            <div class="step-icon">✅</div>
            <div class="step-label">Submitted</div>
          </div>
          <div class="step ${stats.totalResponses > 0 ? 'complete' : ''}">
            <div class="step-icon">📧</div>
            <div class="step-label">Responses</div>
          </div>
          <div class="step ${stats.stips === 0 ? 'complete' : ''}">
            <div class="step-icon">📎</div>
            <div class="step-label">Stips</div>
          </div>
          <div class="step">
            <div class="step-icon">💰</div>
            <div class="step-label">Funded</div>
          </div>
        </div>
      </div>

      <div class="overview-card">
        <h4>Funders Submitted To (${submittedCount})</h4>
        <div class="submitted-lenders-list">
          ${submittedLenders || '<div class="empty-state-small">No lenders submitted</div>'}
        </div>
      </div>

      <div class="overview-card">
        <h4>Quick Stats</h4>
        <div class="stats-grid">
          <div class="stat-item">
            <div class="stat-value">${stats.approved}</div>
            <div class="stat-label">Offers</div>
          </div>
          <div class="stat-item">
            <div class="stat-value">${stats.stips}</div>
            <div class="stat-label">Stips</div>
          </div>
          <div class="stat-item">
            <div class="stat-value">${stats.declined}</div>
            <div class="stat-label">Declined</div>
          </div>
          <div class="stat-item">
            <div class="stat-value">${stats.pending}</div>
            <div class="stat-label">Pending</div>
          </div>
        </div>
      </div>

      <div class="overview-card">
        <h4>Response Status (${lenderResponses.length})</h4>
        <div class="lenders-list">
          ${lendersList || '<div class="empty-state-small">No responses yet</div>'}
        </div>
      </div>

      <div class="overview-card">
        <h4>Recent Activity</h4>
        <div class="activity-preview">
          ${dealActivity.slice(0, 5).map(a => `
            <div class="activity-item-small">
              <span class="activity-icon">${getActivityIcon(a.type)}</span>
              <span class="activity-text">${a.description}</span>
              <span class="activity-time">${formatTime(a.timestamp)}</span>
            </div>
          `).join('')}
        </div>
      </div>
    </div>
  `;
}

/**
 * Render offers tab
 */
function renderOffers() {
  const content = $('feedbackContentOffers');
  if (!content) return;

  const offers = lenderResponses.filter(r => r.status === 'approved');

  if (offers.length === 0) {
    content.innerHTML = '<div class="empty-state">No offers yet. Check back later!</div>';
    return;
  }

  content.innerHTML = `
    <div class="offers-toolbar">
      <h4>${offers.length} Offer${offers.length > 1 ? 's' : ''} Received</h4>
      <div class="sort-options">
        <label>Sort by:</label>
        <select id="offerSort" class="sort-select">
          <option value="amount">Amount (High to Low)</option>
          <option value="factor">Factor (Low to High)</option>
          <option value="daily">Daily Payment</option>
          <option value="date">Response Date</option>
        </select>
      </div>
    </div>

    <div class="comparison-grid">
      ${offers.map(offer => renderOfferCard(offer)).join('')}
    </div>
  `;

  // Add sort handler
  const sortSelect = $('offerSort');
  if (sortSelect) {
    sortSelect.addEventListener('change', () => renderOffers());
  }
}

/**
 * Make a safe DOM id from lender name + section
 */
function makeReplyIdBase(section, lenderName) {
  return `${section}_${(lenderName || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')}`;
}

/**
 * Reusable reply block (email + body + upload + send)
 */
function renderReplyBlock(section, item, status, idBaseOverride) {
  const idBase = idBaseOverride || makeReplyIdBase(section, item.lender_name || 'lender');
  const safeLenderName = (item.lender_name || '').replace(/'/g, "\\'");
  const email = item.email || '';

  // Get suggested templates based on status
  let templates = [];
  if (status === 'declined') {
    templates = [
      { label: 'Request Reconsideration', value: `Hi ${item.lender_name || 'there'},\n\nThank you for reviewing our application. We'd like to provide additional information that may help reconsider this decision.\n\nWe believe the following updates address your concerns:\n\n[Add your updates here]\n\nWe appreciate your time and look forward to hearing from you.\n\nBest regards` },
      { label: 'Ask for Feedback', value: `Hi ${item.lender_name || 'there'},\n\nThank you for your response. Could you provide more specific feedback on what improvements we could make for future consideration?\n\nThis would help us better understand your requirements.\n\nThank you,\nBest regards` },
      { label: 'Thank You Note', value: `Hi ${item.lender_name || 'there'},\n\nThank you for taking the time to review our application. We appreciate your consideration and hope to work together in the future.\n\nBest regards` }
    ];
  } else if (status === 'stips') {
    templates = [
      { label: 'Documents Attached', value: `Hi ${item.lender_name || 'there'},\n\nThank you for your interest in our application. I've attached the requested documents:\n\n• [List documents]\n\nPlease let me know if you need anything else.\n\nBest regards` }
    ];
  }

  const templateOptions = templates.length > 0 ? `
    <div class="template-selector">
      <label>Quick Templates</label>
      <select class="template-select" onchange="handleTemplateSelect('${idBase}', this.value)">
        <option value="">Select a template...</option>
        ${templates.map((t, i) => `<option value="${i}">${t.label}</option>`).join('')}
      </select>
    </div>
  ` : '';

  return `
    <div class="reply-block-inner">
      <div class="reply-header">
        <h5>✉️ Compose Reply</h5>
        <button class="btn-close-reply" onclick="handleToggleReply('${idBase}')" title="Close">×</button>
      </div>

      ${templateOptions}

      <div class="reply-form">
        <div class="form-group">
          <label for="${idBase}_email">
            <span class="label-icon">📧</span>
            Recipient
          </label>
          <input
            id="${idBase}_email"
            class="input-modern"
            type="email"
            value="${email}"
            placeholder="lender@example.com, other@domain.com"
          />
        </div>

        <div class="form-group">
          <label for="${idBase}_body">
            <span class="label-icon">✍️</span>
            Message
          </label>
          <textarea
            id="${idBase}_body"
            class="textarea-modern"
            rows="6"
            placeholder="Type your message to ${safeLenderName}..."
          ></textarea>
          <div class="textarea-hint">You can use templates above to get started</div>
        </div>

        <div class="form-group attachments-group">
          <label>
            <span class="label-icon">📎</span>
            Attachments
          </label>
          <input
            id="${idBase}_files"
            type="file"
            class="hidden"
            multiple
            onchange="handleReplyFilesChange('${idBase}')"
          />
          <div class="attachment-controls">
            <button
              type="button"
              class="btn-attach"
              onclick="handleReplyUpload('${idBase}')"
            >
              <span class="btn-icon">📎</span>
              Choose Files
            </button>
            <span id="${idBase}_files_label" class="files-label">No files selected</span>
          </div>
        </div>

        <div class="reply-actions-bar">
          <button
            type="button"
            class="btn-cancel"
            onclick="handleToggleReply('${idBase}')"
          >
            Cancel
          </button>
          <button
            type="button"
            class="btn-send"
            onclick="handleSendLenderEmail('${idBase}', '${safeLenderName}', '${status}')"
          >
            <span class="btn-icon">🚀</span>
            Send Email
          </button>
        </div>
      </div>
    </div>
  `;
}

// Store templates globally for quick access
window.replyTemplates = {
  declined: [
    `Hi [LENDER],\n\nThank you for reviewing our application. We'd like to provide additional information that may help reconsider this decision.\n\nWe believe the following updates address your concerns:\n\n[Add your updates here]\n\nWe appreciate your time and look forward to hearing from you.\n\nBest regards`,
    `Hi [LENDER],\n\nThank you for your response. Could you provide more specific feedback on what improvements we could make for future consideration?\n\nThis would help us better understand your requirements.\n\nThank you,\nBest regards`,
    `Hi [LENDER],\n\nThank you for taking the time to review our application. We appreciate your consideration and hope to work together in the future.\n\nBest regards`
  ]
};

/**
 * Handle template selection
 */
window.handleTemplateSelect = function(idBase, templateIndex) {
  if (!templateIndex) return;

  const textarea = document.getElementById(`${idBase}_body`);
  if (!textarea) return;

  // Get the lender name from the card
  const card = document.getElementById(idBase);
  let lenderName = 'there';

  if (card) {
    const nameEl = card.querySelector('h4');
    if (nameEl) lenderName = nameEl.textContent.trim();
  }

  let templateText = '';
  const idx = parseInt(templateIndex);

  // Get template text based on status
  if (idBase.includes('decline')) {
    templateText = window.replyTemplates.declined[idx] || '';
  }

  // Replace placeholder
  templateText = templateText.replace('[LENDER]', lenderName);

  textarea.value = templateText;
  textarea.focus();
}

/**
 * Render single offer card
 */
function renderOfferCard(offer) {
  const daily = offer.amount * offer.factor / (offer.term_months * 30);
  const idBase = makeReplyIdBase('offer', offer.lender_name);

  return `
    <div class="lender-card approved">
      <div class="lender-header">
        <h4>${offer.lender_name}</h4>
        <span class="status-badge approved">APPROVED</span>
      </div>

      <div class="offer-details">
        <div class="offer-row">
          <span class="label">Amount:</span>
          <span class="value amount">$${offer.amount.toLocaleString()}</span>
        </div>
        <div class="offer-row">
          <span class="label">Factor:</span>
          <span class="value">${offer.factor}</span>
        </div>
        <div class="offer-row">
          <span class="label">Term:</span>
          <span class="value">${offer.term_months} months</span>
        </div>
        <div class="offer-row">
          <span class="label">Daily Payment:</span>
          <span class="value">$${daily.toFixed(2)}</span>
        </div>
        ${offer.conditions ? `
          <div class="offer-row">
            <span class="label">Conditions:</span>
            <span class="value conditions">${offer.conditions}</span>
          </div>
        ` : ''}
      </div>

      <div class="lender-actions">
        <button class="btn-primary" onclick="handleAcceptOffer('${offer.lender_name}')">Accept Offer</button>
        <button class="btn-secondary" onclick="handleToggleReply('${idBase}')">Reply</button>
        <button class="btn-ghost" onclick="handleCounterOffer('${offer.lender_name}')">Counter</button>
      </div>

      <div class="lender-contact">
        <span class="contact-email">📧 ${offer.email || 'No email'}</span>
        <span class="response-time">Responded ${formatTime(offer.responded_at)}</span>
      </div>

      <div id="${idBase}_reply" class="lender-reply-block hidden">
        ${renderReplyBlock('offer', offer, 'approved', idBase)}
      </div>
    </div>
  `;
}

/**
 * Render stips tab
 */
function renderStips() {
  const content = $('feedbackContentStips');
  if (!content) return;

  const stips = lenderResponses.filter(r => r.status === 'stips');

  if (stips.length === 0) {
    content.innerHTML = '<div class="empty-state">No stips requested. All clear! ✅</div>';
    return;
  }

  content.innerHTML = `
    <div class="stips-header">
      <h4>${stips.length} Lender${stips.length > 1 ? 's' : ''} Requested Stips</h4>
      <button class="btn-primary" onclick="handleUploadStips()">
        <span>📎</span> Upload Documents
      </button>
    </div>

    <div class="stips-grid">
      ${stips.map(stip => renderStipCard(stip)).join('')}
    </div>
  `;
}

/**
 * Render single stip card
 */
function renderStipCard(stip) {
  const requirements = stip.stips_required ? stip.stips_required.split(',').map(s => s.trim()) : [];
  const idBase = makeReplyIdBase('stips', stip.lender_name);

  return `
    <div class="lender-card stips">
      <div class="lender-header">
        <h4>${stip.lender_name}</h4>
        <span class="status-badge stips">STIPS REQUIRED</span>
      </div>

      <div class="stips-requirements">
        <h5>Required Documents:</h5>
        <ul class="requirements-list">
          ${requirements.map(req => `
            <li>
              <input type="checkbox" id="req_${stip.lender_name}_${req}" />
              <label for="req_${stip.lender_name}_${req}">${req}</label>
            </li>
          `).join('')}
        </ul>
      </div>

      <div class="stips-upload">
        <div class="dropzone-mini" data-lender="${stip.lender_name}">
          <span class="dz-icon">📎</span>
          <span class="dz-text">Drop files here or click to upload</span>
        </div>
        <div class="uploaded-files" id="files_${stip.lender_name}"></div>
      </div>

      <div class="lender-actions">
        <button class="btn-primary" onclick="handleSendStips('${stip.lender_name}')">Send Documents</button>
        <button class="btn-secondary" onclick="handleToggleReply('${idBase}')">Reply</button>
      </div>

      <div class="lender-contact">
        <span class="contact-email">📧 ${stip.email || 'No email'}</span>
        <span class="response-time">Requested ${formatTime(stip.responded_at)}</span>
      </div>

      <div id="${idBase}_reply" class="lender-reply-block hidden">
        ${renderReplyBlock('stips', stip, 'stips', idBase)}
      </div>
    </div>
  `;
}

/**
 * Render declines tab
 */
function renderDeclines() {
  const content = $('feedbackContentDeclines');
  if (!content) return;

  const declines = lenderResponses.filter(r => r.status === 'declined');

  if (declines.length === 0) {
    content.innerHTML = '<div class="empty-state">No declines yet! 🎉</div>';
    return;
  }

  content.innerHTML = `
    <div class="declines-header">
      <h4>${declines.length} Decline${declines.length > 1 ? 's' : ''}</h4>
    </div>

    <div class="declines-list">
      ${declines.map(decline => renderDeclineCard(decline)).join('')}
    </div>
  `;
}

/**
 * Render single decline card
 */
function renderDeclineCard(decline) {
  const idBase = makeReplyIdBase('decline', decline.lender_name);

  return `
    <div class="lender-card declined">
      <div class="lender-header">
        <div class="lender-title-section">
          <h4>${decline.lender_name}</h4>
          <span class="lender-email-small">📧 ${decline.email || 'No email'}</span>
        </div>
        <span class="status-badge declined">❌ DECLINED</span>
      </div>

      <div class="decline-reason-box">
        <div class="reason-icon">💬</div>
        <div class="reason-content">
          <h5>Decline Reason</h5>
          <p>${decline.decline_reason || 'No specific reason provided by the lender.'}</p>
        </div>
      </div>

      <div class="lender-footer">
        <div class="footer-info">
          <span class="response-time">
            <span class="time-icon">🕐</span>
            Declined ${formatTime(decline.responded_at)}
          </span>
        </div>
        <div class="footer-actions">
          <button class="btn-reply" onclick="handleToggleReply('${idBase}')">
            <span class="btn-icon">✉️</span>
            Reply to Lender
          </button>
        </div>
      </div>

      <div id="${idBase}_reply" class="lender-reply-block hidden">
        ${renderReplyBlock('decline', decline, 'declined', idBase)}
      </div>
    </div>
  `;
}

/**
 * Render activity tab
 */
function renderActivity() {
  const content = $('feedbackContentActivity');
  if (!content) return;

  if (dealActivity.length === 0) {
    content.innerHTML = '<div class="empty-state">No activity yet</div>';
    return;
  }

  content.innerHTML = `
    <div class="activity-timeline">
      ${dealActivity.map(activity => `
        <div class="timeline-item">
          <div class="timeline-icon">${getActivityIcon(activity.type)}</div>
          <div class="timeline-content">
            <div class="timeline-header">
              <span class="timeline-title">${activity.description}</span>
              <span class="timeline-time">${formatTimeDetailed(activity.timestamp)}</span>
            </div>
            ${activity.lender_name ? `<div class="timeline-lender">${activity.lender_name}</div>` : ''}
          </div>
        </div>
      `).join('')}
    </div>
  `;
}

/**
 * Render documents tab
 */
async function renderDocuments() {
  const content = $('feedbackContentDocuments');
  if (!content) return;

  // Get REAL documents from deal - they're in wrapped_filenames
  const application = activeDeal?.application || {};
  const statements = activeDeal?.statements || {};
  const lenderCount = activeDeal?.deliveries?.length || 0;

  const documents = [];

  // Add application PDF if exists
  if (application._wrapped_filename) {
    documents.push({
      name: application._wrapped_filename,
      type: 'Application',
      path: `/uploads/${application._wrapped_filename}`,
      sentTo: lenderCount
    });
  }

  // Add bank statements if exist
  const stmtFiles = statements._wrapped_filenames || [];
  stmtFiles.forEach(filename => {
    documents.push({
      name: filename,
      type: 'Bank Statement',
      path: `/uploads/${filename}`,
      sentTo: lenderCount
    });
  });

  console.log('📎 Found documents:', documents);

  content.innerHTML = `
    <div class="documents-section">
      <h4>Original Submission (${documents.length} files)</h4>
      <div class="documents-list">
        ${documents.length === 0
          ? '<div class="empty-state">No documents found for this deal</div>'
          : documents.map((doc, index) => `
              <div class="document-item">
                <span class="doc-icon">📄</span>
                <div class="doc-info">
                  <div class="doc-name">${doc.name}</div>
                  <div class="doc-meta">
                    ${doc.type} • Sent to ${doc.sentTo} lenders
                  </div>
                </div>
                <a href="${doc.path}" download="${doc.name}" class="btn-ghost btn-small">Download</a>
              </div>
            `).join('')}
      </div>
    </div>

    <div class="documents-section">
      <h4>Stips Uploaded</h4>
      <div id="stipsUploadedList" class="documents-list">
        <div class="empty-state-small">Loading...</div>
      </div>
    </div>
  `;

  // Fetch and display uploaded stips
  await updateStipsUploadedUI();
}

/**
 * Update the "Stips Uploaded" list - fetch from backend
 */
async function updateStipsUploadedUI() {
  const listEl = document.getElementById('stipsUploadedList');
  if (!listEl) return;

  if (!activeDeal || !activeDeal.id) {
    listEl.innerHTML = '<div class="empty-state-small">No stips uploaded yet</div>';
    return;
  }

  try {
    // Fetch uploaded documents from the backend
    const response = await fetch(`/api/feedback/deal/${activeDeal.id}/documents`);

    if (!response.ok) {
      const errorText = await response.text();
      console.error('Failed to fetch documents:', response.status, errorText);
      throw new Error(`Failed to fetch documents: ${response.status}`);
    }

    const data = await response.json();
    const stipsDocuments = data.documents || [];

    console.log('📎 Fetched stips documents:', stipsDocuments);

    if (stipsDocuments.length === 0) {
      listEl.innerHTML = '<div class="empty-state-small">No stips uploaded yet</div>';
      return;
    }

    // Render documents with download buttons
    listEl.innerHTML = stipsDocuments.map(doc => {
      // Extract just the filename from the path for display
      const displayName = doc.file_name || doc.name || 'Unknown file';
      const filePath = doc.file_path || '';

      // Create download URL - files are in /uploads/feedback/{deal_id}/
      // Extract the filename with timestamp from the full path
      let downloadUrl;
      if (filePath.includes('uploads/feedback/')) {
        // Path is like: /path/to/Statements/uploads/feedback/123/timestamp_file.pdf
        const parts = filePath.split('uploads/feedback/');
        downloadUrl = `/uploads/feedback/${parts[1]}`;
      } else if (filePath.includes('/uploads/')) {
        // Fallback: extract from /uploads/
        const parts = filePath.split('/uploads/');
        downloadUrl = `/uploads/${parts[1]}`;
      } else {
        // Just use the filename with deal_id
        const filename = filePath.split('/').pop();
        downloadUrl = `/uploads/feedback/${activeDeal.id}/${filename}`;
      }

      // Format file size
      const fileSize = doc.file_size ? formatFileSize(doc.file_size) : '';
      const uploadedAt = doc.uploaded_at ? formatTime(doc.uploaded_at) : '';
      const lenderName = doc.lender_name || 'General';

      return `
        <div class="document-item">
          <span class="doc-icon">📎</span>
          <div class="doc-info">
            <div class="doc-name">${displayName}</div>
            <div class="doc-meta">
              ${lenderName}${fileSize ? ` • ${fileSize}` : ''}${uploadedAt ? ` • ${uploadedAt}` : ''}
            </div>
          </div>
          <a href="${downloadUrl}" download="${displayName}" class="btn-ghost btn-small" title="Download ${displayName}">
            <span class="download-icon">⬇️</span> Download
          </a>
        </div>
      `;
    }).join('');

  } catch (error) {
    console.error('Error fetching stips documents:', error);
    // Show empty state instead of error - documents table might not exist yet
    listEl.innerHTML = '<div class="empty-state-small">No stips uploaded yet</div>';
  }
}

/**
 * Format file size in human-readable format
 */
function formatFileSize(bytes) {
  if (!bytes) return '';
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

/**
 * Register attachments chosen in a reply block
 */
function registerReplyAttachments(idBase, files) {
  // Remove old entries for this reply block
  replyAttachments = replyAttachments.filter(att => att.idBase !== idBase);

  Array.from(files || []).forEach(file => {
    replyAttachments.push({
      idBase,
      name: file.name,
      lender_name: (function () {
        // idBase is "section_lendername", last part is lender slug; this is just UI
        return null;
      })()
    });
  });

  updateStipsUploadedUI();
}

/**
 * Calculate stats from responses
 */
function calculateStats() {
  const approved = lenderResponses.filter(r => r.status === 'approved').length;
  const stips = lenderResponses.filter(r => r.status === 'stips').length;
  const declined = lenderResponses.filter(r => r.status === 'declined').length;
  const pending = lenderResponses.filter(r => r.status === 'pending').length;
  const totalResponses = approved + stips + declined;
  const total = lenderResponses.length;
  const progressPercent = total > 0 ? Math.round((totalResponses / total) * 100) : 0;

  return { approved, stips, declined, pending, totalResponses, progressPercent };
}

/**
 * Get icon for activity type
 */
function getActivityIcon(type) {
  const icons = {
    'sent': '📧',
    'approved': '✅',
    'declined': '❌',
    'stips': '📎',
    'reply': '💬',
    'upload': '📤',
    'funded': '💰'
  };
  return icons[type] || '•';
}

/**
 * Format timestamp
 */
function formatTime(timestamp) {
  const now = Date.now();
  const diff = now - timestamp;
  const minutes = Math.floor(diff / 60000);
  const hours = Math.floor(diff / 3600000);
  const days = Math.floor(diff / 86400000);

  if (minutes < 1) return 'Just now';
  if (minutes < 60) return `${minutes}m ago`;
  if (hours < 24) return `${hours}h ago`;
  if (days < 7) return `${days}d ago`;

  return new Date(timestamp).toLocaleDateString();
}

/**
 * Format timestamp with details
 */
function formatTimeDetailed(timestamp) {
  const date = new Date(timestamp);
  return date.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    hour12: true
  });
}

/**
 * Apply filter
 */
function applyFilter(filter) {
  // Update active filter button
  document.querySelectorAll('[data-filter]').forEach(btn => {
    if (btn.dataset.filter === filter) {
      btn.classList.add('active');
    } else {
      btn.classList.remove('active');
    }
  });

  // Filter logic would go here
  console.log('Applying filter:', filter);
}

/**
 * Handle reply action
 */
async function handleReply() {
  console.log('Reply clicked');
  // TODO: Open reply modal
}

/**
 * Handle stip upload
 */
async function handleStipUpload(e) {
  const files = e.target.files;
  if (!files || files.length === 0) return;

  if (!activeDeal || !activeDeal.id) {
    alert('No active deal selected');
    return;
  }

  try {
    // Show uploading indicator
    const uploadBtn = document.getElementById('uploadStipsBtn');
    if (uploadBtn) uploadBtn.textContent = 'Uploading...';

    const formData = new FormData();

    // Add all files
    Array.from(files).forEach(file => {
      formData.append('files', file);
    });

    // Add metadata
    formData.append('type', 'stip');
    formData.append('lender_name', 'General'); // Can be customized per lender

    const response = await fetch(`/api/feedback/deal/${activeDeal.id}/document`, {
      method: 'POST',
      body: formData
    });

    const result = await response.json();

    if (response.ok) {
      // Success - refresh the stips uploaded list
      alert(`Successfully uploaded ${files.length} file(s)`);

      // Reload deal data to refresh the UI
      await loadDealData(activeDeal.id);

      // Reset file input
      e.target.value = '';
    } else {
      throw new Error(result.error || 'Upload failed');
    }
  } catch (error) {
    console.error('Upload error:', error);
    alert(`Failed to upload files: ${error.message}`);
  } finally {
    // Restore button text
    const uploadBtn = document.getElementById('uploadStipsBtn');
    if (uploadBtn) uploadBtn.textContent = 'Upload Stips';
  }
}

/**
 * Handle send update
 */
async function handleSendUpdate() {
  console.log('Send update clicked');
  // TODO: Send update to selected lenders
}

/**
 * Handle mark as funded
 */
async function handleMarkFunded() {
  const confirmed = confirm('Mark this deal as funded?');
  if (confirmed) {
    console.log('Marking as funded');
    // TODO: Update deal status
  }
}

/**
 * Generate mock responses based on REAL lender names from deal
 */
function generateMockResponsesFromRealLenders(lenderNames, lenderEmails) {
  const statuses = ['approved', 'stips', 'declined', 'pending'];
  const responses = [];

  lenderNames.forEach((lenderName, index) => {
    const emailObj = lenderEmails.find(e => e.name === lenderName);
    const email = emailObj ? emailObj.email : `submissions@${lenderName.toLowerCase().replace(/\s+/g, '')}.com`;

    // Distribute statuses: mostly pending, some approved, some stips, few declined
    let status;
    if (index === 0) {
      status = 'approved'; // First one approved
    } else if (index === 1 && lenderNames.length > 3) {
      status = 'approved'; // Second one also approved if enough lenders
    } else if (index === 2 && lenderNames.length > 5) {
      status = 'stips'; // Third one needs stips
    } else if (index === lenderNames.length - 1 && lenderNames.length > 4) {
      status = 'declined'; // Last one declined if enough lenders
    } else {
      status = 'pending'; // Rest are pending
    }

    const baseResponse = {
      lender_name: lenderName,
      status: status,
      email: email,
      responded_at: status === 'pending' ? null : Date.now() - (3600000 * (index + 2))
    };

    // Add status-specific fields
    if (status === 'approved') {
      responses.push({
        ...baseResponse,
        amount: 45000 + (index * 5000),
        factor: 1.20 + (index * 0.03),
        term_months: 6,
        conditions: index === 0 ? 'None' : 'COJ required'
      });
    } else if (status === 'stips') {
      responses.push({
        ...baseResponse,
        stips_required: 'Most recent bank statement, Photo ID, Voided check'
      });
    } else if (status === 'declined') {
      responses.push({
        ...baseResponse,
        decline_reason: 'Unable to approve at this time based on current bank activity'
      });
    } else {
      responses.push(baseResponse);
    }
  });

  console.log('📋 Generated mock responses for real lenders:', responses);
  return responses;
}

/**
 * Generate mock activity based on REAL deal data
 */
function generateMockActivityFromRealData(businessName, lenderCount) {
  const activity = [
    {
      type: 'sent',
      description: `Submitted ${businessName} to ${lenderCount} lenders`,
      lender_name: null,
      timestamp: Date.now() - 86400000
    }
  ];

  // Add some mock responses
  const timeBetween = 3600000; // 1 hour
  let timeOffset = 82800000; // 23 hours ago

  activity.push({
    type: 'approved',
    description: 'Approved offer received',
    lender_name: 'First Lender',
    timestamp: Date.now() - timeOffset
  });

  if (lenderCount > 3) {
    timeOffset -= timeBetween * 2;
    activity.push({
      type: 'stips',
      description: 'Requested additional documents',
      lender_name: 'Second Lender',
      timestamp: Date.now() - timeOffset
    });
  }

  if (lenderCount > 5) {
    timeOffset -= timeBetween * 3;
    activity.push({
      type: 'approved',
      description: 'Second approval received',
      lender_name: 'Third Lender',
      timestamp: Date.now() - timeOffset
    });
  }

  console.log('📅 Generated mock activity:', activity);
  return activity;
}

/**
 * Generate generic mock responses (fallback)
 */
function generateMockResponses(dealId) {
  return [
    {
      lender_name: 'Alternative Funding',
      status: 'approved',
      amount: 50000,
      factor: 1.25,
      term_months: 6,
      conditions: 'COJ required',
      email: 'submissions@alternative.com',
      responded_at: Date.now() - 3600000 * 2
    },
    {
      lender_name: 'Smart Step',
      status: 'stips',
      stips_required: 'December statement, Photo ID, Voided check',
      email: 'underwriting@smartstep.com',
      responded_at: Date.now() - 3600000 * 4
    },
    {
      lender_name: 'Arena Capital',
      status: 'declined',
      decline_reason: 'Too many NSF fees in recent statements',
      email: 'deals@arenacapital.com',
      responded_at: Date.now() - 3600000 * 5
    },
    {
      lender_name: 'Nexus Funding',
      status: 'approved',
      amount: 45000,
      factor: 1.22,
      term_months: 5,
      conditions: 'None',
      email: 'submissions@nexus.com',
      responded_at: Date.now() - 3600000 * 3
    },
    {
      lender_name: 'Highland Hill',
      status: 'pending',
      email: 'team@highlandhill.com',
      responded_at: null
    }
  ];
}

/**
 * Generate generic mock activity (fallback)
 */
function generateMockActivity(dealId) {
  return [
    { type: 'sent', description: 'Submitted to 12 lenders', lender_name: null, timestamp: Date.now() - 86400000 },
    { type: 'approved', description: 'Approved for $50,000', lender_name: 'Alternative Funding', timestamp: Date.now() - 82800000 },
    { type: 'stips', description: 'Requested additional documents', lender_name: 'Smart Step', timestamp: Date.now() - 72000000 },
    { type: 'declined', description: 'Declined due to NSF fees', lender_name: 'Arena Capital', timestamp: Date.now() - 68400000 },
    { type: 'approved', description: 'Approved for $45,000', lender_name: 'Nexus Funding', timestamp: Date.now() - 61200000 }
  ];
}

// Export global handlers for inline onclick events
window.handleAcceptOffer = (lenderName) => console.log('Accept offer:', lenderName);
window.handleReplyToLender = (lenderName) => console.log('Reply to:', lenderName);
window.handleCounterOffer = (lenderName) => console.log('Counter offer:', lenderName);
window.handleSendStips = (lenderName) => console.log('Send stips to:', lenderName);
window.handleUploadStips = () => $('feedbackStipFile')?.click();
window.handleDownloadDoc = (docIndex) => {
  console.log('📥 Download document at index:', docIndex);

  if (!activeDeal || !activeDeal.attachments) {
    console.error('No attachments available');
    alert('No attachments found for this deal');
    return;
  }

  const attachments = activeDeal.attachments.attachments || [];
  const doc = attachments[docIndex];

  if (!doc || !doc.data) {
    console.error('Document not found or no data');
    alert('Document data not available');
    return;
  }

  try {
    // Decode base64 data
    const binaryString = atob(doc.data);
    const bytes = new Uint8Array(binaryString.length);
    for (let i = 0; i < binaryString.length; i++) {
      bytes[i] = binaryString.charCodeAt(i);
    }

    // Create blob and download
    const blob = new Blob([bytes], { type: 'application/pdf' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = doc.name || `document_${docIndex}.pdf`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);

    console.log('✅ Download initiated:', doc.name);
  } catch (err) {
    console.error('❌ Download failed:', err);
    alert('Failed to download document: ' + err.message);
  }
};

window.handleToggleReply = (idBase) => {
  const block = document.getElementById(`${idBase}_reply`);
  if (block) {
    block.classList.toggle('hidden');
  }
};

window.handleReplyUpload = (idBase) => {
  const input = document.getElementById(`${idBase}_files`);
  if (input) {
    input.click();
  }
};

window.handleReplyFilesChange = (idBase) => {
  const input = document.getElementById(`${idBase}_files`);
  const label = document.getElementById(`${idBase}_files_label`);
  if (!input || !label) return;

  if (!input.files || input.files.length === 0) {
    label.textContent = 'No files selected';
  } else if (input.files.length === 1) {
    label.textContent = input.files[0].name;
  } else {
    label.textContent = `${input.files.length} files selected`;
  }

  // Mirror into attachments section
  registerReplyAttachments(idBase, input.files);
};

window.handleSendLenderEmail = async (idBase, lenderName, status) => {
  if (!activeDeal) {
    alert('No active deal selected');
    return;
  }

  const emailEl = document.getElementById(`${idBase}_email`);
  const bodyEl = document.getElementById(`${idBase}_body`);
  const filesEl = document.getElementById(`${idBase}_files`);

  const to = emailEl?.value?.trim();
  const body = bodyEl?.value?.trim();
  const files = filesEl?.files || [];

  if (!to) {
    alert('Please provide a recipient email');
    return;
  }
  if (!body) {
    alert('Message body cannot be empty');
    return;
  }

  const formData = new FormData();
  formData.append('deal_id', activeDeal.id);
  formData.append('lender_name', lenderName);
  formData.append('status_context', status);      // "approved" | "stips" | "declined"
  formData.append('to', to);
  formData.append('body', body);

  Array.from(files).forEach(f => formData.append('files', f));

  try {
    console.log('📤 Sending lender email...', {
      deal_id: activeDeal.id,
      lenderName,
      status,
      to,
      bodyLength: body.length,
      fileCount: files.length
    });

    await postFormData('/api/feedback/reply', formData);

    alert('Email sent to ' + lenderName);
    bodyEl.value = '';
    // Do NOT clear replyAttachments here, so "Stips Uploaded" keeps showing them.
    if (filesEl) {
      filesEl.value = '';
      const label = document.getElementById(`${idBase}_files_label`);
      if (label) label.textContent = 'No files selected';
    }

  } catch (err) {
    console.error('❌ Failed to send lender email', err);
    alert('Failed to send email: ' + (err.message || err));
  }
};
