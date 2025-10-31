/**
 * Main application orchestrator
 */

import { requireAuth, logout } from './auth.js';
import { MODES } from './config.js';
import { $, show, hide } from './utils.js';
import { clearResults } from './components/lender-tiles.js';
import { refreshGmailStatus, initGmailListeners } from './services/gmail.js';
import { initFileUploads } from './components/file-upload.js';
import { initComposer } from './components/composer.js';
import { loadDeals, initDashboard } from './components/dashboard.js';
import { loadLeads, installLeadsExitPatches } from './components/leads.js';
import { initMCA } from './modules/mca.js';
import { activateCCS } from './modules/ccs.js';
import { activateReverse } from './modules/reverse.js';
import { loadOpportunities, installOpportunitiesExitPatches } from './modules/opportunities.js';
import { initFeedbackHub } from './components/feedback-hub.js';

let CURRENT_MODE = MODES.MCA;

export function setMode(mode) {
  CURRENT_MODE = mode;
  clearResults();
  hide($("dashboard"));

  if (mode === MODES.MCA) {
    $("resultsTitle").textContent = "Suggested Lenders";
    $("stmtTitle").textContent = "Statements & Matrix";
    $("stmtLabel").textContent = "Bank Statements (PDFs)";
    $("stmtHint").textContent = "Upload 3 months. If the state is NY/CA, upload 4 months.";
    $("btnStmtsMatch").style.display = "";
    $("posBadge").style.display = "";
    hide($("stmtAnalysis"));
    show($("cardApp"));
    show($("cardStatements"));
    hide($("results"));
    hide($("composer"));
    hide($("opportunities"));
    hide($("leads"));
  } else if (mode === MODES.CCS) {
    hide($("opportunities"));
    hide($("leads"));
    activateCCS();
  } else if (mode === MODES.REV) {
    hide($("opportunities"));
    hide($("leads"));
    activateReverse();
  } else if (mode === MODES.DASH) {
    hide($("cardApp"));
    hide($("cardStatements"));
    hide($("stmtAnalysis"));
    hide($("results"));
    hide($("composer"));
    hide($("opportunities"));
    hide($("leads"));
    show($("dashboard"));
    loadDeals();
  } else if (mode === MODES.LEADS) {
    hide($("cardApp"));
    hide($("cardStatements"));
    hide($("stmtAnalysis"));
    hide($("results"));
    hide($("composer"));
    hide($("opportunities"));
    show($("leads"));
    installLeadsExitPatches();
    loadLeads();
  } else if (mode === MODES.OPP) {
    hide($("cardApp"));
    hide($("cardStatements"));
    hide($("stmtAnalysis"));
    hide($("results"));
    hide($("composer"));
    hide($("leads"));
    show($("opportunities"));
    installOpportunitiesExitPatches();
    loadOpportunities();
  }
}

function initModeButtons() {
  $("btnMCA")?.addEventListener("click", (e) => {
    e.preventDefault();
    setMode(MODES.MCA);
  });
  $("btnCCS")?.addEventListener("click", () => setMode(MODES.CCS));
  $("btnReverse")?.addEventListener("click", () => setMode(MODES.REV));
  $("btnDashboard")?.addEventListener("click", (e) => {
    e.preventDefault?.();
    setMode(MODES.DASH);
  });
  $("btnOpportunities")?.addEventListener("click", () => setMode(MODES.OPP));
  $("btnLeads")?.addEventListener("click", () => setMode(MODES.LEADS));
}

function initLogoutButton() {
  $("btnLogout")?.addEventListener("click", logout);
}

export async function init() {
  // Check authentication first
  const authenticated = await requireAuth();
  if (!authenticated) return;

  // Initialize all components
  await refreshGmailStatus();
  initGmailListeners();
  initFileUploads();
  initComposer();
  initDashboard();
  initFeedbackHub();
  initMCA();
  initModeButtons();
  initLogoutButton();

  // Check if we should go to dashboard after send
  const afterSend = sessionStorage.getItem("afterSendGoTo");
  if (afterSend === "DASH") {
    sessionStorage.removeItem("afterSendGoTo");
    setMode(MODES.DASH);
  }
}

// Auto-initialize on DOM ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}
