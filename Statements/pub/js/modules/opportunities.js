/**
 * Opportunities module
 * Placeholder for opportunities functionality
 */

import { $, show, hide } from '../utils.js';
import { API_BASE } from '../config.js';

export function installOpportunitiesExitPatches() {
  const O = $("opportunities");
  if (!O) return;
  const hideOpp = () => {
    hide(O);
    O.innerHTML = "";
  };

  ["btnMCA", "btnCCS", "btnReverse", "btnDashboard", "btnLeads"].forEach(id => {
    const el = $(id);
    if (el && !el._oppExitAttached) {
      el.addEventListener("click", hideOpp, { capture: true });
      el._oppExitAttached = true;
    }
  });
}

export async function loadOpportunities() {
  const el = $("opportunitiesContent");
  if (!el) return;

  el.innerHTML = "Loading opportunities...";
  try {
    const r = await fetch(`${API_BASE}/api/underwrite/leads?limit=100`, { credentials: "include" });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    // Simplified rendering
    el.innerHTML = `<p>${(data.leads || []).length} opportunities found</p>`;
  } catch (err) {
    console.error(err);
    el.innerHTML = `<div class="note-err">Failed to load opportunities.</div>`;
  }
}
