/**
 * Leads component
 * Placeholder for leads functionality - to be expanded
 */

import { $, hide } from '../utils.js';
import { API_BASE } from '../config.js';

export function installLeadsExitPatches() {
  const L = $("leads");
  if (!L) return;
  const hideLeads = () => {
    hide(L);
    L.innerHTML = "";
  };

  ["btnMCA", "btnCCS", "btnReverse", "btnDashboard"].forEach(id => {
    const el = $(id);
    if (el && !el._leadsExitAttached) {
      el.addEventListener("click", hideLeads, { capture: true });
      el._leadsExitAttached = true;
    }
  });
}

export async function loadLeads() {
  const el = $("leads");
  if (!el) return;

  el.innerHTML = "Loading leads...";
  try {
    const r = await fetch(`${API_BASE}/api/underwrite/leads?limit=100`, { credentials: "include" });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    // Render leads (simplified)
    el.innerHTML = `<h2>Leads</h2><p>${(data.leads || []).length} leads found</p>`;
  } catch (err) {
    console.error(err);
    el.innerHTML = `<div class="note-err">Failed to load leads.</div>`;
  }
}
