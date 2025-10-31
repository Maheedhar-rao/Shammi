/**
 * Dashboard component with pagination and filtering
 */

import { $, show, hide } from '../utils.js';
import { API_BASE } from '../config.js';
import { openFeedbackHub } from './feedback-hub.js';

let DEALS = [];
let FILTERED = [];
let PAGE = 1;
let PAGE_SIZE = 25;
let _searchTimer = null;
let _pagerInitialized = false;

function normalize(s) {
  return (s ?? "").toString().toLowerCase();
}

function applyFilterAndRender() {
  const tbody = $("dealsTbody");
  const pageInfoEl = $("pageInfo");
  if (!tbody || !pageInfoEl) return;

  const q = normalize(($("dealSearch") || {}).value);
  FILTERED = !q ? [...DEALS] : DEALS.filter(d => {
    const id = String(d.id ?? "");
    const mode = normalize(d.mode);
    const biz = normalize(d.business_name || d.subject || "");
    const from = normalize(d.sender_email || d.from || "");
    return [id, mode, biz, from].join(" ").includes(q);
  });

  const total = FILTERED.length;
  const pages = Math.max(1, Math.ceil(total / PAGE_SIZE));
  if (PAGE > pages) PAGE = pages;

  const start = (PAGE - 1) * PAGE_SIZE;
  const end = Math.min(start + PAGE_SIZE, total);
  const slice = FILTERED.slice(start, end);

  renderDealsTable(slice);

  pageInfoEl.textContent = `${total ? (start + 1) : 0}–${end} of ${total} • Page ${PAGE}/${pages}`;
  if ($("pagePrev")) $("pagePrev").disabled = PAGE <= 1;
  if ($("pageNext")) $("pageNext").disabled = PAGE >= pages;
}

function setPageSize(n) {
  PAGE_SIZE = Number(n) || 25;
  PAGE = 1;
  applyFilterAndRender();
}

function initDashboardPager() {
  if (_pagerInitialized) return;
  _pagerInitialized = true;

  const searchEl = $("dealSearch");
  const prevEl = $("pagePrev");
  const nextEl = $("pageNext");
  const sizeEl = $("pageSize");

  if (searchEl) {
    searchEl.addEventListener("input", () => {
      clearTimeout(_searchTimer);
      _searchTimer = setTimeout(() => {
        PAGE = 1;
        applyFilterAndRender();
      }, 150);
    });
  }
  if (prevEl) prevEl.addEventListener("click", () => {
    if (PAGE > 1) {
      PAGE--;
      applyFilterAndRender();
    }
  });
  if (nextEl) nextEl.addEventListener("click", () => {
    const pages = Math.max(1, Math.ceil(FILTERED.length / PAGE_SIZE));
    if (PAGE < pages) {
      PAGE++;
      applyFilterAndRender();
    }
  });
  if (sizeEl) sizeEl.addEventListener("change", (e) => setPageSize(e.target.value));
}

function tsToLocal(ts) {
  if (!ts) return "—";
  const d = new Date(typeof ts === "number" ? (ts > 1e12 ? ts : ts * 1000) : ts);
  return isNaN(d) ? "" : d.toLocaleString();
}

function renderDealsTable(deals) {
  const tbody = $("dealsTbody");
  if (!tbody) return;

  tbody.innerHTML = "";

  if (!deals || deals.length === 0) {
    tbody.innerHTML = '<tr><td colspan="6" class="muted">No deals to display</td></tr>';
    return;
  }

  deals.forEach(d => {
    const tr = document.createElement("tr");
    tr.dataset.dealId = d.id || "";
    tr.innerHTML = `
      <td>${d.id || "—"}</td>
      <td>${d.mode || "—"}</td>
      <td>${d.business_name || d.subject || "—"}</td>
      <td>${d.sender_email || d.from || "—"}</td>
      <td>${d.sent_count || 0}/${d.error_count || 0}/${d.skipped_count || 0}/${d.total_count || 0}</td>
      <td>${tsToLocal(d.created_at || d.timestamp)}</td>
    `;

    tr.addEventListener('click', () => {
      console.log('🖱️ Deal row clicked, ID:', d.id);
      openFeedbackHub(d.id);
    });
    tr.style.cursor = 'pointer';

    tbody.appendChild(tr);
  });
}

export async function loadDeals() {
  const tbody = $("dealsTbody");
  const empty = $("dealsEmpty");
  const summary = $("dashSummary");

  if (tbody) tbody.innerHTML = `<tr><td colspan="6" class="muted">Loading…</td></tr>`;
  if (empty) hide(empty);

  try {
    const r = await fetch(`${API_BASE}/api/underwrite/deals`);
    const data = await r.json();
    DEALS = Array.isArray(data.deals) ? data.deals : [];

    if (summary) {
      const total = DEALS.length;
      const sent = DEALS.reduce((a, d) => a + (Number(d.sent_count ?? 0)), 0);
      const err = DEALS.reduce((a, d) => a + (Number(d.error_count ?? 0)), 0);
      const skip = DEALS.reduce((a, d) => a + (Number(d.skipped_count ?? 0)), 0);
      summary.textContent = `${total} deal${total === 1 ? "" : "s"} • sent ${sent}, errors ${err}, skipped ${skip}`;
    }

    PAGE = 1;
    initDashboardPager();
    applyFilterAndRender();

    if (!DEALS.length && empty) show(empty);
  } catch (e) {
    if (tbody) tbody.innerHTML = "";
    if (empty) show(empty);
    if (summary) summary.textContent = "Failed to load deals.";
  }
}

export function initDashboard() {
  $("dashRefresh")?.addEventListener("click", loadDeals);
}
