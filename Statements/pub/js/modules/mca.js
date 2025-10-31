/**
 * MCA (Merchant Cash Advance) module
 */

import { $, show, hide, money, fmt, ordinal } from '../utils.js';
import { extractApplication, analyzeStatementsAndMatch } from '../services/underwrite-api.js';
import { renderLenders } from '../components/lender-tiles.js';
import { setComposerData } from '../components/composer.js';

export let applicationData = null;
export let lastStatements = null;
export let CURRENT_POSITION = null;
export let RESEND_CTX = null;

export function openPositionModal(onConfirm, onCancel) {
  const overlay = $("positionModal");
  const input = $("posInput");
  const err = $("posError");
  const ok = $("posConfirm");
  const cancel = $("posCancel");

  err.textContent = "";
  input.value = CURRENT_POSITION != null ? String(CURRENT_POSITION) : "";
  overlay.style.display = "flex";
  overlay.setAttribute("aria-hidden", "false");
  setTimeout(() => input.focus(), 0);

  function cleanup() {
    overlay.style.display = "none";
    overlay.setAttribute("aria-hidden", "true");
    ok.removeEventListener("click", handleOk);
    cancel.removeEventListener("click", handleCancel);
    overlay.removeEventListener("click", onBackdrop);
    document.removeEventListener("keydown", onEsc);
  }

  function parse(v) {
    const m = String(v || "").match(/\d+/);
    if (!m) return null;
    const n = parseInt(m[0], 10);
    return (n >= 1 && n <= 10) ? n : null;
  }

  function handleOk(e) {
    e.preventDefault();
    const n = parse(input.value);
    if (n == null) {
      err.textContent = "Enter 1-10.";
      input.focus();
      return;
    }
    cleanup();
    onConfirm?.(n);
  }

  function handleCancel(e) {
    e.preventDefault();
    cleanup();
    onCancel?.();
  }

  function onBackdrop(e) {
    if (e.target === overlay) handleCancel(e);
  }

  function onEsc(e) {
    if (e.key === "Escape") handleCancel(e);
    if (e.key === "Enter") handleOk(e);
  }

  ok.addEventListener("click", handleOk);
  cancel.addEventListener("click", handleCancel);
  overlay.addEventListener("click", onBackdrop);
  document.addEventListener("keydown", onEsc);
}

export function ensurePosition() {
  if (CURRENT_POSITION != null) return Promise.resolve(CURRENT_POSITION);
  return new Promise((res, rej) => openPositionModal(n => {
    CURRENT_POSITION = n;
    res(n);
  }, () => rej(new Error("Position required"))));
}

export function renderKV(container, data) {
  container.innerHTML = "";
  if (!data || typeof data !== "object") return;
  [
    ["business_name", "Business Name"],
    ["state", "State"],
    ["industry", "Industry"],
    ["fico", "FICO"],
    ["length_of_ownership", "Length of Ownership"],
    ["length_months", "TIB (months)"]
  ].forEach(([k, label]) => {
    if (data[k] === undefined || data[k] === null || String(data[k]).trim() === "") return;
    const box = document.createElement("div");
    box.className = "kv";
    box.innerHTML = `<div class="k">${label}</div><div class="v">${(typeof data[k] === "object") ? JSON.stringify(data[k]) : String(data[k])}</div>`;
    container.appendChild(box);
  });
}

export async function handleExtractApplication() {
  const application = $("application").files[0];
  if (!application) return alert("Upload the application PDF.");

  $("btnAppExtract").disabled = true;
  $("appStatus").textContent = "Extracting...";

  try {
    const data = await extractApplication(application);
    applicationData = data.application || {};
    renderKV($("kvContainer"), applicationData);

    const bn = applicationData.business_name || "";
    $("subject").value = `New Submission - Harvest Lending/Pathway Catalyst - ${bn} - #DealID`;

    const stateGuess = (applicationData.state || "").toString().trim().toUpperCase();
    if (stateGuess && stateGuess.length === 2) $("state").value = stateGuess;

    show($("appDetails"));
    show($("cardStatements"));
    $("appStatus").textContent = "Done";
  } catch (e) {
    alert(e.message || "Something went wrong.");
    $("appStatus").textContent = "";
  } finally {
    $("btnAppExtract").disabled = false;
  }
}

export async function handleAnalyzeStatements() {
  if (!applicationData) return alert("Extract the application first.");

  try {
    const n = await ensurePosition();
    $("posBadge").textContent = `Current position: ${ordinal(n)}`;
    applicationData.positions = n;
  } catch {
    alert("Please provide the current position to continue.");
    return;
  }

  const state = ($("state").value || applicationData.state || "").trim().toUpperCase();
  if (!state || state.length !== 2) return alert("Enter a 2-letter state code.");

  const statements = Array.from($("statements").files || []);
  const need = (state === "NY" || state === "CA") ? 4 : 3;
  if (statements.length < need) {
    return alert(`Please upload at least ${need} statement PDFs for ${state}.`);
  }

  $("btnStmtsMatch").disabled = true;
  $("stmtStatus").textContent = "Analyzing statements and matching...";

  try {
    const data = await analyzeStatementsAndMatch(
      statements,
      state,
      applicationData,
      RESEND_CTX?.statements || null
    );

    lastStatements = data.statements || null;
    renderStatementAnalysis(lastStatements);

    const matched = (data.lenders || []).filter(x => Number(x.score) > 0);
    renderLenders(matched);
    show($("results"));
    show($("composer"));

    // Update composer data
    setComposerData("MCA", applicationData, lastStatements, RESEND_CTX);

    $("stmtStatus").textContent = "Done";
  } catch (e) {
    show($("stmtHelp"));
    alert(e.message || "Something went wrong.");
    $("stmtStatus").textContent = "";
  } finally {
    $("btnStmtsMatch").disabled = false;
  }
}

export function renderStatementAnalysis(statements) {
  if (!statements || typeof statements !== "object") {
    show($("stmtHelp"));
    return;
  }
  show($("stmtAnalysis"));

  const agg = $("stmtAgg");
  agg.innerHTML = "";
  const kv = (k, v) =>
    `<div class="kv"><div class="k">${k}</div><div class="v">${v}</div></div>`;

  const md = statements.monthly_deposits || {};
  const months = Object.keys(md).sort().map(m => `${m}: $${money(md[m])}`).join(" • ") || "—";

  const avgRevVal = statements.average_revenue != null ? statements.average_revenue : null;
  const avgAdbVal = statements.average_daily_balance != null ? statements.average_daily_balance : null;
  const aggNegVal = statements.aggregate_negative_days != null ? statements.aggregate_negative_days : 0;

  agg.insertAdjacentHTML("beforeend", kv("Average Revenue", avgRevVal != null ? `$${money(avgRevVal)}` : "—"));
  agg.insertAdjacentHTML("beforeend", kv("Avg Daily Balance (mean)", avgAdbVal != null ? `$${money(avgAdbVal)}` : "—"));
  agg.insertAdjacentHTML("beforeend", kv("Aggregate Negative Days", fmt(aggNegVal || 0)));
  agg.insertAdjacentHTML("beforeend", kv("Deposits by Month", months));

  const tbody = $("stmtTable").querySelector("tbody");
  tbody.innerHTML = "";
  const list = statements.per_statement || [];

  if (!list.length) {
    show($("stmtEmpty"));
    return;
  }
  hide($("stmtEmpty"));

  list.forEach(s => {
    const tr = document.createElement("tr");
    const cells = [
      s.statement_month || "—",
      s.bank_name || "—",
      s.account_number || "—",
      s.average_daily_balance != null ? `$${money(s.average_daily_balance)}` : "—",
      fmt(s.negative_ending_days || 0),
      fmt(s.credit_count || 0),
      fmt(s.debit_count || 0),
      `$${money(s.monthly_deposits_excl_zelle || 0)}`
    ];
    cells.forEach((c, i) => {
      const td = document.createElement("td");
      td.textContent = String(c);
      if ([3, 4, 5, 6, 7].includes(i)) td.style.textAlign = "right";
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
}

export function initMCA() {
  $("btnAppExtract")?.addEventListener("click", handleExtractApplication);
  $("btnStmtsMatch")?.addEventListener("click", handleAnalyzeStatements);
}
