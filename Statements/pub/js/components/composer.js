/**
 * Email composer component
 */

import { $, show, hide } from '../utils.js';
import { API_BASE } from '../config.js';
import { selectedLenders } from './lender-tiles.js';
import { buildPerLenderAttachments } from '../services/pdf-wrapper.js';
import { gmailConnected } from '../services/gmail.js';

let applicationData = null;
let lastStatements = null;
let CURRENT_MODE = null;
let RESEND_CTX = null;

export function setComposerData(mode, appData, statements, resendCtx = null) {
  CURRENT_MODE = mode;
  applicationData = appData;
  lastStatements = statements;
  RESEND_CTX = resendCtx;
}

export async function sendEmails() {
  if (!gmailConnected) {
    const go = confirm("Gmail is not connected. Connect now?");
    if (go) window.location.href = `${API_BASE}/api/auth/google/login`;
    return;
  }

  const chosen = Array.from(selectedLenders);
  if (!chosen.length) return alert("Select at least one lender.");

  const appFile = $("application")?.files?.[0] || null;
  const stmtFiles = Array.from($("statements")?.files || []);
  const allFiles = [...(appFile ? [appFile] : []), ...stmtFiles];

  if (!allFiles.length) {
    const cont = confirm("No PDFs selected to attach. Send without attachments?");
    if (!cont) return;
  }

  $("btnSend").disabled = true;

  // Build per-lender attachments
  let perMap = {};
  try {
    perMap = await buildPerLenderAttachments(chosen);
  } catch (e) {
    console.error(e);
    alert("Failed preparing per-lender attachments.");
    $("btnSend").disabled = false;
    return;
  }

  $("sendStatus").textContent = "Sending…";

  const payload = {
    selected_lenders: chosen,
    subject: $("subject").value || "Submission",
    message: $("message").value || "",
    cc: ($("cc").value || "").split(",").map(s => s.trim()).filter(Boolean),
    mode: CURRENT_MODE,
    application: applicationData || {},
    statements: lastStatements || {},
    per_lender_attachments: perMap,
    wrap_pdfs: true,
    use_secure_links: true
  };

  if (RESEND_CTX) {
    payload.parent_deal_id = RESEND_CTX.dealId;
    payload.exclude = Array.from(RESEND_CTX.alreadySent || []);
  }

  try {
    const res = await fetch(`${API_BASE}/api/underwrite/send`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || "Send failed.");

    const sent = (data.deliveries || []).filter(d => d.status === "sent").length;
    const skipped = (data.deliveries || []).filter(d => d.status === "skipped").length;
    alert(`Sent to ${sent} lender(s).${skipped ? " " + skipped + " skipped." : ""}`);
    $("sendStatus").textContent = "Done";
    sessionStorage.setItem("afterSendGoTo", "DASH");
    setTimeout(() => window.location.reload(), 350);
  } catch (e) {
    alert(e.message || "Something went wrong.");
    $("sendStatus").textContent = "";
  } finally {
    $("btnSend").disabled = false;
  }
}

export function initComposer() {
  $("btnSend")?.addEventListener("click", sendEmails);
}
