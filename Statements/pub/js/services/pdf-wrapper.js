/**
 * PDF wrapping service
 */

import { API_BASE } from '../config.js';

export async function fileToBase64(file) {
  const buf = await file.arrayBuffer();
  let bin = '';
  const bytes = new Uint8Array(buf);
  const chunk = 0x8000;
  for (let i = 0; i < bytes.length; i += chunk) {
    bin += String.fromCharCode.apply(null, bytes.subarray(i, i + chunk));
  }
  return btoa(bin);
}

export async function wrapPdfForLender(originalFile, lenderName, opts = {}) {
  // 1) Skip if already wrapped
  if (/\.(?:wrapped)\.pdf$/i.test(originalFile.name) || /-\s*wrapped\.pdf$/i.test(originalFile.name)) {
    return originalFile;
  }

  // 2) Build form with DEFAULT watermark/footer
  const fd = new FormData();
  fd.append("file", originalFile, originalFile.name);
  fd.append("lender", lenderName || "");
  fd.append("force_watermark_text", opts.force_watermark_text || "SENT VIA PATHWAY CATALYST");
  fd.append("force_footer_template", opts.force_footer_template || "Submitted to {{lender}} by {{user}} ({{email}}) • Deal #{{deal}}");
  if (opts.deal_id) fd.append("deal_id", String(opts.deal_id));
  if (opts.recipient_email) fd.append("recipient_email", opts.recipient_email);

  const res = await fetch(`${API_BASE}/api/underwrite/wrap`, { method: "POST", body: fd, credentials: "same-origin" });

  // 3) Robust error handling
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`wrap failed (${res.status}): ${text || "unknown error"}`);
  }
  const ct = res.headers.get("content-type") || "";
  if (!ct.toLowerCase().includes("application/pdf")) {
    const text = await res.text().catch(() => "");
    throw new Error(`wrap returned non-PDF (${ct}): ${text.slice(0, 200)}`);
  }

  // 4) Normalize filename
  const base = originalFile.name.replace(/\.pdf$/i, "");
  const safeLender = (lenderName || "").replace(/[\/\\:*?"<>|]+/g, "_").trim();
  const outName = safeLender ? `${base}.${safeLender}.wrapped.pdf` : `${base}.wrapped.pdf`;

  const blob = await res.blob();
  return new File([blob], outName, { type: "application/pdf" });
}

export async function buildPerLenderAttachments(lenders) {
  const appFile = document.getElementById("application")?.files?.[0] || null;
  const stmtFiles = Array.from(document.getElementById("statements")?.files || []);
  const allFiles = [...(appFile ? [appFile] : []), ...stmtFiles];

  if (!allFiles.length) throw new Error("No files selected to wrap.");

  const perMap = {};
  for (let i = 0; i < lenders.length; i++) {
    const lender = lenders[i];
    document.getElementById("sendStatus").textContent = `Preparing ${i + 1}/${lenders.length}: ${lender}…`;
    const list = [];
    for (const f of allFiles) {
      const b64 = await fileToBase64(f);
      list.push({ name: f.name, mime: "application/pdf", data: b64 });
    }
    perMap[lender] = list;
  }
  return perMap;
}
