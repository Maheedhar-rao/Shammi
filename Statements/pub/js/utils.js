/**
 * Utility functions
 */

// DOM helpers
export const $ = (id) => document.getElementById(id);
export const show = (el) => el?.classList.remove("hidden");
export const hide = (el) => el?.classList.add("hidden");

// Formatting
export const fmt = (n) => isFinite(n) ? Number(n).toLocaleString() : "";
export const money = (n) => isFinite(n) ? Number(n).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : "";

export function ordinal(n) {
  n = Number(n);
  if (!isFinite(n)) return String(n);
  const s = ["th", "st", "nd", "rd"];
  const v = n % 100;
  return n + (s[(v - 20) % 10] || s[v] || s[0]);
}

export function formatBytes(bytes) {
  if (!Number.isFinite(bytes)) return "";
  const u = ["B", "KB", "MB", "GB"];
  let i = 0, n = bytes;
  while (n >= 1024 && i < u.length - 1) {
    n /= 1024;
    i++;
  }
  return `${n.toFixed(n >= 10 || i === 0 ? 0 : 1)} ${u[i]}`;
}

// Input helpers
export function setInputValue(id, v) {
  const el = $(id);
  if (el) el.value = v ?? "";
}

// File helpers
export async function fileToBase64(file) {
  return new Promise((resolve, reject) => {
    const r = new FileReader();
    r.onload = () => resolve(r.result); // data URL
    r.onerror = reject;
    r.readAsDataURL(file);
  });
}

export function isPDF(file) {
  const n = (file?.name || "").toLowerCase();
  return (file?.type || "").toLowerCase().includes("pdf") || n.endsWith(".pdf");
}

// Lender helpers
export function makeStaticLenders(names) {
  return names.map(n => ({ business_name: n, score: 1.0, reason: "Program lender" }));
}
