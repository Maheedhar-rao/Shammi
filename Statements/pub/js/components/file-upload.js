/**
 * File upload component with drag-and-drop
 */

import { $, formatBytes } from '../utils.js';

export function setupDropzone(dropId, inputId, multiple, onPick) {
  const dz = $(dropId);
  const input = $(inputId);
  if (!dz || !input) return;

  const highlight = (on) => dz.classList.toggle("dragover", !!on);
  const pick = () => input.click();

  dz.addEventListener("click", pick);
  dz.addEventListener("keydown", (e) => {
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      pick();
    }
  });
  dz.addEventListener("dragover", (e) => {
    e.preventDefault();
    highlight(true);
  });
  dz.addEventListener("dragleave", () => highlight(false));
  dz.addEventListener("drop", (e) => {
    e.preventDefault();
    highlight(false);
    const files = Array.from(e.dataTransfer?.files || []).filter(f => f.type === "application/pdf");
    if (!files.length) return;
    try {
      const dt = new DataTransfer();
      files.forEach(f => dt.items.add(f));
      input.files = dt.files;
    } catch (_) {}
    if (!multiple && input.files.length > 1) {
      try {
        const dt = new DataTransfer();
        dt.items.add(input.files[0]);
        input.files = dt.files;
      } catch (_) {}
    }
    onPick?.();
  });
  input.addEventListener("change", onPick);
}

export async function prepareApplicationFile() {
  const box = $("appPreview");
  const input = $("application");
  const f = input.files[0];
  if (!f) {
    box.innerHTML = "";
    return;
  }
  box.innerHTML = `<strong>Selected:</strong> ${f.name} <span class="muted">(${formatBytes(f.size)})</span>`;
}

export async function prepareStatementsFiles() {
  const box = $("stmtsPreview");
  const input = $("statements");
  const files = Array.from(input.files || []);
  if (!files.length) {
    box.innerHTML = "";
    return;
  }

  const total = files.reduce((a, f) => a + (f.size || 0), 0);
  box.innerHTML = `<strong>${files.length} PDF${files.length > 1 ? "s" : ""}:</strong> ${formatBytes(total)}` +
    `<ul>${files.map(f => `<li>${f.name} <span class="muted">(${formatBytes(f.size)})</span></li>`).join("")}</ul>`;
}

export function initFileUploads() {
  setupDropzone("appDrop", "application", false, prepareApplicationFile);
  setupDropzone("stmtDrop", "statements", true, prepareStatementsFiles);
}
