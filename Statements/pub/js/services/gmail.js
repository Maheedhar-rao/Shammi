/**
 * Gmail integration service
 */

import { API_BASE } from '../config.js';
import { $, show, hide } from '../utils.js';

export let gmailConnected = false;
export let gmailEmail = null;

export async function refreshGmailStatus() {
  const dot = $("gmailDot");
  const txt = $("gmailText");
  const btnC = $("btnConnectGmail");
  const btnD = $("btnDisconnectGmail");

  try {
    const res = await fetch(`${API_BASE}/api/auth/google/status`);
    const data = await res.json();
    gmailConnected = !!data.connected;
    gmailEmail = data.email || null;
  } catch {
    gmailConnected = false;
    gmailEmail = null;
  }

  if (gmailConnected) {
    dot.classList.add("connected");
    txt.textContent = `Connected: ${gmailEmail}`;
    hide(btnC);
    show(btnD);
  } else {
    dot.classList.remove("connected");
    txt.textContent = "Not connected";
    show(btnC);
    hide(btnD);
  }
}

export function connectGmail() {
  window.location.href = `${API_BASE}/api/auth/google/login`;
}

export async function disconnectGmail() {
  try {
    await fetch(`${API_BASE}/api/auth/google/logout`, { method: "POST" });
  } catch {}
  await refreshGmailStatus();
}

export function initGmailListeners() {
  $("btnConnectGmail")?.addEventListener("click", connectGmail);
  $("btnDisconnectGmail")?.addEventListener("click", disconnectGmail);
}
