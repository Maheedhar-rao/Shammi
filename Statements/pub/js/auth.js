/**
 * Authentication module
 */

import { API_BASE } from './config.js';

export async function requireAuth() {
  try {
    const res = await fetch(`${API_BASE}/api/auth/user/status`, { credentials: 'include' });
    const j = await res.json();
    const isAuthed = j && (j.authenticated || j.authorized || j.logged_in || j.loggedIn) && (j.user?.email || j.email);

    if (!isAuthed) {
      const next = encodeURIComponent(location.pathname + location.search);
      location.href = `/login.html?expired=1&next=${next}`;
      return false;
    }
    return true;
  } catch (e) {
    const next = encodeURIComponent(location.pathname + location.search);
    location.href = `/login.html?expired=1&next=${next}`;
    return false;
  }
}

export async function logout() {
  try {
    await fetch(`${API_BASE}/api/auth/user/logout`, { method: "POST", credentials: "same-origin" });
  } catch (_) {
    // no-op: we'll still redirect
  }
  // Clean up any local state and go to login page
  try {
    sessionStorage.clear();
    localStorage.removeItem("croc_email");
  } catch {}
  // Replace to prevent going back to a protected page
  window.location.replace("/login.html?logged_out=1");
}
