/**
 * Login page functionality
 */

const $ = id => document.getElementById(id);

function showMsg(el, msg, ok = true) {
  el.className = "msg " + (ok ? "ok" : "err");
  el.textContent = msg;
  el.style.display = "block";
}

function hideMsg(el) {
  el.style.display = "none";
  el.textContent = "";
  el.className = "msg";
}

function togglePw(input, btn) {
  const go = () => {
    const t = input.type === "password" ? "text" : "password";
    input.type = t;
    btn.textContent = (t === "password" ? "Show" : "Hide");
  };
  btn.addEventListener("click", go);
  btn.addEventListener("keydown", (e) => {
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      go();
    }
  });
}

function swap(view) {
  ["viewSignIn", "viewRequestReset", "viewReset"].forEach(id => $(id).classList.add("hidden"));
  $(view).classList.remove("hidden");
  ["noteIn", "noteRequestReset", "noteReset"].forEach(id => hideMsg($(id)));
}

async function call(url, payload) {
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {})
  });
  const ct = (r.headers.get("content-type") || "").toLowerCase();
  const data = ct.includes("application/json") ? await r.json().catch(() => ({})) : {};
  if (!r.ok) throw new Error((data && data.error) || `HTTP ${r.status}`);
  return data;
}

function afterAuthSuccess(msg) {
  const next = new URLSearchParams(location.search).get("next") || "/underwrite.html";
  const el = $("noteIn");
  el.className = "msg ok";
  el.textContent = msg || "Signed in. Redirecting…";
  el.style.display = "block";
  setTimeout(() => { location.href = next; }, 300);
}

// Initialize
function init() {
  // Password toggles
  togglePw($("inPassword"), $("inPwToggle"));
  [["resetNew", "resetNewToggle"], ["resetNew2", "resetNew2Toggle"]].forEach(([i, t]) => togglePw($(i), $(t)));

  // Forgot password link
  $("linkForgot").addEventListener("click", (e) => {
    e.preventDefault();
    const email = $("inEmail").value.trim();
    swap("viewRequestReset");
    $("resetEmail").value = email;
    $("resetEmail").focus();
  });

  // Back to login
  $("btnBackToLogin").addEventListener("click", () => swap("viewSignIn"));

  // Send reset email
  $("btnSendReset").addEventListener("click", async () => {
    hideMsg($("noteRequestReset"));
    try {
      const email = $("resetEmail").value.trim();
      if (!email) throw new Error("Please enter your email address.");

      await call("/api/auth/user/reset", { email });
      $("resetEmail").value = "";
      showMsg($("noteRequestReset"), "If your email exists, a reset link was sent. Check your inbox.", true);
    } catch (err) {
      showMsg($("noteRequestReset"), err.message || "Could not send reset email", false);
    }
  });

  // Login
  $("btnLogin").addEventListener("click", async () => {
    hideMsg($("noteIn"));
    try {
      const email = $("inEmail").value.trim();
      const password = $("inPassword").value;
      if (!email || !password) throw new Error("Enter email and password.");
      await call("/api/auth/user/login", { email, password });
      afterAuthSuccess("Signed in. Redirecting…");
    } catch (e) {
      showMsg($("noteIn"), e.message || "Login failed", false);
    }
  });

  // Reset password
  $("btnResetPassword").addEventListener("click", async () => {
    hideMsg($("noteReset"));
    try {
      const p1 = $("resetNew").value, p2 = $("resetNew2").value;
      if (!p1 || p1.length < 8) throw new Error("New password must be at least 8 characters.");
      if (p1 !== p2) throw new Error("Passwords do not match.");

      const qs = new URLSearchParams(location.hash.replace(/^#/, ""));
      const token = qs.get("access_token");

      if (!token) throw new Error("No recovery token found. Please use the link from your email.");

      await call("/api/auth/user/reset-confirm", { token, new_password: p1 });
      showMsg($("noteReset"), "Password updated! Redirecting to sign in…", true);
      setTimeout(() => {
        location.hash = "";
        swap("viewSignIn");
      }, 700);
    } catch (e) {
      showMsg($("noteReset"), e.message || "Could not reset password", false);
    }
  });

  // Handle recovery token in URL
  (function handleRecovery() {
    const qs = new URLSearchParams(location.hash.replace(/^#/, ""));
    if (qs.get("type") === "recovery") {
      swap("viewReset");
    }
  })();

  // Enter key handlers
  ["inEmail", "inPassword"].forEach(id => {
    $(id).addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        $("btnLogin").click();
      }
    });
  });

  $("resetEmail").addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      $("btnSendReset").click();
    }
  });
}

// Auto-initialize
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}
