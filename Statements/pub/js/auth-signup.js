/**
 * Signup page functionality
 */

const $ = (id) => document.getElementById(id);
const show = (el, msg) => {
  if (msg) el.textContent = msg;
  el.style.display = "block";
};
const hide = (el) => {
  el.style.display = "none";
};
const lock = (on) => {
  $("btnLogin").disabled = on;
  $("btnSignup").disabled = on;
};

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

function init() {
  // Password toggle
  $("pwToggle").onclick = () => {
    const inp = $("password");
    const next = inp.type === "password" ? "text" : "password";
    inp.type = next;
    $("pwToggle").textContent = next === "password" ? "Show" : "Hide";
  };

  // Go to login
  $("btnLogin").onclick = () => location.href = "/login.html?next=/underwrite.html";

  // Signup
  $("btnSignup").onclick = async () => {
    hide($("ok"));
    hide($("err"));
    lock(true);
    try {
      const email = $("email").value.trim();
      const password = $("password").value;
      const first = $("first_name").value.trim();
      const last = $("last_name").value.trim();
      const mobile = $("mobile").value.trim();

      if (!first || !last) throw new Error("Please provide first and last name.");
      if (!email) throw new Error("Enter your email.");
      if (!password || password.length < 8) throw new Error("Password must be at least 8 characters.");

      await call("/api/auth/user/signup", {
        email,
        password,
        profile: { full_name: `${first} ${last}`.trim(), mobile }
      });

      show($("ok"), "Account created. Check your email to confirm, then sign in.");
      setTimeout(() => location.reload(), 800);
    } catch (e) {
      show($("err"), e.message || "Signup failed");
    } finally {
      lock(false);
    }
  };
}

// Auto-initialize
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}
