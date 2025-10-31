# Authentication Fix for app1.py

## Problem
After signing in on the login page, the redirect to `/underwrite.html` failed and sent users back to `/login`.

## Root Cause
The auth flow had two issues:

1. **auth_user_proxy.py** set httpOnly cookies but **didn't set the Flask session**
2. **app1.py** checked `session.get("user_email")` at line 141
3. Since session wasn't set, the check failed → redirect to /login

## Solution
Updated [auth_user_proxy.py](auth_user_proxy.py) to set the Flask session on login and clear it on logout:

### Login (Line 86)
```python
# Set session for Flask's session-based auth checks
session["user_email"] = email
```

### Logout (Line 144)
```python
# Clear Flask session
session.pop("user_email", None)
```

## Testing

### Test the fix:
```bash
# 1. Start app1.py
python app1.py

# 2. Open browser to http://localhost:5056/login

# 3. Sign in with valid credentials

# 4. Should redirect to /underwrite.html ✅
```

### Verify session is set:
```bash
# After login, check:
curl -b cookies.txt http://localhost:5056/api/whoami

# Should see:
# "session_user_email": "your@email.com"
```

## Flow Diagram

### Before Fix (❌ Failed):
```
Login Page
  ↓
POST /api/auth/user/login
  ↓
auth_user_proxy.py sets cookies ✅
auth_user_proxy.py DOES NOT set session ❌
  ↓
JavaScript redirects to /underwrite.html
  ↓
app1.py checks session.get("user_email") → None ❌
  ↓
Redirect back to /login ❌
```

### After Fix (✅ Works):
```
Login Page
  ↓
POST /api/auth/user/login
  ↓
auth_user_proxy.py sets cookies ✅
auth_user_proxy.py sets session["user_email"] ✅
  ↓
JavaScript redirects to /underwrite.html
  ↓
app1.py checks session.get("user_email") → "user@email.com" ✅
  ↓
Serve /underwrite.html ✅
```

## Why Two Auth Systems?

The codebase has **two parallel auth systems**:

1. **auth_user.py** - Old SQLite-based session auth
2. **auth_user_proxy.py** - New Supabase-based auth with httpOnly cookies

Both are registered in app1.py (lines 33-35). The app uses **auth_user_proxy.py** for actual authentication (Supabase), but **app1.py still checks the Flask session** for route protection.

The fix bridges these two systems by making auth_user_proxy.py also set the session that app1.py expects.

## Files Modified
- [auth_user_proxy.py:86](auth_user_proxy.py#L86) - Added `session["user_email"] = email` after successful login
- [auth_user_proxy.py:144](auth_user_proxy.py#L144) - Added `session.pop("user_email", None)` on logout

---

**Status: FIXED ✅**

Now when you sign in, you'll be properly redirected to the underwrite page!
