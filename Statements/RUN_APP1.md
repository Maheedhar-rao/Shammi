# Running app1.py - Refactored Version

## Quick Start

```bash
# Navigate to the Statements directory
cd "/Users/maheedharraogovada/Desktop/Paradise again/Statements"

# Run the refactored version
python app1.py
```

## Key Differences from app.py

| Feature | app.py (Original) | app1.py (Refactored) |
|---------|-------------------|---------------------|
| **Static Folder** | `public/` | `pub/` |
| **Port** | 5055 | 5056 |
| **Login Page** | `public/login.html` (230 lines) | `pub/login.html` (79 lines) |
| **Signup Page** | `public/signup.html` (117 lines) | `pub/signup.html` (52 lines) |
| **Underwrite Page** | `public/underwrite.html` (2,679 lines) | `pub/underwrite.html` (277 lines) |
| **CSS** | Inline in HTML | Separate files in `pub/css/` |
| **JavaScript** | Inline in HTML | ES6 modules in `pub/js/` |

## Access URLs

### With app1.py running on port 5056:

- **Login**: http://localhost:5056/login
- **Signup**: http://localhost:5056/signup
- **Underwrite**: http://localhost:5056/underwrite.html (after login)
- **Root**: http://localhost:5056/ (redirects based on auth)

### Original app.py on port 5055:

- **Login**: http://localhost:5055/login
- **Signup**: http://localhost:5055/signup
- **Underwrite**: http://localhost:5055/underwrite.html

## Running Both Versions Side-by-Side

You can run both versions simultaneously for A/B testing:

### Terminal 1 - Original version:
```bash
python app.py
# Runs on http://localhost:5055
```

### Terminal 2 - Refactored version:
```bash
python app1.py
# Runs on http://localhost:5056
```

## What's Different in app1.py?

### 1. Static Folder Changed
```python
# app.py
static_folder=str(PUBLIC_DIR)  # Serves from public/

# app1.py
static_folder=str(PUB_DIR)     # Serves from pub/
```

### 2. CORS Origins Updated
```python
# app.py
"origins": ["http://127.0.0.1:5055", "http://localhost:5055"]

# app1.py
"origins": ["http://127.0.0.1:5056", "http://localhost:5056"]
```

### 3. Additional Routes for Assets
```python
# app1.py adds these routes:
@app.get("/css/<path:filename>")    # Serves pub/css/*
@app.get("/js/<path:filename>")     # Serves pub/js/*
```

### 4. Updated ALLOW_PREFIXES
```python
ALLOW_PREFIXES = (
    "/login",
    "/login.html",
    "/signup",
    "/signup.html",
    "/css/",       # NEW - Allow CSS files
    "/js/",        # NEW - Allow JS files
    "/api/auth/user/",
    "/api/auth/google/",
    # ... rest unchanged
)
```

### 5. Port Changed
```python
# app.py
app.run(host="0.0.0.0", port=5055, debug=True)

# app1.py
app.run(host="0.0.0.0", port=5056, debug=True)
```

## Testing Checklist

### 1. Login Page (30 seconds)
```bash
# Open in browser:
http://localhost:5056/login

# Check:
✅ Page loads without console errors
✅ CSS is properly applied (dark gradient background)
✅ Password toggle works (Show/Hide)
✅ "Forgot password?" link works
```

### 2. Signup Page (30 seconds)
```bash
# Open in browser:
http://localhost:5056/signup

# Check:
✅ Page loads without console errors
✅ CSS is properly applied (same style as login)
✅ All fields present (first name, last name, mobile, email, password)
✅ Password toggle works
```

### 3. Underwrite Page (1-2 minutes)
```bash
# Open in browser:
http://localhost:5056/underwrite.html

# Check:
✅ Page loads without console errors
✅ Product tabs visible (MCA, CCS, REV, DASH, LEADS, OPP)
✅ Application dropzone visible
✅ Statements dropzone visible
✅ Console shows no JavaScript errors
```

## Common Issues

### Issue 1: Port Already in Use
```bash
# Error: Address already in use: 5056

# Solution: Kill the process
lsof -ti:5056 | xargs kill -9

# Or change the port in app1.py
app.run(host="0.0.0.0", port=5057, debug=True)
```

### Issue 2: CSS/JS Not Loading
```bash
# Check browser console for 404 errors

# Verify files exist:
ls -la pub/css/
ls -la pub/js/

# Verify routes with:
curl http://localhost:5056/debug/routes
```

### Issue 3: Module Import Errors
```bash
# If you see: "Cannot use import statement outside a module"

# Verify underwrite.html has:
<script type="module" src="js/main.js"></script>
#                      ^^^^^ This is critical!
```

## Debugging

### Check All Routes
```bash
# See all registered routes:
curl http://localhost:5056/debug/routes | jq
```

### Check Authentication Status
```bash
# See current auth status:
curl http://localhost:5056/api/whoami
```

### Monitor Console Output
```bash
# When running app1.py, watch for:
🚀 Starting REFACTORED version (app1.py)
📂 Serving from: pub/ folder
🌐 URL: http://localhost:5056
```

## Switching to Production

Once you've tested app1.py and confirmed it works:

### Option 1: Replace app.py
```bash
# Backup original
cp app.py app.py.backup

# Use the refactored version
cp app1.py app.py

# Update port back to 5055 in app.py
```

### Option 2: Keep Both
```bash
# Keep app1.py for refactored version
# Keep app.py for original version
# Deploy whichever you prefer
```

## Files Structure

```
Statements/
├── app.py              # Original (port 5055, serves public/)
├── app1.py             # Refactored (port 5056, serves pub/)
├── public/             # Original monolithic files
│   ├── login.html      (230 lines)
│   ├── signup.html     (117 lines)
│   └── underwrite.html (2,679 lines)
└── pub/                # Refactored modular files
    ├── login.html      (79 lines)
    ├── signup.html     (52 lines)
    ├── underwrite.html (277 lines)
    ├── css/
    │   ├── auth.css
    │   ├── base.css
    │   ├── components.css
    │   └── underwrite.css
    └── js/
        ├── config.js
        ├── utils.js
        ├── auth.js
        ├── auth-login.js
        ├── auth-signup.js
        ├── api.js
        ├── main.js
        ├── components/
        ├── modules/
        └── services/
```

## Next Steps

1. ✅ Run `python app1.py`
2. ✅ Test login page at http://localhost:5056/login
3. ✅ Test signup page at http://localhost:5056/signup
4. ✅ Test underwrite page (after login)
5. ✅ Check browser console for errors
6. ✅ Compare with original at http://localhost:5055
7. ✅ Switch to production when confident

## Benefits of Refactored Version

1. **89.7% smaller HTML** - 277 lines vs 2,679 lines
2. **Maintainable** - Each file has a single responsibility
3. **Testable** - JavaScript modules can be tested independently
4. **Cacheable** - Browser caches CSS/JS files
5. **Scalable** - Easy to add new features
6. **Reusable** - Components can be shared across pages
7. **Modern** - Uses ES6 modules and best practices

---

**Ready to test!** 🚀
