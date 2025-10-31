#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flask UI for Bank Statements Extraction — separate app, styled like your Application Extractor.
"""

import os
from typing import Dict
from flask import Flask, request, render_template_string, redirect, url_for, flash
import Statements_extractor as se  # same folder

MAX_CONTENT_LENGTH = 50 * 1024 * 1024  # 50MB
ALLOWED = {"pdf"}

app = Flask(__name__)
app.secret_key = os.environ.get("APP_SECRET", "dev")
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH

TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Bank Statements Extractor</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root {
      --bg: #f6f7fb; --card: #ffffff; --ink: #202124; --muted: #5f6368;
      --line: #e5e7eb; --accent: #2d5cf6;
    }
    * { box-sizing: border-box; }
    body { margin: 0; background: var(--bg); color: var(--ink);
           font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, "Helvetica Neue", Arial, "Noto Sans", "Liberation Sans", sans-serif; }
    .wrap { max-width: 1100px; margin: 28px auto; padding: 0 16px; }
    header { display:flex; align-items:center; justify-content:space-between; margin-bottom: 18px; }
    h1 { margin: 0; font-size: 24px; letter-spacing: .2px; }
    .card { background: var(--card); border: 1px solid var(--line); border-radius: 14px; padding: 16px; box-shadow: 0 4px 16px rgba(0,0,0,.04); }
    .uploader { display:grid; grid-template-columns: 1.2fr .8fr; gap: 16px; margin-bottom: 18px; }
    @media (max-width: 900px) { .uploader { grid-template-columns: 1fr; } }
    .drop { border: 2px dashed var(--line); border-radius: 12px; padding: 24px; text-align:center; background:#fafbff; transition: border-color .2s, background .2s; cursor:pointer; }
    .drop.drag { border-color: var(--accent); background:#f0f4ff; }
    .hint { color: var(--muted); font-size: 14px; margin-top: 6px; }
    .controls { display:flex; gap:12px; align-items:center; flex-wrap: wrap; }
    .controls label { font-size: 14px; color: var(--muted); }
    select, button { padding: 10px 12px; border-radius: 10px; border:1px solid var(--line); background:#fff; font-size:14px; }
    button[type=submit] { background: var(--accent); color:#fff; border:none; padding: 10px 16px; cursor:pointer; }
    .results { display: grid; gap: 16px; }
    .grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 14px; }
    .field { padding: 12px; border-radius: 12px; border: 1px solid var(--line); background:#fff; }
    .kv { display:flex; justify-content:space-between; gap: 8px; margin:.2rem 0; }
    .pill { display:inline-block; background:#eef2ff; border:1px solid #dfe3f8; padding:4px 8px; border-radius:999px; font-size:12px; margin:2px 6px 0 0; }
    .divider { height:1px; background: var(--line); margin: 10px 0; }
  </style>
</head>
<body>
  <div class="wrap">
    <header><h1>Bank Statements Extractor</h1></header>

    <form class="card" action="{{ url_for('extract') }}" method="post" enctype="multipart/form-data" id="form">
      <div class="uploader">
        <div>
          <div id="drop" class="drop">
            <div style="font-size:16px; margin-bottom:6px;">Drag & drop your PDF statements (multi-file)</div>
            <div class="hint">or click to choose files (max 50MB total). Upload ~4 months for best signal.</div>
            <input type="file" name="files" id="files" accept=".pdf" style="display:none" multiple required />
          </div>
          <div style="color:#b00020; margin-top:6px;">
            {% with messages = get_flashed_messages() %}
              {% if messages %}
                <ul style="margin:6px 0 0 18px;">{% for m in messages %}<li>{{ m }}</li>{% endfor %}</ul>
              {% endif %}
            {% endwith %}
          </div>
        </div>

        <div>
          <div class="controls">
            <label for="state">State rule</label>
            <select id="state" name="state">
              <option value="">All states (avg of all months)</option>
              <option value="NY" {% if request.form.get('state')=='NY' %}selected{% endif %}>NY (best 3 of provided months)</option>
              <option value="CA" {% if request.form.get('state')=='CA' %}selected{% endif %}>CA (best 3 of provided months)</option>
            </select>
            <button type="submit">Extract</button>
          </div>
          <div class="hint" style="margin-top:8px;">Average Revenue uses deposits only (Zelle excluded).</div>
        </div>
      </div>
    </form>

    {% if results %}
      <div class="results">

        {% for item in results %}
          <div class="card">
            <h3 style="margin:0 0 8px 0;">{{ item.filename }}</h3>

            <div class="grid">
              <div class="field">
                <div class="kv"><span>Bank Name</span> <b>{{ item.summary.bank_name or "[Not Found]" }}</b></div>
                <div class="kv"><span>Business Name</span> <b>{{ item.summary.business_name or "[Not Found]" }}</b></div>
                <div class="kv"><span>Account Number</span> <b>{{ item.summary.account_number or "[Not Found]" }}</b></div>
                <div class="kv"><span>Statement Month</span> <b>{{ item.summary.statement_month }}</b></div>
                <div class="kv"><span>Deposits excl Zelle</span> <b>{{ "%.2f"|format(item.summary.monthly_deposits_excl_zelle) }}</b></div>
              </div>

              <div class="field">
                <div class="kv"><span>Debit Count</span> <b>{{ item.summary.debit_count }}</b></div>
                <div class="kv"><span>Credit Count</span> <b>{{ item.summary.credit_count }}</b></div>
                <div class="kv"><span>Negative Ending Days</span> <b>{{ item.summary.negative_ending_days }}</b></div>
                <div class="kv"><span>Average Daily Balance</span> <b>{{ item.summary.average_daily_balance if item.summary.average_daily_balance is not none else "[N/A]" }}</b></div>
              </div>

              <div class="field" style="grid-column: 1 / -1;">
                <div><b>Positions - Daily:</b>
                  {% if item.summary.positions_daily %}
                    {% for p in item.summary.positions_daily %}<span class="pill">{{ p }}</span>{% endfor %}
                  {% else %}[none]{% endif %}
                </div>
                <div style="margin-top:6px;"><b>Positions - Weekly:</b>
                  {% if item.summary.positions_weekly %}
                    {% for p in item.summary.positions_weekly %}<span class="pill">{{ p }}</span>{% endfor %}
                  {% else %}[none]{% endif %}
                </div>
              </div>
            </div>
          </div>
        {% endfor %}

        <div class="card">
          <h3 style="margin:0 0 8px 0;">Average Revenue (Deposits only, excludes Zelle)</h3>
          <div class="kv"><span>State rule</span> <b>{{ avg_rule }}</b></div>
          <div class="divider"></div>
          {% for m, v in monthly_deposits_sorted %}
            <div class="kv"><span>{{ m }}</span> <b>{{ "%.2f"|format(v) }}</b></div>
          {% endfor %}
          <div class="divider"></div>
          <div class="kv"><b>AVERAGE</b> <b>{{ avg_revenue if avg_revenue is not none else "[N/A]" }}</b></div>
        </div>

      </div>
    {% endif %}
  </div>

  <script>
    const drop = document.getElementById('drop');
    const fileInput = document.getElementById('files');
    drop.addEventListener('click', () => fileInput.click());
    drop.addEventListener('dragover', e => { e.preventDefault(); drop.classList.add('drag'); });
    drop.addEventListener('dragleave', e => { drop.classList.remove('drag'); });
    drop.addEventListener('drop', e => {
      e.preventDefault(); drop.classList.remove('drag');
      if (e.dataTransfer.files && e.dataTransfer.files.length) {
        fileInput.files = e.dataTransfer.files;
        document.getElementById('form').submit();
      }
    });
  </script>
</body>
</html>
"""

def _allowed(fn: str) -> bool:
    return "." in fn and fn.rsplit(".", 1)[1].lower() in ALLOWED

@app.route("/", methods=["GET"])
def index():
    return render_template_string(TEMPLATE, results=None)

@app.route("/extract", methods=["POST"])
def extract():
    files = request.files.getlist("files")
    if not files:
        flash("Please select at least one PDF.")
        return redirect(url_for("index"))

    state = request.form.get("state") or None
    if state:
        state = state.strip().upper()
        if len(state) != 2:
            flash("Invalid state; using default (average of all months).")
            state = None

    results = []
    monthly_deposits: Dict[str, float] = {}

    for f in files:
        if not f or not f.filename or not _allowed(f.filename):
            continue
        data = f.read()
        if not data:
            flash(f"Empty file: {f.filename}")
            continue

        try:
            summary, daily, txns = se.summarize_statement_from_bytes(data, filename=f.filename)

            # Aggregate deposits for revenue
            if summary.statement_month != "[unknown]":
                monthly_deposits[summary.statement_month] = monthly_deposits.get(summary.statement_month, 0.0) + summary.monthly_deposits_excl_zelle
            else:
                # Fallback: bucket by txns’ months if label unknown
                by_month: Dict[str, float] = {}
                for t in txns:
                    if t.amount > 0 and not any(x in t.desc.lower() for x in se.EXCLUDE_DEPOSIT_KEYWORDS):
                        k = se.month_key(t.dt)
                        by_month[k] = by_month.get(k, 0.0) + t.amount
                for k, v in by_month.items():
                    monthly_deposits[k] = monthly_deposits.get(k, 0.0) + v

            results.append({"filename": f.filename, "summary": summary})
        except Exception as e:
            flash(f"{f.filename}: {e}")

    avg_revenue = se.pick_avg_revenue(monthly_deposits, state)
    avg_rule = ("Best 3 of provided months (NY/CA)" if (state in ("NY","CA")) else "Average of all months")
    monthly_deposits_sorted = sorted(monthly_deposits.items(), key=lambda x: x[0])

    return render_template_string(
        TEMPLATE,
        results=results,
        monthly_deposits_sorted=monthly_deposits_sorted,
        avg_revenue=avg_revenue,
        avg_rule=avg_rule
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050, debug=True)
