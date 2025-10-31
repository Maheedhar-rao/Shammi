import os
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

import pdfplumber

# Anthropic (Claude) client
try:
    from anthropic import Anthropic
except Exception:
    Anthropic = None  # allow import even if not installed yet


class MCAAnalyzer:
    """
    Reference analyzer used by app.py
    """

    def __init__(self, anthropic_api_key: str = ""):
        self.anthropic_api_key = anthropic_api_key

        # Lazy client init to avoid errors if key missing during import-time
        self._client = None
        if anthropic_api_key and Anthropic is not None:
            try:
                self._client = Anthropic(api_key=anthropic_api_key)
            except Exception:
                self._client = None

    # ----------------------------
    # PDF extraction
    # ----------------------------
    def load_bank_statement_pdf(self, pdf_path: str) -> Dict[str, Any]:
        """
        Returns:
          {
            "type": "pdf",
            "text": "...",
            "tables": [
              {"rows": [["col1","col2",...], ...], "page": 1},
              ...
            ]
          }
        """
        text_parts: List[str] = []
        tables_out: List[Dict[str, Any]] = []

        try:
            with pdfplumber.open(pdf_path) as pdf:
                for i, page in enumerate(pdf.pages, start=1):
                    try:
                        page_text = page.extract_text() or ""
                    except Exception:
                        page_text = ""
                    if page_text.strip():
                        text_parts.append(f"[Page {i}]\n{page_text}")

                    # extract_tables() returns list[list[list[str|None]]]
                    try:
                        page_tables = page.extract_tables() or []
                        for tbl in page_tables:
                            rows = [[(cell or "").strip() for cell in row] for row in tbl if row]
                            if rows:
                                tables_out.append({"page": i, "rows": rows})
                    except Exception:
                        pass
        except Exception as e:
            return {"type": "pdf", "text": f"[ERROR reading PDF: {e}]", "tables": []}

        return {"type": "pdf", "text": "\n\n".join(text_parts), "tables": tables_out}

    # ----------------------------
    # LLM Analysis
    # ----------------------------
    def _llm_analyze(self, combined_text: str, tables: List[Dict[str, Any]], business_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calls Anthropic Claude to produce structured underwriting metrics + narrative.
        If no API key, returns a deterministic mock.
        """
        if not self._client:
            # Mock output (useful in dev without keys)
            return {
                "generated_at": datetime.utcnow().isoformat(),
                "business": business_info,
                "summary": "Mock analysis (no Anthropic key): extracted from text/tables.",
                "metrics": {
                    "avg_daily_balance": 12345.67,
                    "avg_monthly_revenue": 78500.00,
                    "monthly_deposit_count": 83,
                    "negative_days_last_3_months": 2,
                    "large_deposits_over_10k": [
                        {"date": "2025-08-12", "amount": 15000.00, "note": "Potential MCA funding"}
                    ],
                    "positions_detected": 2
                },
                "eligibility": {
                    "meets_min_revenue": True,
                    "meets_min_adb": True,
                    "meets_neg_days_policy": True,
                    "restricted_industries_flags": [],
                },
                "notes": [
                    "This is a mock output. Connect Anthropic to compute real values.",
                    "Tables and textual patterns were not evaluated by an LLM."
                ]
            }

        system = (
            "You are a rigorous underwriting assistant for merchant cash advance (MCA). "
            "You parse bank statement text and table-like rows to compute underwriting metrics. "
            "Return a concise JSON ONLY with keys: generated_at, business, metrics, eligibility, notes, narrative."
        )

        # Make tables compact for the prompt
        compact_tables = []
        for t in tables[:10]:  # cap to reduce token use
            rows = t.get("rows", [])
            compact_tables.append({"page": t.get("page"), "rows_sample": rows[:20]})

        user = {
            "business_info": business_info,
            "instructions": {
                "compute": [
                    "avg_daily_balance (per 3-4 months, approximate okay if only text available)",
                    "avg_monthly_revenue (exclude intra-account transfers if possible)",
                    "monthly_deposit_count (approx OK)",
                    "negative_days_last_3_months (count days balance < 0; if unavailable, infer from text)",
                    "large_deposits_over_10k (list {date, amount, note})",
                    "positions_detected (infer MCA remittance frequency from transactions)"
                ],
                "return_format": "JSON object only, no prose outside JSON",
                "extra": [
                    "eligibility booleans for min revenue/adp/negative days as generic screening (assume thresholds: revenue>=20000, ADB>=1000, negatives<=4/mo)",
                    "notes: array of short caveats",
                    "narrative: 4-6 sentence underwriting summary"
                ],
            },
            "statement_text_excerpt": combined_text[:24000],  # keep within context
            "statement_tables_excerpt": compact_tables,
        }

        msg = self._client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=1400,
            temperature=0.1,
            system=system,
            messages=[
                {"role": "user", "content": json.dumps(user, ensure_ascii=False)}
            ],
        )
        content = (msg.content[0].text if getattr(msg, "content", None) else "").strip()

        # Try to parse JSON; if it isn't pure JSON, try to find a JSON block.
        parsed: Optional[Dict[str, Any]] = None
        try:
            parsed = json.loads(content)
        except Exception:
            # naive fallback: strip backticks if present
            try:
                content2 = content.strip("` \n")
                parsed = json.loads(content2)
            except Exception:
                parsed = {
                    "generated_at": datetime.utcnow().isoformat(),
                    "business": business_info,
                    "summary": "Model did not return strict JSON; raw content included.",
                    "raw": content,
                }
        return parsed

    def prepare_analysis_data(self, combined_pdf_payload: Dict[str, Any], business_info: Dict[str, Any]) -> Any:
        """
        Orchestrates the analysis call.
        Returns either a structured dict or a JSON string (for display).
        """
        text = combined_pdf_payload.get("text", "")
        tables = combined_pdf_payload.get("tables", [])

        result = self._llm_analyze(text, tables, business_info)

        # You can choose to return dict (preferred) or a pretty JSON string for the frontend.
        # The provided index.html can display either.
        return result

    # Optional: file persistence
    def save_report(self, report: Any, folder: str = "reports") -> str:
        os.makedirs(folder, exist_ok=True)
        ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        path = os.path.join(folder, f"report-{ts}.json")
        try:
            with open(path, "w", encoding="utf-8") as f:
                if isinstance(report, str):
                    f.write(report)
                else:
                    json.dump(report, f, ensure_ascii=False, indent=2)
        except Exception:
            pass
        return path
    @staticmethod
    def get_underwrite_statements_schema() -> Dict[str, Any]:
        """
        Schema description for the `statements` object that
        /api/underwrite/statements-and-match should return.

        This matches what underwrite.html expects in
        renderStatementAnalysis(statements).
        """
        return {
            # Aggregate tiles at the top of the "Statement Analysis" card
            "average_revenue": (
                "float: average monthly revenue across the uploaded statements, "
                "shown as 'Average Revenue' in the UI"
            ),
            "average_daily_balance": (
                "float: overall mean Average Daily Balance across the period, "
                "shown as 'Avg Daily Balance (mean)' in the UI"
            ),
            "aggregate_negative_days": (
                "int: total count of negative ending days across all parsed "
                "statements, shown as 'Aggregate Negative Days'"
            ),
            "monthly_deposits": {
                "<month_label>": (
                    "float: total deposits for that month "
                    "(e.g. '2024-01': 53241.23). "
                    "Rendered as 'Deposits by Month' joined by ' • '"
                )
            },

            # Per-statement rows used in the table
            "per_statement": [
                {
                    "statement_month": (
                        "str: label of the statement period, e.g. '2024-01' or "
                        "'Jan 2024' (Month column)"
                    ),
                    "bank_name": "str: bank name (Bank column)",
                    "account_number": (
                        "str: masked or raw account identifier (Account column)"
                    ),
                    "average_daily_balance": (
                        "float: ADB for this statement (Avg Daily Balance column)"
                    ),
                    "negative_ending_days": (
                        "int: count of negative ending days (Neg Days column)"
                    ),
                    "credit_count": (
                        "int: number of credit transactions (Credits column)"
                    ),
                    "debit_count": (
                        "int: number of debit transactions (Debits column)"
                    ),
                    "monthly_deposits_excl_zelle": (
                        "float: total deposits for the statement period excluding "
                        "Zelle/transfers (Deposits (excl Zelle) column)"
                    ),
                }
            ],
        }

    # -------------------------------------------------
    # 2) Small factory to build the payload
    # -------------------------------------------------
    @staticmethod
    def make_underwrite_statements_payload(
        average_revenue: float,
        average_daily_balance: float,
        aggregate_negative_days: int,
        monthly_deposits: Dict[str, float],
        per_statement: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Convenience helper so underwrite.py can easily build the exact
        JSON blob that underwrite.html expects under the key `statements`.
        """
        return {
            "average_revenue": float(average_revenue) if average_revenue is not None else None,
            "average_daily_balance": float(average_daily_balance) if average_daily_balance is not None else None,
            "aggregate_negative_days": int(aggregate_negative_days) if aggregate_negative_days is not None else 0,
            "monthly_deposits": dict(monthly_deposits or {}),
            "per_statement": list(per_statement or []),
        }

