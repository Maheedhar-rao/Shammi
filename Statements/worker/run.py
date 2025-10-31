#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time, os
from pathlib import Path
from gmail_worker import GmailWatcher

BASE_DIR = Path(__file__).resolve().parents[1]  # -> Statements/
TOKENS_DIR = BASE_DIR / "tokens"                # created by auth_google.py
DB_PATH = Path(os.environ.get("DEALS_DB_PATH", str(BASE_DIR / "deals.db")))

def list_accounts():
    if not TOKENS_DIR.exists():
        return []
    out = []
    for p in TOKENS_DIR.glob("*.json"):
        # token files are saved as "<email>.json" by auth_google.py
        out.append(p.stem)
    return sorted(out)

if __name__ == "__main__":
    emails = list_accounts()
    if not emails:
        print("No Gmail tokens found in tokens/. Connect Gmail first.")
        raise SystemExit(2)

    print(f"Starting watchers for: {', '.join(emails)}  (DB: {DB_PATH})")
    watchers = [GmailWatcher(email=e, db_path=str(DB_PATH)) for e in emails]

    try:
        while True:
            for w in watchers:
                try:
                    w.tick()  # one light polling cycle
                except Exception as e:
                    print(f"[{w.email}] tick error: {e}")
            time.sleep(60)  # tune as you like
    except KeyboardInterrupt:
        print("Exiting.")
