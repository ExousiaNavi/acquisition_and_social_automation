"""
main.py – run back‑office fetch from AcquisitionController
"""
import os
import json
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from controllers.AcquisitionController import AcquisitionController
from controllers.SpreadSheetController import SpreadsheetController
from helpers.spreadsheet_controller import SpreadsheetController as Sheet
# ───────────────────────────────────────────────────────────
# 1. Load env vars from .env (only once, near program start)
# ───────────────────────────────────────────────────────────
load_dotenv()                      # reads .env into the process

USERNAME = os.getenv("BO_USERNAME", "")
PASSWORD = os.getenv("BO_PASSWORD","")
SPREADSHEET=os.getenv("SOCIALMEDIA_SHEET", "")

if not USERNAME or not PASSWORD or not SPREADSHEET:
    raise RuntimeError("BO_USERNAME / BO_PASSWORD / SPREADSHEET missing in environment")

# ───────────────────────────────────────────────────────────
# 2. fetching data on gsheet
# BAJI A1:A
# SIX6S B1:B
# JEETBUZZ C1:C
# CITINOW D1:D
# ───────────────────────────────────────────────────────────

# tasked = ["BAJI", "SIX6S", "JEETBUZZ", "CITINOW"]
yesterday = (datetime.now() - timedelta(days=1)).date()
targetdate = yesterday.strftime("%Y/%m/%d")  # → "2025/07/13"
sheetdate = yesterday.strftime("%d/%m/%Y")  # → "2025/07/13"

# ------------------------------------------------------------------
# 1.  Map each brand to the exact range it lives in
#     (sheet‑tab followed by cell range)
# ------------------------------------------------------------------
RANGES = {
    "BAJI"     : "SocialMedia!A1:A",
    "SIX6S"    : "SocialMedia!B1:B",
    "JEETBUZZ" : "SocialMedia!C1:C",
    "CITINOW"  : "SocialMedia!D1:D",
}

for brand, sheet_range in RANGES.items():
    print(f"Fetching keywords for brand: {brand}")
    
    keys = SpreadsheetController(SPREADSHEET, sheet_range)
    acq = AcquisitionController(email=USERNAME, password=PASSWORD, 
                                currency='all', currency_type=-1, 
                                # keyword=['richadspkr','adcashpkr','trafficnompkr','daoadpkr'], 
                                brand=brand,
                                targetdate=targetdate)  # Example date format

    try:
        # Read keywords from the spreadsheet
        keywords = keys.get_keywords()
        # print(keywords)
        BRAND = keywords[0] if keywords else "BAJI"
        DESTINATIONSHEET = keywords[1] if len(keywords) > 1 else SPREADSHEET
        if not keywords:
            raise ValueError("No keywords found in the spreadsheet.")
        
        print("Keywords fetched successfully:")

        result = acq.fetch_bo_batched(keywords,targetdate, batch_size=5)
        print("Total rows:", result["total"])
        rows_dict = result["data"]                     # ← trimmed dicts

        # 3. lay them out in the column order used by the sheet
        sheet_rows = [
            [
                sheetdate,
                "",
                r["affiliate_username"],
                r["currency"],
                r["username"],
                r["total_deposit"],
                r["total_withdrawal"],
                r["total_number_of_bets"],
                r["total_turnover"],
                r["total_profit_and_loss"],
                r["total_bonus"],
            ]
            for r in rows_dict
        ]

        # 4. append in one request
        sheet = Sheet(spreadsheet=DESTINATIONSHEET, tab="*Daily_Data (Player)")
        last_row = sheet.append_rows_return_last(sheet_rows)
        print(f"✓ Wrote {len(sheet_rows)} rows.  Last occupied row is now {last_row}.")

        # # 2) Pick a path (defaults to project root)
        # out_file = Path("bo_response.json")

        # # 3) Write prettified JSON
        # with out_file.open("w", encoding="utf-8") as f:
        #     json.dump(result, f, indent=2, ensure_ascii=False)

        # print(f"Saved → {out_file.resolve()}")   # e.g. /home/user/project/bo_response.json

    except Exception as exc:
        print(f"[ERROR] Could not fetch BO data: {exc}")

    print(f"Finished processing brand: {brand}\n")