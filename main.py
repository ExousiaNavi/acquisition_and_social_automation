"""
main.py – run back‑office fetch from AcquisitionController
"""
import os
import json
from pathlib import Path
from dotenv import load_dotenv
from controllers.AcquisitionController import AcquisitionController
from controllers.SpreadSheetController import SpreadsheetController

# ───────────────────────────────────────────────────────────
# 1. Load env vars from .env (only once, near program start)
# ───────────────────────────────────────────────────────────
load_dotenv()                      # reads .env into the process

USERNAME = os.getenv("BO_USERNAME", "")
PASSWORD = os.getenv("BO_PASSWORD","")
SPREADSHEET=os.getenv("ACQUISATION_SHEET", "")

if not USERNAME or not PASSWORD or not SPREADSHEET:
    raise RuntimeError("BO_USERNAME / BO_PASSWORD / SPREADSHEET missing in environment")

# ───────────────────────────────────────────────────────────
# 2. fetching data on gsheet
# ───────────────────────────────────────────────────────────
keys = SpreadsheetController(SPREADSHEET, "BAJI!A3:A")

acq = AcquisitionController(email=USERNAME, password=PASSWORD, 
                            currency='all', currency_type=-1, 
                            # keyword=['richadspkr','adcashpkr','trafficnompkr','daoadpkr'], 
                            brand='BAJI',
                            targetdate='2025/07/10')  # Example date format

try:
    # Read keywords from the spreadsheet
    keywords = keys.get_keywords()
    if not keywords:
        raise ValueError("No keywords found in the spreadsheet.")
    
    print("Keywords fetched successfully:")

    result = acq.fetch_bo_batched(keywords,"2025/07/10", batch_size=5)
    print("Total rows:", result["total"])
    # 2) Pick a path (defaults to project root)
    out_file = Path("bo_response.json")

    # 3) Write prettified JSON
    with out_file.open("w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"Saved → {out_file.resolve()}")   # e.g. /home/user/project/bo_response.json

except Exception as exc:
    print(f"[ERROR] Could not fetch BO data: {exc}")
