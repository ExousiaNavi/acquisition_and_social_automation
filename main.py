"""
main.py — fetch SocialMedia first, then Affiliates (full‑field rows)
"""
import os, json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
from rich.console import Console

from controllers.AcquisitionController import AcquisitionController
from controllers.SpreadSheetController import SpreadsheetController
from helpers.spreadsheet_controller import SpreadsheetController as Sheet

# ────────────────────────── ENV ──────────────────────────
load_dotenv()
USERNAME           = os.getenv("BO_USERNAME", "")
PASSWORD           = os.getenv("BO_PASSWORD", "")
SOCIAL_SHEET_ID    = os.getenv("SOCIALMEDIA_SHEET", "")
AFFILIATE_SHEET_ID = os.getenv("AFFILIATE_SHEET", "")

if not all([USERNAME, PASSWORD, SOCIAL_SHEET_ID, AFFILIATE_SHEET_ID]):
    raise RuntimeError("Missing BO_USERNAME / BO_PASSWORD / SOCIALMEDIA_SHEET / AFFILIATE_SHEET")

# ────────────────────── CONSTANTS ────────────────────────
yday        = (datetime.now() - timedelta(days=1)).date()
target_date = yday.strftime("%Y/%m/%d")   # 2025/07/14
sheet_date  = yday.strftime("%d/%m/%Y")   # 14/07/2025

SOCIAL_RANGES = {
    "BAJI": "SocialMedia!A1:A",
    "SIX6S": "SocialMedia!B1:B",
    "JEETBUZZ": "SocialMedia!C1:C",
    "CITINOW": "SocialMedia!D1:D",
}
AFFILIATE_RANGES = {
    "BAJI": "Acquisition!A1:A",
    "SIX6S": "Acquisition!B1:B",
    "JEETBUZZ": "Acquisition!C1:C",
    "CITINOW": "Acquisition!D1:D",
}

# ────────────────────── HELPERS ──────────────────────────
def build_social_row(rec):
    return [
        sheet_date, "", rec["affiliate_username"], rec["currency"],
        rec["player_username"], rec["total_deposit"], rec["total_withdrawal"],
        rec["total_number_of_bets"], rec["total_turnover"],
        rec["total_profit_and_loss"], rec["total_bonus"]
    ]

def build_affiliate_row(rec):
    return [
        sheet_date, "", rec["affiliate_username"], rec["currency"],
        rec["registered_users"], rec["number_of_fd"], rec["first_deposit"],
        rec["active_player"], rec["total_deposit"],
        rec.get("total_withdrawal", ""), rec.get("total_turnover", ""),
        rec.get("total_profit_and_loss", ""), rec.get("total_bonus", ""),
    ]

def build_affiliate_row_socmed(rec):
    return [
        sheet_date, "", rec["affiliate_username"], rec["currency"],
        rec["registered_users"], rec["number_of_fd"], rec["first_deposit"],
        rec["active_player"]
    ]

console = Console(force_terminal=True,
                  force_interactive=True,
                  color_system="truecolor")


def fetch_dual(type, ac: AcquisitionController, kw, target_date, batch=3):
    """Return both Affiliates and SocialMedia data in one dict."""
    if type == "Affiliates":
        aff_rows = ac.fetch_bo_batched("Affiliates",  kw, target_date, batch)["data"]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
        return {"data": aff_rows, "socmed_data": []}
    else:
        aff_rows = ac.fetch_bo_batched("SocialMedia",  kw, target_date, batch)["data"]
        soc_rows = ac.fetch_bo_batched("SocialMedia", kw, target_date, batch)["data_socmed"]
        return {"data": aff_rows, "socmed_data": soc_rows}

def process_sheet(sheet_id, ranges, row_builder,row_builder_socmed, type, fixed_tab=None):
    # brand_total = len(ranges)

    with Progress(
        SpinnerColumn(style="green"),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(
            bar_width=40,
            complete_style="bright_cyan",
            finished_style="bright_green",
            pulse_style="dim",
        ),
        TextColumn("{task.completed}/{task.total}", style="bold"),
        console=console,
        transient=False,  # Keep bar after completion
    ) as progress:

        task = progress.add_task(f"[white]Processing {type}", total=len(ranges))

        for brand, rng in ranges.items():
            try:
                progress.update(task, description=f"[cyan]{type}: {brand}")
                
                kw = SpreadsheetController(sheet_id, rng).get_keywords()
                if not kw:
                    console.log(f"[yellow]{brand}: no keywords")
                    progress.advance(task)
                    continue

                dest_sheet = kw[1] if len(kw) > 1 else sheet_id
                tab_name   = fixed_tab or brand
                print(f"fixed_tab: {fixed_tab}, dest_sheet: {dest_sheet}, TabName: {tab_name}")

                data = AcquisitionController(
                    email=USERNAME, password=PASSWORD,
                    currency="all", currency_type=-1,
                    brand=brand, targetdate=target_date
                )
                # .fetch_bo_batched(type, kw, target_date, batch_size=5)["data"]
                out  = fetch_dual(type, data, kw, target_date)
                data_aff   = out["data"]          # byPlayer and Affiliates
                data_soc   = out["socmed_data"]   # socmed affiliates

                # print(data_aff)
                print("Writing rows to spreadsheet…................................")
                rows = [row_builder(r) for r in data_aff]
                sheet = Sheet(spreadsheet=dest_sheet, tab=tab_name, type=type)
                sheet.append_rows_return_last(rows, debug=True)
                print("Done writing rows to spreadsheet.............................")
                # check if it has anything
                if not data_soc:             # True for [] or None
                    print("No Social‑Media rows found")
                else:

                    print(f"{len(data_soc)} Social‑Media rows")
                    rows2 = [row_builder_socmed(r) for r in data_soc]
                    sheet2 = Sheet(spreadsheet=dest_sheet, tab="*Daily_Data (Aff)", type=type)
                    sheet2.append_rows_return_last(rows2, debug=True)
                    
                console.log(f"[green]{brand}: {len(rows)} rows → {tab_name}")
                

            except Exception as e:
                console.log(f"[red]{brand} ERROR: {e}")

            progress.advance(task)  # <- This is what animates the bar



# 1️⃣  SocialMedia ➜ fixed tab "*Daily_Data (Player)"
process_sheet(
    SOCIAL_SHEET_ID, SOCIAL_RANGES, build_social_row, build_affiliate_row_socmed, "SocialMedia",
    fixed_tab="*Daily_Data (Player)")

# 2️⃣  Affiliates ➜ tab derived from range ("Affiliates")
#     i.e. "Affiliates!A1:A" → "Affiliates"
process_sheet(
    AFFILIATE_SHEET_ID, AFFILIATE_RANGES, build_affiliate_row, build_affiliate_row_socmed,  # fixed_tab=None (default behavior, no fixed tab)
    type="Affiliates"  # type is not used in this context, but kept for consistency
)