# spreadsheet_controller.py
from typing import List, Any
import re
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
from config.config import Config

# Load environment variables
load_dotenv()

class SpreadsheetController:
    """
    Small wrapper around the Google Sheets API that:

    • accepts EITHER a full Sheets URL OR the bare spreadsheet ID  
    • can tell you the first empty row (rarely needed)  
    • can append many rows in one call and return the final row index
    """

    # Regex to pull “…/d/<ID>/…” from any Sheets/Docs URL
    _ID_REGEX = re.compile(r"/d/([a-zA-Z0-9-_]+)")

    # ───────────────────────────────────────────────────────────
    # Constructor
    # ───────────────────────────────────────────────────────────
    def __init__(self, *, spreadsheet: str, tab: str):
        """
        Parameters
        ----------
        spreadsheet : str
            Spreadsheet ID OR the full link (the class extracts the ID).
        tab : str
            Worksheet/tab name (e.g. "BAJI").
        """
        self.spreadsheet_id = self._extract_id(spreadsheet)
        self.tab = tab
        config_dict = Config.as_dict()
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(config_dict, scopes=scope)
        self.svc = build("sheets", "v4", credentials=creds)

    # ───────────────────────────────────────────────────────────
    # Private helper
    # ───────────────────────────────────────────────────────────
    @classmethod
    def _extract_id(cls, link_or_id: str) -> str:
        """Return the spreadsheet ID whether given a link or just the ID."""
        if "/" not in link_or_id:                 # looks like bare ID
            return link_or_id.strip()
        match = cls._ID_REGEX.search(link_or_id)
        if not match:
            raise ValueError("Could not parse spreadsheet ID from URL.")
        return match.group(1)

    # ───────────────────────────────────────────────────────────
    # A. Find the first empty row (1‑based index)
    # ───────────────────────────────────────────────────────────
    def next_empty_row(self) -> int:
        """
        Return the index of the first blank row in the tab.
        (Usually unnecessary because append handles placement.)
        """
        try:
            # Cheap read on a single column
            result = (
                self.svc.spreadsheets()
                .values()
                .get(spreadsheetId=self.spreadsheet_id, range=f"{self.tab}!A:A")
                .execute()
            )
            last_row = len(result.get("values", []))
        except HttpError:
            last_row = 0

        return last_row + 1

    # ───────────────────────────────────────────────────────────
    # B. Append rows and return the LAST occupied row index
    #    – single API request, no overwrite risk
    # ───────────────────────────────────────────────────────────
    def append_rows_return_last(
        self,
        rows: List[List[Any]],
        start_cell: str = "A1",
        value_input_option: str = "USER_ENTERED",
    ) -> int:
        """
        Append `rows` below existing data and return the new last‑row index.

        Parameters
        ----------
        rows : List[List[Any]]
            Already‑ordered list of rows to write.
        start_cell : str
            Any cell inside the table (Sheets finds the actual bottom).
        value_input_option : str
            "USER_ENTERED" lets Sheets auto‑detect numbers/dates.
        """
        if not rows:
            raise ValueError("Nothing to write")

        try:
            resp = (
                self.svc.spreadsheets()
                .values()
                .append(
                    spreadsheetId=self.spreadsheet_id,
                    range=f"{self.tab}!{start_cell}",
                    valueInputOption=value_input_option,
                    insertDataOption="INSERT_ROWS",      # guarantees append
                    includeValuesInResponse=False,
                    body={"values": rows},
                )
                .execute()
            )
        except HttpError as err:
            raise RuntimeError(f"Sheets append error: {err}") from err

        # Parse e.g. "BAJI!A45:H52" → 52
        updated_range = resp["updates"]["updatedRange"]
        last_row_after = int(re.search(r"[A-Z]+(\d+):", updated_range).group(1))
        return last_row_after
