# helpers/by_player.py
from typing import List, Dict, Any

class ByPlayer:
    """
    Format acquisition rows (by player) so each row only contains
    the business‑friendly columns we care about.
    """

    # <raw API key> -> <friendly column name>
    DEFAULT_FIELD_MAP: Dict[str, str] = {
        "affiliateName":          "affiliate_username",
        "affiliateCurrency":      "currency",
        "player":                 "username",
        "deposit":                "total_deposit",
        "withdrawal":             "total_withdrawal",
        "betCount":               "total_number_of_bets",
        "turnover":               "total_turnover",
        "profit":                 "total_profit_and_loss",
        "bonus":                  "total_bonus",
    }

    def __init__(self, field_map: Dict[str, str] | None = None) -> None:
        self.field_map = field_map or self.DEFAULT_FIELD_MAP

    def filter_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        cleaned_rows: list[dict] = []
        for row in rows:
            cleaned_rows.append({
                pretty: row.get(raw)                # raw key comes from JSON
                for raw, pretty in self.field_map.items()
            })
        return cleaned_rows
