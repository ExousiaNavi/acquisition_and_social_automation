import logging
from typing import List, Dict, Any

class BoDataAPI:
    """
    Small helper that fetches *Affiliates* or *SocialMedia* rows from
    the BO endpoint in (keyword‑batch × page) blocks and returns a flat
    list.  Re‑uses the caller’s requests.Session so you keep cookies.
    """

    def __init__(
        self,
        session,
        # endpoint: str,
        cookies: Dict[str, str],
        currency_type: int = -1,
        page_size: int = 100,   # BO’s hard limit
    ):
        self.session       = session
        # self.endpoint      = endpoint
        self.cookies       = cookies
        self.currency_type = currency_type
        self.page_size     = page_size

    # ───────────────────────────────────────────────────────────
    # public
    # ───────────────────────────────────────────────────────────
    def fetch(
        self,
        *,
        endpoint: str,          # e.g. "https://bo.example.com/affiliate/data"
        data_type: str,            # "Affiliates" | "SocialMedia"
        keywords: List[str],       # whole sheet row: [brand, sheetId, kw1, …]
        target_date: str,          # "YYYY/MM/DD"
        batch_size: int = 5,
    ) -> List[Dict[str, Any]]:
        """
        Returns a flat list of dict rows for all keywords & pages.
        """
        logging.info("Starting data fetch – type=%s  kws=%s", data_type, len(keywords) - 2)

        all_rows: List[Dict[str, Any]] = []
        user_ids = keywords[2:]                    # skip brand & sheetId

        for start in range(0, len(user_ids), batch_size):
            batch = user_ids[start : start + batch_size]

            page = 1
            while True:
                rows = self._fetch_page(
                    endpoint=endpoint,
                    batch=batch,
                    data_type=data_type,
                    target_date=target_date,
                    page=page,
                )
                all_rows.extend(rows)

                if len(rows) < self.page_size:
                    break          # last page for this batch
                page += 1          # go get next page

        return all_rows

    # ───────────────────────────────────────────────────────────
    # private
    # ───────────────────────────────────────────────────────────
    def _fetch_page(
        self,
        *,
        endpoint: str,
        batch: List[str],
        data_type: str,
        target_date: str,
        page: int,
    ) -> List[Dict[str, Any]]:

        params = {
            "resultBy": "" if data_type == "SocialMedia" else 1,
            "visibleColumns": "",
            "currencyType": self.currency_type,
            "searchStatus": -99,
            "userId": ",".join(batch),
            "affiliateInternalType": -1,
            "searchTimeStart": target_date,
            "searchTimeEnd": target_date,
            "pageNumber": page,
            "pageSize": self.page_size,
            "sortCondition": 14,
            "sortName": "turnover",
            "sortOrder": 1,
            "searchText": "",
        }

        resp = self.session.get(endpoint, params=params, cookies=self.cookies)
        if resp.status_code != 200:
            logging.error("Fetch failed (batch=%s page=%s): %s", batch, page, resp.status_code)
            return []

        payload = resp.json()
        return payload.get("aaData", [])
