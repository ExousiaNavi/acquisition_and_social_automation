import math
import hashlib
import logging
from typing import List

import requests
from bs4 import BeautifulSoup
from helpers.byPlayer import ByPlayer

class AcquisitionController:
    _links = {
        "BAJI": [
            "https://bjabo8888.com/page/manager/login.jsp",
            "https://bjabo8888.com/login/manager/managerController/login",
            "https://bjabo8888.com/manager/AffiliateController/searchPerformancePlayerReport",
        ],
        "SIX6S": [
            "https://666666bo.com/page/manager/login.jsp",
            "https://666666bo.com/login/manager/managerController/login",
            "https://666666bo.com/manager/AffiliateController/searchPerformancePlayerReport",
        ],
        "JEETBUZZ": [
            "https://jbbo8888.com/page/manager/login.jsp",
            "https://jbbo8888.com/login/manager/managerController/login",
            "https://jbbo8888.com/manager/AffiliateController/searchPerformancePlayerReport",
        ],
        "CITINOW": [
            "https://ctncps.com/page/manager/login.jsp",
            "https://ctncps.com/login/manager/managerController/login",
            "https://ctncps.com/manager/AffiliateController/searchPerformancePlayerReport",
        ],
    }

    def __init__(
        self,
        email: str,
        password: str,
        currency: str,
        currency_type: int,
        brand: str,
        targetdate: str,
        max_retries: int = 3,
    ):
        self.email = email
        self.password = password
        self.currency = currency
        self.brand = brand
        self.targetdate = targetdate
        self._currency_type = currency_type
        self.max_retries = max_retries

        self.session = requests.Session()
        self.cookies = None  # populated after _authenticate()

    # ────────────────────────────────────────────────────────────
    # Public helper: fetch every keyword in batches of five
    # ────────────────────────────────────────────────────────────
    def fetch_bo_batched(self, keywords: List[str], targetdate:str, batch_size: int = 5, page_size: int = 100):
        """
        Fetch BO data for an arbitrary keyword list, five at a time.
        Returns: dict(status, text, data=[…], total=int)
        """
        if not keywords:
            return {"status": 400, "text": "No keywords provided.", "data": [], "total": 0}

        auth_ok = self._authenticate()
        if not auth_ok:
            return {"status": 401, "text": "Authentication failed."}

        urls = self._links[self.brand]
        endpoint = urls[2]

        all_rows = []
        # ---- iterate batches of N keywords ---------------------------------
        keywords_to_use = keywords[2:]          # everything from index 2 onward
        for start in range(0, len(keywords_to_use), batch_size):
            batch = keywords[start : start + batch_size]
            # ---- page through 100‑row blocks until exhausted -------------
            page = 1
            while True:
                params = {
                    "resultBy": "",
                    "visibleColumns": "",
                    "currencyType": self._currency_type,      # -1 == all
                    "searchStatus": -99,
                    "userId": ",".join(batch),                # ← comma‑sep
                    "affiliateInternalType": -1,
                    "searchTimeStart": targetdate,
                    "searchTimeEnd": targetdate,
                    "pageNumber": page,
                    "pageSize": page_size,                    # BO hard‑limit
                    "sortCondition": 14,
                    "sortName": "turnover",
                    "sortOrder": 1,
                    "searchText": "",
                }

                resp = self.session.get(endpoint, params=params, cookies=self.cookies)
                if resp.status_code != 200:
                    logging.error("Data fetch failed (batch %s, page %s): %s",
                                  batch, page, resp.status_code)
                    break

                payload = resp.json()
                all_rows.extend(payload.get("aaData", []))

                # stop when less than full page returned
                if len(payload.get("aaData", [])) < page_size:
                    break
                page += 1

        # ---- end of batches -----------------------------------------------
        byPlayer = ByPlayer()  # Import the helper class
        # 🔄  Delegate filtering/renaming to the helper
        filtered_rows = byPlayer.filter_rows(all_rows)

        return {
            "status": 200,
            "text": "Data fetched and filtered successfully.",
            "data": filtered_rows,
            "total": len(filtered_rows),
        }

    # ────────────────────────────────────────────────────────────
    # Internal: sign‑in once per controller instance
    # ────────────────────────────────────────────────────────────
    def _authenticate(self) -> bool:
        """Login + capture cookies. Returns True on success."""
        urls = self._links.get(self.brand)
        if not urls:
            logging.error("Brand links not found.")
            return False

        # 1) GET login page
        resp = self.session.get(urls[0])
        if resp.status_code != 200:
            logging.error("Failed to load login page: %s", resp.status_code)
            return False

        # 2) Scrape randomCode
        soup = BeautifulSoup(resp.text, "html.parser")
        random_tag = soup.find("input", {"id": "randomCode"})
        if not random_tag:
            logging.error("randomCode input not found on login page.")
            return False

        random_code_val = random_tag.get("value")

        # 3) POST credentials
        auth_payload = {
            "username": self.email,
            "password": hashlib.sha1(self.password.encode()).hexdigest(),
            "randomCode": random_code_val,
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Accept": "*/*",
        }

        login = self.session.post(urls[1], data=auth_payload, headers=headers)
        if login.status_code != 200:
            logging.error("Login failed: %s", login.status_code)
            return False

        self.cookies = self.session.cookies.get_dict()
        logging.info("Authentication successful.")
        return True
