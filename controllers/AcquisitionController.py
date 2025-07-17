import math
import hashlib
import logging, hashlib, traceback
from typing import List
import time
import requests
import re, hashlib, traceback, time, requests, logging
from bs4 import BeautifulSoup
from helpers.byPlayer import ByPlayer
from helpers.byAffiliate import ByAffiliate
from helpers.byAffiliateSocialMedia import ByAffiliateSocialMedia
from api.getRequest import BoDataAPI

class AcquisitionController:
    _socmedlinks = {
        "BAJI": [
            "https://bjabo8888.com/page/manager/login.jsp",
            "https://bjabo8888.com/login/manager/managerController/login",
            "https://bjabo8888.com/manager/AffiliateController/searchPerformancePlayerReport",
            "https://bjabo8888.com/manager/AffiliateController/searchPerformanceAffiliateReport",
        ],
        "SIX6S": [
            "https://666666bo.com/page/manager/login.jsp",
            "https://666666bo.com/login/manager/managerController/login",
            "https://666666bo.com/manager/AffiliateController/searchPerformancePlayerReport",
            "https://666666bo.com/manager/AffiliateController/searchPerformanceAffiliateReport",
        ],
        "JEETBUZZ": [
            "https://jbbo8888.com/page/manager/login.jsp",
            "https://jbbo8888.com/login/manager/managerController/login",
            "https://jbbo8888.com/manager/AffiliateController/searchPerformancePlayerReport",
            "https://jbbo8888.com/manager/AffiliateController/searchPerformanceAffiliateReport",
        ],
        "CITINOW": [
            "https://ctncps.com/page/manager/login.jsp",
            "https://ctncps.com/login/manager/managerController/login",
            "https://ctncps.com/manager/AffiliateController/searchPerformancePlayerReport",
            "https://ctncps.com/manager/AffiliateController/searchPerformanceAffiliateReport",
        ],
    }
    _afflinks = {
        "BAJI": [
            "https://bjabo8888.com/page/manager/login.jsp",
            "https://bjabo8888.com/login/manager/managerController/login",
            "https://bjabo8888.com/manager/AffiliateController/searchPerformanceAffiliateReport",
        ],
        "SIX6S": [
            "https://666666bo.com/page/manager/login.jsp",
            "https://666666bo.com/login/manager/managerController/login",
            "https://666666bo.com/manager/AffiliateController/searchPerformanceAffiliateReport",
        ],
        "JEETBUZZ": [
            "https://jbbo8888.com/page/manager/login.jsp",
            "https://jbbo8888.com/login/manager/managerController/login",
            "https://jbbo8888.com/manager/AffiliateController/searchPerformanceAffiliateReport",
        ],
        "CITINOW": [
            "https://ctncps.com/page/manager/login.jsp",
            "https://ctncps.com/login/manager/managerController/login",
            "https://ctncps.com/manager/AffiliateController/searchPerformanceAffiliateReport",
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
    def fetch_bo_batched(self, type: str, keywords: List[str], targetdate:str, batch_size: int = 5, page_size: int = 100):
        """
        Fetch BO data for an arbitrary keyword list, five at a time.
        Returns: dict(status, text, data=[…], total=int)
        """
        if not keywords:
            return {"status": 400, "text": "No keywords provided.", "data": [], "total": 0}

        auth_ok = self._authenticate(type)
        if not auth_ok:
            return {"status": 401, "text": "Authentication failed."}
        
        link_dict = self._socmedlinks if type == "SocialMedia" else self._afflinks
        urls = link_dict.get(self.brand)

        if not urls or len(urls) < 2:
            raise ValueError(f"No login URLs found for brand: {self.brand}")
        
        # urls = self._socmedlinks if type == "SocialMedia" else self._afflinks
        # if not urls.get(self.brand):
        #     return {"status": 404, "text": f"Brand {self.brand} not found."}
        
        # urls.get(self.brand)
        # endpoint = urls[2]

        # print("Starting data fetch for type:", type)
        # all_rows = []
        # # ---- iterate batches of N keywords ---------------------------------
        # keywords_to_use = keywords[2:]          # everything from index 2 onward
        # for start in range(0, len(keywords_to_use), batch_size):
        #     batch = keywords[start : start + batch_size]
        #     # ---- page through 100‑row blocks until exhausted -------------
        #     page = 1
        #     while True:
        #         params = {
        #             "resultBy": "" if type == "SocialMedia" else 1,
        #             "visibleColumns": "",
        #             "currencyType": self._currency_type,      # -1 == all
        #             "searchStatus": -99,
        #             "userId": ",".join(batch),                # ← comma‑sep
        #             "affiliateInternalType": -1,
        #             "searchTimeStart": targetdate,
        #             "searchTimeEnd": targetdate,
        #             "pageNumber": page,
        #             "pageSize": page_size,                    # BO hard‑limit
        #             "sortCondition": 14,
        #             "sortName": "turnover",
        #             "sortOrder": 1,
        #             "searchText": "",
        #         }

        #         resp = self.session.get(endpoint, params=params, cookies=self.cookies)
        #         if resp.status_code != 200:
        #             logging.error("Data fetch failed (batch %s, page %s): %s",
        #                           batch, page, resp.status_code)
        #             break

        #         payload = resp.json()
        #         all_rows.extend(payload.get("aaData", []))

        #         # stop when less than full page returned
        #         if len(payload.get("aaData", [])) < page_size:
        #             break
        #         page += 1

        print("Initialized API for fetching data...")
        api = BoDataAPI(session=self.session,cookies=self.cookies,currency_type=self._currency_type, page_size=page_size)
        print("-------------------------------------------------------------------------------------")
        print("Initialized API for fetching data completed...")
        print("-------------------------------------------------------------------------------------")
        # ---- end of batches ----------------------------------------------- 
        # 🔄  Delegate filtering/renaming to the helper
        print("Identifying processing type:", type)
        if type == "SocialMedia":
            print("Collecting SocialMedia data for player...")
            all_rows_socmed_player = api.fetch(endpoint=urls[2],data_type="SocialMedia", keywords=keywords,target_date=targetdate, batch_size=batch_size)
            byPlayer = ByPlayer()   # Import the helper class
            filtered_rows = byPlayer.filter_rows(all_rows_socmed_player)
            print("-------------------------------------------------------------------------------------")
            print("Collecting SocialMedia data for player completed...")
            print("-------------------------------------------------------------------------------------")
            print("Collecting SocialMedia data for affiliates...")
            all_rows_socmend_aff = api.fetch(endpoint=urls[3],data_type="Affiliates", keywords=keywords,target_date=targetdate, batch_size=batch_size)
            byAffliateSocmed = ByAffiliateSocialMedia()  # Import the helper class
            filtered_rows_socmed = byAffliateSocmed.filter_rows(all_rows_socmend_aff)
            print("-------------------------------------------------------------------------------------")
            print("Collecting SocialMedia data for affiliates completed...")
            print("-------------------------------------------------------------------------------------")
        else:
            print("Collecting Affiliate data...")
            all_rows_aff = api.fetch(endpoint=urls[2],data_type="Affiliates", keywords=keywords,target_date=targetdate, batch_size=batch_size)
            byAffliate = ByAffiliate()  # Import the helper class
            filtered_rows = byAffliate.filter_rows(all_rows_aff)    
            print("-------------------------------------------------------------------------------------")
            print("Collecting Affiliate data completed...")
            print("-------------------------------------------------------------------------------------")
        print("Fetching Completed.......:", type)
        print("-------------------------------------------------------------------------------------")
        return {
            "status": 200,
            "text": "Data fetched and filtered successfully.",
            "data": filtered_rows,
            "data_socmed": filtered_rows_socmed if type == "SocialMedia" else [],
            "total": len(filtered_rows),
        }

    # ────────────────────────────────────────────────────────────
    # Internal: sign‑in once per controller instance
    # ────────────────────────────────────────────────────────────
    def _authenticate(self, type) -> bool:
        """
        Login + capture cookies.
        Returns True on success, False on ANY failure,
        while printing the full error/traceback.
        """
        print("Authenticating user")          # keep your console cue
        try:
            # -------- 0) Choose brand‑specific URLs --------
            link_dict = self._socmedlinks if type == "SocialMedia" else self._afflinks
            urls = link_dict.get(self.brand)

            if not urls or len(urls) < 2:
                raise ValueError(f"No login URLs found for brand: {self.brand}")

            print(f"Using URLs: {urls[0]} and {urls[1]}")
            # -------- 1) GET login page --------
            self.session.cookies.clear()  # <- force fresh login
            resp = self.session.get(urls[0], timeout=10)
            resp.raise_for_status()            # throws for 4xx / 5xx
            # time.sleep(3)  # avoid hammering the server
            # -------- 2) Scrape randomCode --------
            soup = BeautifulSoup(resp.text, "html.parser")
            random_tag = soup.find("input", {"id": "randomCode"})
            if random_tag is None:
                raise RuntimeError("randomCode input not found on login page.")
            random_code_val = random_tag["value"]

            # -------- 3) POST credentials --------
            auth_payload = {
                "username": self.email,
                "password": hashlib.sha1(self.password.encode()).hexdigest(),
                "randomCode": random_code_val,
            }
            headers = {
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Accept": "*/*",
            }
            login = self.session.post(urls[1], data=auth_payload,
                                    headers=headers, timeout=10)
            login.raise_for_status()

            # -------- 4) Success --------
            self.cookies = self.session.cookies.get_dict()
            print("Authentication successful.")
            return True

        except (requests.RequestException, Exception) as e:
            # Print full traceback so you immediately see *where* it failed
            print("Authentication error:\n%s", traceback.format_exc())
            return False
