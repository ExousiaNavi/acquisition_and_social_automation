import requests
import os
import time
import re
import random
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
from config.config import Config
from datetime import datetime, timedelta, timezone
from typing import Optional

# Load environment variables
load_dotenv()

class SpreadsheetController:
    def __init__(self, spreadsheet, range=None):
        self.spreadsheet = spreadsheet
        self.range = range if range else "BAJI!A3:A"

    def get_keywords(self):
        print("Fetching accounts from spreadsheet...")
        config_dict = Config.as_dict()
        scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_info(config_dict, scopes=scope)
        try:
            service = build('sheets', 'v4', credentials=creds)
            sheet = service.spreadsheets()
            result = sheet.values().get(spreadsheetId=self.spreadsheet, range=self.range).execute()
            rows = result.get("values", [])# list[list[str]]
            # ────────── flatten and strip blanks ──────────
            keywords = [row[0].strip() for row in rows if row and row[0].strip()]
            return keywords    
            
        except HttpError as err:
            print(f"An error occurred: {err}")
            return []


