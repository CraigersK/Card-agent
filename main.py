from __future__ import annotations

import re
from io import BytesIO
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Tuple, Dict

import pandas as pd
from dateutil import parser as dateparser
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


APP_TITLE = "Card Pricing Agent"
SOURCE_NAME = "130point (eBay sold search)"
SALES_URL = "https://130point.com/sales/?q={query}"

NOW_UTC = lambda: datetime.now(timezone.utc)

app = FastAPI(title=APP_TITLE, version="1.0.0")


@app.get("/health")
def health():
    return {"status": "ok"}


def _safe_str(x) -> str:
    if x is None:
        return ""
    return str(x).strip()


def _clean_grade(val) -> Optional[str]:
    if val is None:
        return None
    m = re.search(r"(\d{1,2})", str(val))
    return m.group(1) if m else None


def _build_query_from_row(row: pd.Series, colmap: Dict[str, str]) -> str:
    def col(*names):
        for n in names:
            if n in colmap:
                return colmap
