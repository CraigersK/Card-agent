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
                return colmap[n]
        return None

    year = _safe_str(row.get(col("year")))
    setname = _safe_str(row.get(col("set", "product", "brand")))
    player = _safe_str(row.get(col("player", "name")))
    cardno = _safe_str(row.get(col("card number", "card#", "#", "number")))
    desc = _safe_str(row.get(col("description", "card", "title")))

    grade = _clean_grade(row.get(col("grade")))

    parts = []
    if year:
        parts.append(year)
    if setname:
        parts.append(setname)
    if player:
        parts.append(player)
    if cardno:
        parts.append(f"#{cardno.lstrip('#')}")
    if not parts and desc:
        parts.append(desc)
    if grade:
        parts.append(f"PSA {grade}")

    if not parts:
        for v in row.values:
            s = _safe_str(v)
            if s:
                parts.append(s)
                break

    return " ".join(parts)


def _parse_price(text: str) -> Optional[float]:
    if not text:
        return None
    text = text.replace(",", "")
    m = re.search(r"(\d+(?:\.\d+)?)", text)
    return float(m.group(1)) if m else None


def _parse_date(text: str) -> Optional[datetime]:
    try:
        dt = dateparser.parse(text, fuzzy=True)
        if dt is None:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _extract_sales(page) -> List[Tuple[Optional[datetime], Optional[float]]]:
    rows = page.locator("table tbody tr")
    sales = []

    for i in range(rows.count()):
        try:
            txt = rows.nth(i).inner_text(timeout=2000)
        except Exception:
            continue

        date = _parse_date(txt)

        price = None
        m = re.search(r"\$\s*([\d,]+(?:\.\d+)?)", txt)
        if m:
            price = _parse_price(m.group(1))
        else:
            price = _parse_price(txt)

        if price is not None:
            sales.append((date, price))

    return sales


def _avg_90d(sales):
    cutoff = NOW_UTC() - timedelta(days=90)
    prices = [p for d, p in sales if d and d >= cutoff]

    if not prices:
        return None, 0, "No sold comps in last 90 days."

    avg = round(sum(prices) / len(prices), 2)
    return avg, len(prices), ""


def _price_query(query: str):
    url = SALES_URL.format(query=re.sub(r"\s+", "+", query))

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=30000)

            try:
                page.wait_for_selector("table tbody tr", timeout=8000)
            except PlaywrightTimeoutError:
                browser.close()
                return None, 0, "No results found.", url

            sales = _extract_sales(page)
            avg, comps, notes = _avg_90d(sales)

            browser.close()
            return avg, comps, notes, url

    except Exception as e:
        return None, 0, f"Error: {e}", url


@app.post("/price/spreadsheet")
async def price_spreadsheet(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Upload an .xlsx file.")

    raw = await file.read()
    df = pd.read_excel(BytesIO(raw))

    colmap = {str(c).strip().lower(): c for c in df.columns}

    for c in [
        "Avg Sold Price (90d, USD)",
        "# Sold Comps (90d)",
        "Source",
        "Query Used",
        "Notes",
    ]:
        if c not in df.columns:
            df[c] = ""

    for idx in range(len(df)):
        query = _build_query_from_row(df.iloc[idx], colmap)
        avg, comps, notes, url = _price_query(query)

        df.at[idx, "Avg Sold Price (90d, USD)"] = avg or ""
        df.at[idx, "# Sold Comps (90d)"] = comps
        df.at[idx, "Source"] = SOURCE_NAME
        df.at[idx, "Query Used"] = url
        df.at[idx, "Notes"] = notes

    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    out.seek(0)

    return StreamingResponse(
        out,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="priced_{file.filename}"'},
    )
