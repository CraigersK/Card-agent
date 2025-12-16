from __future__ import annotations

import re
from io import BytesIO
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Tuple

import pandas as pd
from dateutil import parser as dateparser
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

APP_TITLE = "Card Pricing Agent"
SOURCE_NAME = "130point (eBay sold search)"
SALES_URL = "https://130point.com/sales/?q={query}"

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

app = FastAPI(title=APP_TITLE, version="1.0.1")

@app.get("/health")
def health():
    return {"status": "ok"}

def clean_grade(val) -> Optional[str]:
    if val is None:
        return None
    m = re.search(r"(\d{1,2})", str(val))
    return m.group(1) if m else None

def normalize_item_text(item: str) -> str:
    # Your Item looks like: "#109 ISIAH THOMAS | 1986 FLEER"
    # Replace pipes with spaces to help search.
    s = (item or "").strip()
    s = s.replace("|", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def build_query(item: str, grade: Optional[str]) -> str:
    base = normalize_item_text(item)
    if grade:
        if base:
            return f"{base} PSA {grade}"
        return f"PSA {grade}"
    return base

def parse_price(text: str) -> Optional[float]:
    if not text:
        return None
    text = text.replace(",", "")
    m = re.search(r"\$\s*([\d]+(?:\.\d+)?)", text)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None

def parse_date(text: str) -> Optional[datetime]:
    try:
        dt = dateparser.parse(text, fuzzy=True)
        if dt is None:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

async def extract_sales(page) -> List[Tuple[Optional[datetime], Optional[float]]]:
    rows = page.locator("table tbody tr")
    sales: List[Tuple[Optional[datetime], Optional[float]]] = []

    n = await rows.count()
    for i in range(n):
        try:
            txt = await rows.nth(i).inner_text(timeout=2000)
        except Exception:
            continue

        dt = parse_date(txt)
        price = parse_price(txt)
        if price is not None:
            sales.append((dt, price))

    return sales

def avg_90d(sales: List[Tuple[Optional[datetime], Optional[float]]]):
    cutoff = now_utc() - timedelta(days=90)
    prices = [p for d, p in sales if d is not None and d >= cutoff and p is not None]
    if not prices:
        return None, 0, "No sold comps with dates in last 90 days."
    return round(sum(prices) / len(prices), 2), len(prices), ""

async def price_query(query: str):
    q = query.strip()
    if not q:
        return None, 0, "Empty query.", ""

    url = SALES_URL.format(query=re.sub(r"\s+", "+", q))

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)

            try:
                await page.wait_for_selector("table tbody tr", timeout=8000)
            except PlaywrightTimeoutError:
                await browser.close()
                return None, 0, "No results table found (0 comps or page layout changed).", url

            sales = await extract_sales(page)
            avg, comps, notes = avg_90d(sales)

            await browser.close()
            return avg, comps, notes, url

    except Exception as e:
        return None, 0, f"Error pricing query: {e}", url

@app.post("/price/spreadsheet")
async def price_spreadsheet(file: UploadFile = File(...)):
    if not (file.filename or "").lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Please upload an .xlsx file.")

    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Empty file.")

    df = pd.read_excel(BytesIO(raw))
    if df.empty:
        raise HTTPException(status_code=400, detail="Spreadsheet has no rows.")

    # Output columns
    out_cols = [
        "Avg Sold Price (90d, USD)",
        "# Sold Comps (90d)",
        "Source",
        "Query Used",
        "Notes",
    ]
    for c in out_cols:
        if c not in df.columns:
            df[c] = ""

    # Expect columns Item + Grade
    if "Item" not in df.columns:
        raise HTTPException(status_code=400, detail="Expected an 'Item' column in the sheet.")

    for idx in range(len(df)):
        item = str(df.at[idx, "Item"]) if "Item" in df.columns else ""
        grade = clean_grade(df.at[idx, "Grade"]) if "Grade" in df.columns else None

        q = build_query(item, grade)
        avg, comps, notes, url = await price_query(q)

        df.at[idx, "Avg Sold Price (90d, USD)"] = "" if avg is None else avg
        df.at[idx, "# Sold Comps (90d)"] = int(comps)
        df.at[idx, "Source"] = SOURCE_NAME
        df.at[idx, "Query Used"] = url or q
        df.at[idx, "Notes"] = notes

    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    out.seek(0)

    out_name = "priced_" + (file.filename or "cards.xlsx")
    return StreamingResponse(
        out,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{out_name}"'},
    )
