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


def _clean_grade(val: str) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    # accept "10", "PSA 10", etc -> keep just number
    m = re.search(r"(\d{1,2})", s)
    return m.group(1) if m else None


def _safe_str(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    return s


def _build_query_from_row(row: pd.Series, columns_lower: Dict[str, str]) -> str:
    """
    Build a query string for 130point from whatever columns exist.
    We try common names. If we can't find structured columns, fall back to the first non-empty text cell.
    Always append PSA + grade when available.
    """
    # common column name guesses
    def col(*names):
        for n in names:
            if n in columns_lower:
                return columns_lower[n]
        return None

    year_c = col("year")
    set_c = col("set", "product", "brand")
    player_c = col("player", "name")
    cardno_c = col("card number", "card #", "card#", "number", "#")
    desc_c = col("description", "card", "title")

    grade_c = col("grade")
    grader_c = col("grader", "company")

    year = _safe_str(row.get(year_c)) if year_c else ""
    setname = _safe_str(row.get(set_c)) if set_c else ""
    player = _safe_str(row.get(player_c)) if player_c else ""
    cardno = _safe_str(row.get(cardno_c)) if cardno_c else ""
    desc = _safe_str(row.get(desc_c)) if desc_c else ""

    grader = _safe_str(row.get(grader_c)) if grader_c else ""
    grade = _clean_grade(row.get(grade_c)) if grade_c else None

    # If grader exists and isn't PSA, we still price but we will bias query to PSA only if PSA
    # You said all are PSA, but this prevents accidental mixing.
    wants_psa = True
    if grader and "psa" not in grader.lower():
        wants_psa = False

    parts = []
    if year:
        parts.append(year)
    if setname:
        parts.append(setname)
    if player:
        parts.append(player)
    if cardno:
        # try to keep # in query
        if cardno.startswith("#"):
            parts.append(cardno)
        else:
            parts.append(f"#{cardno}")
    if not parts and desc:
        parts.append(desc)

    # Always include PSA + grade if we have grade (your requirement)
    if grade:
        if wants_psa:
            parts.append(f"PSA {grade}")
        else:
            parts.append(f"{grade}")  # fallback

    # If still empty, use anything we can find
    if not any(p.strip() for p in parts):
        # grab first non-empty string in the row
        for v in row.values:
            s = _safe_str(v)
            if s:
                parts.append(s)
                break

    return " ".join([p for p in parts if p.strip()])


def _parse_money(text: str) -> Optional[float]:
    if not text:
        return None
    # remove commas, keep digits+dot
    s = text.replace(",", "")
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def _parse_date(text: str) -> Optional[datetime]:
    if not text:
        return None
    try:
        dt = dateparser.parse(text, fuzzy=True)
        if not dt:
            return None
        # Assume UTC if no tz
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return None


def _extract_sales_from_130point(page) -> List[Tuple[Optional[datetime], Optional[float]]]:
    """
    Tries to extract (date, price) pairs from the results table.
    130point layout can change; we use resilient parsing by scanning table rows.
    """
    rows = page.locator("table tbody tr")
    n = rows.count()
    sales: List[Tuple[Optional[datetime], Optional[float]]] = []

    for i in range(n):
        row = rows.nth(i)
        try:
            txt = row.inner_text(timeout=2000)
        except Exception:
            continue

        # common row text contains a date and a price; we extract both loosely
        # date candidates: look for month/day/year or similar
        dt = _parse_date(txt)

        # price candidates: "$123.45" etc
        price = None
        # try find $-prefixed first
        m = re.search(r"\$\s*([\d,]+(?:\.\d+)?)", txt)
        if m:
            price = _parse_money(m.group(1))
        else:
            # fallback any number
            price = _parse_money(txt)

        if price is not None:
            sales.append((dt, price))

    return sales


def _avg_90d_from_sales(sales: List[Tuple[Optional[datetime], Optional[float]]]) -> Tuple[Optional[float], int, str]:
    """
    Returns (avg_price, comp_count, notes).
    Only uses entries with a parsed date within last 90 days.
    If no dated comps exist, returns (None, 0, reason).
    """
    cutoff = NOW_UTC() - timedelta(days=90)
    prices = []
    for dt, price in sales:
        if price is None:
            continue
        if dt is None:
            continue
        if dt >= cutoff:
            prices.append(price)

    if not prices:
        return None, 0, "No sold comps with dates in last 90 days (or date parsing failed)."

    avg = sum(prices) / float(len(prices))
    return round(avg, 2), len(prices), ""


def _price_one_query_playwright(query: str) -> Tuple[Optional[float], int, str, str]:
    """
    Returns (avg_90d, comps_90d, notes, url_used)
    """
    q = query.strip()
    if not q:
        return None, 0, "Empty query.", ""

    url = SALES_URL.format(query=re.sub(r"\s+", "+", q))
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()

            # 130point is usually straightforward; wait for table
            page.goto(url, wait_until="domcontentloaded", timeout=30000)

            # If results are slow, give it a moment
            page.wait_for_timeout(1500)

            # Ensure a table exists; if not, it may still load or query returned nothing
            # We’ll try a quick wait; if still none, return no comps
            try:
                page.wait_for_selector("table tbody tr", timeout=8000)
            except PlaywrightTimeoutError:
                browser.close()
                return None, 0, "No results table found (0 comps or page layout changed).", url

            sales = _extract_sales_from_130point(page)
            avg, comps, notes = _avg_90d_from_sales(sales)

            browser.close()
            return avg, comps, notes, url

    except Exception as e:
        return None, 0, f"Error pricing query: {e}", url


@app.post("/price/spreadsheet")
async def price_spreadsheet(file: UploadFile = File(...)):
    """
    Upload an .xlsx, return an updated .xlsx with 90-day average sold price (USD) where available.
    """
    filename = (file.filename or "").lower()
    if not filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Please upload an .xlsx file.")

    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Empty file.")

    # Read Excel
    try:
        df = pd.read_excel(BytesIO(raw))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not read Excel: {e}")

    if df.empty:
        raise HTTPException(status_code=400, detail="Spreadsheet has no rows.")

    # Prepare column name mapping (lowered -> original)
    columns_lower = {str(c).strip().lower(): c for c in df.columns}

    # Add output columns (don’t overwrite if they exist)
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

    # Price each row
    # NOTE: This runs sequentially. For 400+ rows it can take a while, but it’s the simplest + safest.
    for idx in range(len(df)):
        row = df.iloc[idx]
        query = _build_query_from_row(row, columns_lower)

        avg, comps, notes, url_used = _price_one_query_playwright(query)

        df.at[idx, "Avg Sold Price (90d, USD)"] = "" if avg is None else avg
        df.at[idx, "# Sold Comps (90d)"] = comps
        df.at[idx, "Source"] = SOURCE_NAME
        df.at[idx, "Query Used"] = url_used or query
        df.at[idx, "Notes"] = notes

    # Write back to Excel
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
