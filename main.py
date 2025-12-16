from __future__ import annotations

import re
import asyncio
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


app = FastAPI(title=APP_TITLE, version="1.0.2")


@app.get("/health")
def health():
    return {"status": "ok"}


def clean_grade(val) -> Optional[str]:
    if val is None:
        return None
    m = re.search(r"(\d{1,2})", str(val))
    return m.group(1) if m else None


def normalize_item_text(item: str) -> str:
    s = (item or "").strip()
    s = s.replace("|", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def build_query(item: str, grade: Optional[str]) -> str:
    base = normalize_item_text(item)
    if grade:
        return f"{base} PSA {grade}" if base else f"PSA {grade}"
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


def avg_90d(sales: List[Tuple[Optional[datetime], Optional[float]]]):
    cutoff = now_utc() - timedelta(days=90)
    prices = [p for d, p in sales if d is not None and d >= cutoff and p is not None]
    if not prices:
        return None, 0, "No sold comps with dates in last 90 days."
    return round(sum(prices) / len(prices), 2), len(prices), ""


def looks_blocked(text: str) -> bool:
    t = (text or "").lower()
    blockers = [
        "verify you are human",
        "captcha",
        "cloudflare",
        "attention required",
        "access denied",
        "unusual traffic",
        "temporarily blocked",
        "robot",
        "enable cookies",
    ]
    return any(b in t for b in blockers)


async def scrape_130point_for_query(page, query: str) -> tuple[Optional[float], int, str, str]:
    """
    Returns: (avg_price_90d, comps_90d, notes, url_used)
    """
    q = query.strip()
    if not q:
        return None, 0, "Empty query.", ""

    url = SALES_URL.format(query=re.sub(r"\s+", "+", q))

    # Navigate
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=45000)
    except Exception as e:
        return None, 0, f"Navigation error: {e}", url

    # Give client-side JS time to render (130point can be slow)
    await page.wait_for_timeout(1500)

    # Quick block detection
    try:
        body_txt = await page.inner_text("body")
    except Exception:
        body_txt = ""

    if looks_blocked(body_txt):
        title = ""
        try:
            title = await page.title()
        except Exception:
            pass
        snippet = (body_txt[:180] + "…") if body_txt else ""
        return None, 0, f"Blocked/Captcha suspected. title='{title}' snippet='{snippet}'", url

    # Try multiple table selectors (130point has changed markup over time)
    table_selectors = [
        "table tbody tr",
        "table tr",  # fallback
        ".table tbody tr",
        ".sales-table tbody tr",
    ]

    rows_found = 0
    last_err = ""
    for sel in table_selectors:
        try:
            await page.wait_for_selector(sel, timeout=12000)
            rows = page.locator(sel)
            rows_found = await rows.count()
            if rows_found > 0:
                # Parse rows
                sales: List[Tuple[Optional[datetime], Optional[float]]] = []
                n = min(rows_found, 200)  # cap parsing work
                for i in range(n):
                    try:
                        txt = await rows.nth(i).inner_text(timeout=2000)
                    except Exception:
                        continue
                    dt = parse_date(txt)
                    price = parse_price(txt)
                    if price is not None:
                        sales.append((dt, price))

                avg, comps, notes = avg_90d(sales)
                if comps == 0:
                    # We found rows but didn't parse date+price reliably
                    title = ""
                    try:
                        title = await page.title()
                    except Exception:
                        pass
                    return None, 0, f"Found table rows ({rows_found}) but parsed 0 comps. title='{title}'", url

                return avg, comps, "", url

        except PlaywrightTimeoutError:
            last_err = f"Timeout waiting for selector: {sel}"
        except Exception as e:
            last_err = f"Error with selector {sel}: {e}"

    # If no table found, return diagnostics
    title = ""
    try:
        title = await page.title()
    except Exception:
        pass
    snippet = (body_txt[:180] + "…") if body_txt else ""
    return None, 0, f"No results table found. title='{title}'. {last_err}. snippet='{snippet}'", url


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

    # Ensure output columns exist
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

    if "Item" not in df.columns:
        raise HTTPException(status_code=400, detail="Expected an 'Item' column in the sheet.")

    # IMPORTANT: One Playwright browser/context/page per upload (much faster, less blocky)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            viewport={"width": 1280, "height": 800},
        )
        page = await context.new_page()

        # Light rate limiting to reduce blocks
        for idx in range(len(df)):
            item = str(df.at[idx, "Item"]) if "Item" in df.columns else ""
            grade = clean_grade(df.at[idx, "Grade"]) if "Grade" in df.columns else None

            q = build_query(item, grade)
            avg, comps, notes, url = await scrape_130point_for_query(page, q)

            df.at[idx, "Avg Sold Price (90d, USD)"] = "" if avg is None else avg
            df.at[idx, "# Sold Comps (90d)"] = int(comps)
            df.at[idx, "Source"] = SOURCE_NAME
            df.at[idx, "Query Used"] = url or q
            df.at[idx, "Notes"] = notes

            # polite delay (important)
            await asyncio.sleep(0.6)

        await context.close()
        await browser.close()

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
