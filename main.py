from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Dict, Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


GAMSTOP_ESTIMATE_URL = "https://www.gamestop.com/graded-trading-cards/estimate"


class GamestopEstimateModel(BaseModel):
    psa_cert: str
    card_name: Optional[str]
    grade: Optional[str]
    gamestop_cash_offer: Optional[float]
    gamestop_credit_offer: Optional[float]
    currency: str
    fetched_at: str
    raw_fields: Dict[str, Any]


class GamestopEstimateError(Exception):
    pass


class InvalidCertFormatError(GamestopEstimateError):
    pass


class EstimateNotFoundError(GamestopEstimateError):
    pass


class SiteChangedError(GamestopEstimateError):
    pass


class GamestopTimeoutError(GamestopEstimateError):
    pass


def _validate_cert(psa_cert: str) -> str:
    psa_cert = psa_cert.strip()
    if not psa_cert:
        raise InvalidCertFormatError("PSA cert number is empty.")
    if not psa_cert.isdigit():
        raise InvalidCertFormatError("PSA cert must be numeric.")
    if len(psa_cert) < 5:
        raise InvalidCertFormatError("PSA cert looks too short.")
    return psa_cert


def _query_gamestop_estimate_raw(psa_cert: str) -> Dict[str, Any]:
    psa_cert = _validate_cert(psa_cert)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        try:
            page.goto(GAMSTOP_ESTIMATE_URL, wait_until="networkidle", timeout=20000)

            # TODO: these selectors may need to be updated if GameStop changes the page.
            PSA_INPUT_SELECTOR = "input[name='psaCert']"
            SUBMIT_BUTTON_SELECTOR = "button:has-text('Get Estimate')"

            RESULT_CONTAINER_SELECTOR = "[data-testid='psa-estimate-result']"
            NO_OFFER_SELECTOR = "[data-testid='psa-estimate-no-offer']"
            NO_OFFER_TEXT_SNIPPET = "no estimate"

            page.wait_for_timeout(1000)

            try:
                page.fill(PSA_INPUT_SELECTOR, psa_cert)
            except PlaywrightTimeoutError:
                raise SiteChangedError("PSA input field not found; selectors likely outdated.")

            try:
                page.click(SUBMIT_BUTTON_SELECTOR)
            except PlaywrightTimeoutError:
                raise SiteChangedError("Submit button not found; selectors likely outdated.")

            try:
                page.wait_for_function(
                    """(resultSel, noOfferSel) => {
                        const result = document.querySelector(resultSel);
                        const noOffer = document.querySelector(noOfferSel);
                        return !!result || !!noOffer;
                    }""",
                    (RESULT_CONTAINER_SELECTOR, NO_OFFER_SELECTOR),
                    timeout=15000,
                )
            except PlaywrightTimeoutError:
                raise GamestopTimeoutError("Timed out waiting for GameStop estimate result.")

            no_offer_el = page.query_selector(NO_OFFER_SELECTOR)
            if no_offer_el:
                raise EstimateNotFoundError(no_offer_el.inner_text().strip())

            body_text = page.inner_text("body")
            if NO_OFFER_TEXT_SNIPPET.lower() in body_text.lower():
                raise EstimateNotFoundError("GameStop did not provide an estimate for this cert.")

            result_container = page.query_selector(RESULT_CONTAINER_SELECTOR)
            if not result_container:
                raise SiteChangedError("Result container missing; markup may have changed.")

            def safe_text(selector: str) -> Optional[str]:
                try:
                    el = page.query_selector(selector)
                    return el.inner_text().strip() if el else None
                except Exception:
                    return None

            card_name = safe_text(f"{RESULT_CONTAINER_SELECTOR} .card-name")
            grade = safe_text(f"{RESULT_CONTAINER_SELECTOR} .card-grade")

            def parse_price(selector: str) -> Optional[float]:
                text = safe_text(selector)
                if not text:
                    return None
                import re
                digits = re.sub(r"[^0-9.]", "", text)
                if not digits:
                    return None
                try:
                    return float(digits)
                except ValueError:
                    return None

            cash_offer = parse_price(f"{RESULT_CONTAINER_SELECTOR} .cash-offer")
            credit_offer = parse_price(f"{RESULT_CONTAINER_SELECTOR} .credit-offer")

            data = {
                "psa_cert": psa_cert,
                "card_name": card_name,
                "grade": grade,
                "gamestop_cash_offer": cash_offer,
                "gamestop_credit_offer": credit_offer,
                "currency": "USD",
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "raw_fields": {
                    "card_name_raw": card_name,
                    "grade_raw": grade,
                },
            }
            return data

        except GamestopEstimateError:
            raise
        except PlaywrightTimeoutError as e:
            raise GamestopTimeoutError(str(e))
        except Exception as e:
            raise GamestopEstimateError(f"Unexpected error: {e}")
        finally:
            browser.close()


app = FastAPI(
    title="Gamestop Graded Cards Estimate API",
    version="1.0.0",
    description="Service that calls GameStop's graded trading card estimate tool."
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/gamestop/estimate", response_model=GamestopEstimateModel)
def gamestop_estimate(psa_cert: str = Query(..., description="PSA certification number")):
    try:
        data = _query_gamestop_estimate_raw(psa_cert)
        return GamestopEstimateModel(**data)
    except InvalidCertFormatError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except EstimateNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (SiteChangedError, GamestopTimeoutError) as e:
        raise HTTPException(status_code=502, detail=str(e))
    except GamestopEstimateError as e:
        raise HTTPException(status_code=500, detail=str(e))
