import logging
import httpx

logger = logging.getLogger(__name__)

# DKAN endpoints — SQL endpoint supports CAST for proper numeric sort
SQL_URL   = "https://openpaymentsdata.cms.gov/api/1/datastore/sql"
POST_URL  = "https://openpaymentsdata.cms.gov/api/1/datastore/query/{dataset_id}/0"
LIST_URL  = "https://openpaymentsdata.cms.gov/api/1/metastore/schemas/dataset/items?show-reference-ids=false"

# Verified dataset IDs from CMS metastore (api/1/metastore/schemas/dataset/items)
OPEN_PAYMENTS_DATASETS: dict[int, str] = {
    2020: "a08c4b30-5cf3-4948-ad40-36f404619019",
    2021: "0380bbeb-aea1-58b6-b708-829f92a48202",
    2022: "df01c2f8-dc1f-4e79-96cb-8208beaf143c",
    2023: "fb3a65aa-c901-4a38-a813-b04b00dfa2a9",
}

FETCH_LIMIT = 2000  # Records to fetch — enough to capture large consulting fees

# CMS Open Payments field names in the API response
FIELD_NPI     = "covered_recipient_npi"
FIELD_FIRST   = "covered_recipient_first_name"
FIELD_LAST    = "covered_recipient_last_name"
FIELD_COMPANY = "applicable_manufacturer_or_applicable_gpo_making_payment_name"
FIELD_DRUG    = "name_of_drug_or_biological_or_device_or_medical_supply_1"
FIELD_AMOUNT  = "total_amount_of_payment_usdollars"
FIELD_NATURE  = "nature_of_payment_or_transfer_of_value"
FIELD_DATE    = "date_of_payment"


def _parse_payment(row: dict) -> dict | None:
    npi     = row.get(FIELD_NPI, "").strip()
    company = row.get(FIELD_COMPANY, "").strip()
    amount_raw = row.get(FIELD_AMOUNT, 0)

    if not npi or not company:
        return None

    try:
        amount = float(amount_raw)
    except (TypeError, ValueError):
        amount = 0.0

    return {
        "npi":            npi,
        "physician_first": row.get(FIELD_FIRST, "").strip(),
        "physician_last":  row.get(FIELD_LAST, "").strip(),
        "company":         company,
        "drug":            row.get(FIELD_DRUG, "").strip(),
        "amount":          amount,
        "nature":          row.get(FIELD_NATURE, "").strip(),
        "date":            row.get(FIELD_DATE, "").strip(),
    }


async def _fetch_via_sql(
    client: httpx.AsyncClient, dataset_id: str, state: str
) -> list[dict] | None:
    """
    Use the DKAN SQL endpoint which supports CAST for proper numeric sorting.
    Returns None if the endpoint is unavailable so the caller can fall back.
    """
    select_cols = ", ".join([
        FIELD_NPI, FIELD_FIRST, FIELD_LAST, FIELD_COMPANY,
        FIELD_DRUG, FIELD_AMOUNT, FIELD_NATURE, FIELD_DATE,
    ])
    # DKAN SQL syntax: each clause in its own square brackets
    query = (
        f"[SELECT {select_cols} FROM {dataset_id}]"
        f"[WHERE recipient_state = '{state}']"
        f"[ORDER BY CAST({FIELD_AMOUNT} AS DECIMAL) DESC]"
        f"[LIMIT {FETCH_LIMIT}]"
    )
    try:
        resp = await client.get(SQL_URL, params={"query": query}, timeout=40)
        if resp.status_code != 200:
            logger.warning("SQL endpoint HTTP %s for state=%s", resp.status_code, state)
            return None
        rows = resp.json()
        if not isinstance(rows, list):
            logger.warning("SQL endpoint returned non-list: %s", type(rows))
            return None
        payments = [p for p in (_parse_payment(r) for r in rows) if p]
        logger.info("SQL endpoint: %d payments for state=%s", len(payments), state)
        return payments
    except Exception as exc:
        logger.warning("SQL endpoint error: %s", exc)
        return None


async def _fetch_via_post(
    client: httpx.AsyncClient, dataset_id: str, state: str, year: int
) -> list[dict]:
    """
    Fall back to POST query endpoint (no reliable numeric sort, but still useful).
    """
    url = POST_URL.format(dataset_id=dataset_id)
    payload = {
        "conditions": [{"property": "recipient_state", "value": state, "operator": "="}],
        "limit": FETCH_LIMIT,
        "offset": 0,
    }
    try:
        resp = await client.post(url, json=payload, timeout=40)
        if resp.status_code == 404:
            # Dataset ID stale — resolve from metastore
            resolved = await _resolve_dataset_id(client, year)
            if not resolved:
                return []
            url = POST_URL.format(dataset_id=resolved)
            resp = await client.post(url, json=payload, timeout=40)
        if resp.status_code != 200:
            logger.error("POST fallback HTTP %s for state=%s year=%s", resp.status_code, state, year)
            return []
        data = resp.json()
        rows = data.get("results", data.get("data", []))
        payments = [p for p in (_parse_payment(r) for r in rows) if p]
        logger.info("POST fallback: %d payments for state=%s year=%s", len(payments), state, year)
        return payments
    except Exception as exc:
        logger.error("POST fallback error: %s", exc)
        return []


async def _resolve_dataset_id(client: httpx.AsyncClient, year: int) -> str | None:
    try:
        resp = await client.get(LIST_URL, timeout=10)
        resp.raise_for_status()
        datasets = resp.json()
        year_str = str(year)
        for ds in datasets:
            identifier = ds.get("identifier", "")
            title = ds.get("title", "").lower()
            if year_str in title or year_str in identifier:
                logger.info("Resolved dataset ID for %s: %s", year, identifier)
                return identifier
    except Exception as exc:
        logger.error("Failed to fetch datastore list: %s", exc)
    return None


async def fetch_open_payments(state: str, year: int) -> list[dict]:
    """
    Fetch the top pharma→physician payments for a given state and year.

    Tries the SQL endpoint first (supports CAST for numeric sort — gives us the
    largest payments). Falls back to POST if SQL is unavailable.
    """
    dataset_id = OPEN_PAYMENTS_DATASETS.get(year)
    if not dataset_id:
        logger.error("No dataset ID configured for year %s", year)
        return []

    async with httpx.AsyncClient() as client:
        # Try SQL endpoint (proper numeric sort)
        result = await _fetch_via_sql(client, dataset_id, state)
        if result is not None:
            return result

        # Fall back to POST (no numeric sort, but still returns data)
        logger.warning("Falling back to POST for state=%s year=%s", state, year)
        return await _fetch_via_post(client, dataset_id, state, year)
