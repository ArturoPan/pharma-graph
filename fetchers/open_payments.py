import asyncio
import logging
import urllib.parse

import httpx

logger = logging.getLogger(__name__)

SQL_URL  = "https://openpaymentsdata.cms.gov/api/1/datastore/sql"
POST_URL = "https://openpaymentsdata.cms.gov/api/1/datastore/query/{dataset_id}/0"

# Dataset IDs (from CMS metastore).  The SQL endpoint needs the *distribution* ID
# (the child CSV resource), not the dataset wrapper ID.  We look it up once per year.
DATASET_IDS: dict[int, str] = {
    2018: "f003634c-c103-568f-876c-73017fa83be0",
    2019: "4e54dd6c-30f8-4f86-86a7-3c109a89528e",
    2020: "a08c4b30-5cf3-4948-ad40-36f404619019",
    2021: "0380bbeb-aea1-58b6-b708-829f92a48202",
    2022: "df01c2f8-dc1f-4e79-96cb-8208beaf143c",
    2023: "fb3a65aa-c901-4a38-a813-b04b00dfa2a9",
    2024: "e6b17c6a-2534-4207-a4a1-6746a14911ff",
}

# Distribution IDs pre-fetched from the metastore — SQL endpoint needs these.
# Fetch once; cached in module-level dict so server restarts re-use them.
_dist_id_cache: dict[int, str] = {}

# Payment natures ordered from highest-$ to lowest-$.
# Fetching each separately lets us capture consulting/speaking fees (large) as
# well as food/beverage (small but generates more physician-company connections).
PAYMENT_NATURES = [
    "Consulting Fee",
    "Speaker honoraria",
    "Compensation for services other than consulting",
    "Travel and Lodging",
    "Education",
    "Food and Beverage",
    "Grant",
]

# Field names in the SQL (Title_Case) vs POST (snake_case) APIs are different
SQL_FIELDS = {
    "npi":     "Covered_Recipient_NPI",
    "first":   "Covered_Recipient_First_Name",
    "last":    "Covered_Recipient_Last_Name",
    "company": "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name",
    "drug":    "Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1",
    "amount":  "Total_Amount_of_Payment_USDollars",
    "nature":  "Nature_of_Payment_or_Transfer_of_Value",
    "date":    "Date_of_Payment",
}

POST_FIELDS = {
    "npi":     "covered_recipient_npi",
    "first":   "covered_recipient_first_name",
    "last":    "covered_recipient_last_name",
    "company": "applicable_manufacturer_or_applicable_gpo_making_payment_name",
    "drug":    "name_of_drug_or_biological_or_device_or_medical_supply_1",
    "amount":  "total_amount_of_payment_usdollars",
    "nature":  "nature_of_payment_or_transfer_of_value",
    "date":    "date_of_payment",
}


def _parse_row(row: dict, fields: dict) -> dict | None:
    npi     = row.get(fields["npi"], "").strip()
    company = row.get(fields["company"], "").strip()
    if not npi or not company:
        return None
    try:
        amount = float(row.get(fields["amount"], 0) or 0)
    except (TypeError, ValueError):
        amount = 0.0
    return {
        "npi":            npi,
        "physician_first": row.get(fields["first"], "").strip(),
        "physician_last":  row.get(fields["last"], "").strip(),
        "company":         company,
        "drug":            row.get(fields["drug"], "").strip(),
        "amount":          amount,
        "nature":          row.get(fields["nature"], "").strip(),
        "date":            row.get(fields["date"], "").strip(),
    }


async def _get_distribution_id(client: httpx.AsyncClient, year: int) -> str | None:
    """Look up the CSV distribution ID for the given year's General Payment dataset."""
    if year in _dist_id_cache:
        return _dist_id_cache[year]
    dataset_id = DATASET_IDS.get(year)
    if not dataset_id:
        return None
    try:
        url = (
            f"https://openpaymentsdata.cms.gov/api/1/metastore/schemas/dataset/items"
            f"/{dataset_id}?show-reference-ids=true"
        )
        resp = await client.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        distributions = data.get("distribution", [])
        if distributions:
            dist_id = distributions[0].get("identifier", "")
            if dist_id:
                _dist_id_cache[year] = dist_id
                logger.info("Distribution ID for %s: %s", year, dist_id)
                return dist_id
    except Exception as exc:
        logger.warning("Could not resolve distribution ID for year %s: %s", year, exc)
    return None


def _sql_get(client: httpx.AsyncClient, query: str):
    """
    Execute a DKAN SQL query.  CRITICAL: build the URL manually so spaces are
    encoded as %20 — httpx params= encodes them as + which DKAN rejects.
    """
    encoded = urllib.parse.quote(query, safe="")
    url = f"{SQL_URL}?query={encoded}"
    return client.get(url, timeout=30)


async def _fetch_via_sql(
    client: httpx.AsyncClient, dist_id: str, state: str
) -> list[dict]:
    """
    Fetch payments via the SQL endpoint, one query per payment nature.
    This bypasses the broken numeric sort by letting us request consulting/
    speaking fees (large $) separately from food/beverage (small $).
    Returns [] if all queries fail.
    """
    nature_field = SQL_FIELDS["nature"]
    payments: list[dict] = []

    for nature in PAYMENT_NATURES:
        query = (
            f'[SELECT * FROM {dist_id}]'
            f'[WHERE recipient_state = "{state}" AND {nature_field} = "{nature}"]'
            f'[LIMIT 500]'
        )
        try:
            resp = await _sql_get(client, query)
            if resp.status_code != 200:
                logger.warning("SQL %s HTTP %s", nature, resp.status_code)
                continue
            rows = resp.json()
            # Guard against DKAN returning {"expression":"N"} instead of real rows
            if not isinstance(rows, list) or (rows and "expression" in rows[0]):
                logger.warning("SQL returned expression result for nature=%s, skipping", nature)
                continue
            parsed = [p for p in (_parse_row(r, SQL_FIELDS) for r in rows) if p]
            payments.extend(parsed)
            logger.info("SQL '%s': %d records for state=%s", nature, len(parsed), state)
        except Exception as exc:
            logger.warning("SQL error for nature=%s: %s", nature, exc)

    return payments


async def _fetch_via_post(
    client: httpx.AsyncClient, dataset_id: str, state: str, year: int
) -> list[dict]:
    """Fallback: POST query with 500 records (no reliable numeric sort)."""
    url = POST_URL.format(dataset_id=dataset_id)
    payload = {
        "conditions": [{"property": "recipient_state", "value": state, "operator": "="}],
        "limit": 500,
        "offset": 0,
    }
    try:
        resp = await client.post(url, json=payload, timeout=40)
        if resp.status_code != 200:
            logger.error("POST HTTP %s for state=%s year=%s", resp.status_code, state, year)
            return []
        data = resp.json()
        rows = data.get("results", data.get("data", []))
        payments = [p for p in (_parse_row(r, POST_FIELDS) for r in rows) if p]
        logger.info("POST fallback: %d records for state=%s year=%s", len(payments), state, year)
        return payments
    except Exception as exc:
        logger.error("POST fallback error: %s", exc)
        return []


async def fetch_open_payments(state: str, year: int) -> list[dict]:
    """
    Fetch pharma→physician payments for the given state and year.

    Strategy:
    1. Look up the distribution ID (needed for SQL endpoint).
    2. Query SQL endpoint once per payment nature — this captures large consulting
       and speaking fees that the random-sample POST approach almost always misses.
    3. Fall back to POST (500 random records) if SQL is unavailable.
    """
    dataset_id = DATASET_IDS.get(year)
    if not dataset_id:
        logger.error("No dataset ID for year %s", year)
        return []

    async with httpx.AsyncClient() as client:
        dist_id = await _get_distribution_id(client, year)

        if dist_id:
            payments = await _fetch_via_sql(client, dist_id, state)
            if payments:
                return payments
            logger.warning("SQL returned 0 payments, falling back to POST for state=%s year=%s", state, year)

        return await _fetch_via_post(client, dataset_id, state, year)
