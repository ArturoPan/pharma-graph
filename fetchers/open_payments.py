import logging
import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://openpaymentsdata.cms.gov/api/1/datastore/query/{dataset_id}/0"
LIST_URL = "https://openpaymentsdata.cms.gov/api/1/metastore/schemas/dataset/items?show-reference-ids=false"

# Verified dataset IDs from CMS metastore (api/1/metastore/schemas/dataset/items)
OPEN_PAYMENTS_DATASETS: dict[int, str] = {
    2020: "a08c4b30-5cf3-4948-ad40-36f404619019",
    2021: "0380bbeb-aea1-58b6-b708-829f92a48202",
    2022: "df01c2f8-dc1f-4e79-96cb-8208beaf143c",
    2023: "fb3a65aa-c901-4a38-a813-b04b00dfa2a9",
}

# CMS Open Payments field names in the API response
FIELD_NPI = "covered_recipient_npi"
FIELD_FIRST = "covered_recipient_first_name"
FIELD_LAST = "covered_recipient_last_name"
FIELD_COMPANY = "applicable_manufacturer_or_applicable_gpo_making_payment_name"
FIELD_DRUG = "name_of_drug_or_biological_or_device_or_medical_supply_1"
FIELD_AMOUNT = "total_amount_of_payment_usdollars"
FIELD_NATURE = "nature_of_payment_or_transfer_of_value"
FIELD_DATE = "date_of_payment"


async def _resolve_dataset_id(client: httpx.AsyncClient, year: int) -> str | None:
    """
    If the hardcoded dataset ID for a year returns 404, fall back to fetching
    the datastore list and finding the dataset matching the program year.
    """
    try:
        resp = await client.get(LIST_URL, timeout=10)
        resp.raise_for_status()
        datasets = resp.json()
        year_str = str(year)
        for ds in datasets:
            identifier = ds.get("identifier", "")
            title = ds.get("title", "").lower()
            # Match on program year in the title or identifier
            if year_str in title or year_str in identifier:
                logger.info("Resolved dataset ID for %s: %s", year, identifier)
                return identifier
        logger.warning("Could not resolve dataset ID for year %s from list endpoint", year)
    except Exception as exc:
        logger.error("Failed to fetch datastore list: %s", exc)
    return None


def _parse_payment(row: dict) -> dict | None:
    npi = row.get(FIELD_NPI, "").strip()
    company = row.get(FIELD_COMPANY, "").strip()
    amount_raw = row.get(FIELD_AMOUNT, 0)

    # Skip rows missing the fields we need for graph edges
    if not npi or not company:
        return None

    try:
        amount = float(amount_raw)
    except (TypeError, ValueError):
        amount = 0.0

    return {
        "npi": npi,
        "physician_first": row.get(FIELD_FIRST, "").strip(),
        "physician_last": row.get(FIELD_LAST, "").strip(),
        "company": company,
        "drug": row.get(FIELD_DRUG, "").strip(),
        "amount": amount,
        "nature": row.get(FIELD_NATURE, "").strip(),
        "date": row.get(FIELD_DATE, "").strip(),
    }


async def fetch_open_payments(state: str, year: int) -> list[dict]:
    """
    Fetch pharma→physician payments for a given state and year from CMS Open Payments.
    Returns a list of payment dicts. Returns [] on any error.
    """
    dataset_id = OPEN_PAYMENTS_DATASETS.get(year)
    if not dataset_id:
        logger.error("No dataset ID configured for year %s", year)
        return []

    url = BASE_URL.format(dataset_id=dataset_id)
    payload = {
        "conditions": [
            {"property": "recipient_state", "value": state, "operator": "="}
        ],
        "limit": 500,
        "offset": 0,
        "sort": [{"property": FIELD_AMOUNT, "order": "desc"}],
    }

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, json=payload, timeout=20)

            # If the hardcoded ID is stale, try to resolve it from the list endpoint
            if resp.status_code == 404:
                logger.warning(
                    "CMS Open Payments: 404 for dataset %s (year %s), attempting resolution",
                    dataset_id, year,
                )
                resolved = await _resolve_dataset_id(client, year)
                if not resolved:
                    return []
                url = BASE_URL.format(dataset_id=resolved)
                resp = await client.post(url, json=payload, timeout=20)

            if resp.status_code != 200:
                logger.error(
                    "CMS Open Payments: HTTP %s for state=%s year=%s",
                    resp.status_code, state, year,
                )
                return []

            data = resp.json()
            rows = data.get("results", data.get("data", []))

            payments = []
            for row in rows:
                parsed = _parse_payment(row)
                if parsed:
                    payments.append(parsed)

            logger.info(
                "CMS Open Payments: %d payments fetched for state=%s year=%s",
                len(payments), state, year,
            )
            return payments

    except Exception as exc:
        logger.error("CMS Open Payments: unexpected error for state=%s year=%s: %s", state, year, exc)
        return []
