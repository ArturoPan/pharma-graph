"""
OpenFDA drug label fetcher. Looks up drugs (and parses conditions from indications)
for pharma company names from CMS Open Payments.

Why some pharma have no drugs/conditions:
- Name mismatch: we try exact CMS name, normalized name (no Inc/LLC), then
  primary-word fallback so FDA spelling differences still match.
- Device/GPO companies: the drug label API only has drugs; device makers and
  GPOs return no results and will always show only physicians and payments.
"""
import asyncio
import logging
import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://api.fda.gov/drug/label.json"

CONDITION_MAP = {
    # Cardiovascular
    "atrial fibrillation":   ("Atrial Fibrillation",    "I48"),
    "heart failure":         ("Heart Failure",           "I50"),
    "hypertension":          ("Hypertension",            "I10"),
    "deep vein thrombosis":  ("DVT",                     "I82"),
    "stroke":                ("Stroke",                  "I63"),
    "coronary artery":       ("Coronary Artery Disease", "I25"),
    "angina":                ("Angina",                  "I20"),
    # Metabolic / Endocrine
    "type 2 diabetes":       ("Type 2 Diabetes",         "E11"),
    "type 1 diabetes":       ("Type 1 Diabetes",         "E10"),
    "diabetes":              ("Diabetes",                "E14"),
    "hypothyroid":           ("Hypothyroidism",          "E03"),
    "obesity":               ("Obesity",                 "E66"),
    "hyperlipidemia":        ("Hyperlipidemia",          "E78"),
    "cholesterol":           ("High Cholesterol",        "E78"),
    # Oncology
    "cancer":                ("Cancer",                  "C80"),
    "carcinoma":             ("Carcinoma",               "C80"),
    "lymphoma":              ("Lymphoma",                "C85"),
    "leukemia":              ("Leukemia",                "C91"),
    "melanoma":              ("Melanoma",                "C43"),
    "tumor":                 ("Tumor",                   "D48"),
    "myeloma":               ("Multiple Myeloma",        "C90"),
    "prostate cancer":       ("Prostate Cancer",         "C61"),
    "breast cancer":         ("Breast Cancer",           "C50"),
    "lung cancer":           ("Lung Cancer",             "C34"),
    # Neurological / Psychiatric
    "multiple sclerosis":    ("Multiple Sclerosis",      "G35"),
    "epilep":                ("Epilepsy",                "G40"),
    "seizure":               ("Epilepsy",                "G40"),
    "migraine":              ("Migraine",                "G43"),
    "alzheimer":             ("Alzheimer's Disease",     "G30"),
    "parkinson":             ("Parkinson's Disease",     "G20"),
    "bipolar":               ("Bipolar Disorder",        "F31"),
    "schizophrenia":         ("Schizophrenia",           "F20"),
    "depression":            ("Depression",              "F32"),
    "anxiety":               ("Anxiety",                 "F41"),
    "adhd":                  ("ADHD",                    "F90"),
    "attention deficit":     ("ADHD",                    "F90"),
    # Respiratory
    "asthma":                ("Asthma",                  "J45"),
    "copd":                  ("COPD",                    "J44"),
    "pulmonary":             ("Pulmonary Disease",       "J98"),
    "pneumonia":             ("Pneumonia",               "J18"),
    # Musculoskeletal / Autoimmune
    "rheumatoid arthritis":  ("Rheumatoid Arthritis",    "M05"),
    "arthritis":             ("Arthritis",               "M13"),
    "psoriasis":             ("Psoriasis",               "L40"),
    "crohn":                 ("Crohn's Disease",         "K50"),
    "ulcerative colitis":    ("Ulcerative Colitis",      "K51"),
    "lupus":                 ("Lupus",                   "M32"),
    "osteoporosis":          ("Osteoporosis",            "M81"),
    # Infectious Disease
    "hiv":                   ("HIV/AIDS",                "B20"),
    "hepatitis":             ("Hepatitis",               "B19"),
    "infection":             ("Infection",               "A49"),
    # GI / Other
    "endometriosis":         ("Endometriosis",           "N80"),
    "overactive bladder":    ("Overactive Bladder",      "N32"),
    "pain":                  ("Chronic Pain",            "R52"),
    "fibromyalgia":          ("Fibromyalgia",            "M79"),
    "anemia":                ("Anemia",                  "D64"),
    "kidney":                ("Kidney Disease",          "N18"),
    "glaucoma":              ("Glaucoma",                "H40"),
    "macular degeneration":  ("Macular Degeneration",   "H35"),
}


def _parse_conditions(indications_text: str) -> list[dict]:
    """
    Scan the indications_and_usage text for known condition keywords.
    A single drug can match multiple conditions.
    Returns a list of { name, icd10 } dicts.
    """
    text = indications_text.lower()
    matched = []
    for keyword, (name, icd10) in CONDITION_MAP.items():
        if keyword in text:
            matched.append({"name": name, "icd10": icd10})
    return matched


def _slugify(name: str) -> str:
    return name.lower().strip().replace(" ", "_").replace("/", "_")


# Strip common suffixes so "Pfizer Inc" can match OpenFDA's "Pfizer Inc." or "Pfizer"
def _normalize_company(name: str) -> str:
    s = name.strip()
    for suffix in (
        ", Inc.", " Inc.", ", Inc", " Inc", ", LLC", " LLC",
        ", Corp.", " Corp.", ", Corporation", " Corporation",
        ", Co.", " Co.", ", Ltd.", " Ltd.", ", L.P.", " L.P.",
        ", LLP", " LLP", ", PLC", " PLC",
    ):
        if s.endswith(suffix):
            s = s[: -len(suffix)].strip() or s
            break
        no_dot = suffix.rstrip(".")
        if no_dot and s.endswith(no_dot):
            s = s[: -len(no_dot)].strip() or s
            break
    return s.strip() or name.strip()


def _company_search_variants(company: str) -> list[str]:
    """Return search strings to try in order: exact, normalized, then primary name(s) for partial match."""
    variants = []
    raw = company.strip()
    if raw:
        variants.append(raw)
    normalized = _normalize_company(raw)
    if normalized and normalized != raw:
        variants.append(normalized)
    # First word or first two words (e.g. "Eli Lilly") for unquoted OpenFDA search
    words = [w for w in normalized.split() if len(w) > 1 and w.lower() not in ("the", "and", "for")]
    if words:
        variants.append(" ".join(words[:2]))
    if words and len(words) >= 1:
        variants.append(words[0])
    # Dedupe preserving order
    seen = set()
    out = []
    for v in variants:
        v = v.strip()
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _parse_drug(result: dict) -> dict | None:
    openfda = result.get("openfda", {})

    brand_names = openfda.get("brand_name", [])
    generic_names = openfda.get("generic_name", [])
    manufacturers = openfda.get("manufacturer_name", [])
    indications_list = result.get("indications_and_usage", [])

    brand = brand_names[0].strip() if brand_names else ""
    generic = generic_names[0].strip() if generic_names else ""
    manufacturer = manufacturers[0].strip() if manufacturers else ""
    indications = indications_list[0] if indications_list else ""

    if not brand and not generic:
        return None

    label = brand or generic
    conditions = _parse_conditions(indications)

    return {
        "id": f"drug_{_slugify(label)}",
        "brand": brand,
        "generic": generic,
        "manufacturer": manufacturer,
        "conditions": conditions,
    }


async def _fetch_drugs_for_company(client: httpx.AsyncClient, company: str) -> list[dict]:
    """
    Fetch drugs for one pharma company. Tries exact CMS name first, then normalized
    name, then primary-word fallback (unquoted) so we still get drugs when CMS and
    FDA use different spellings. Device/GPO companies have no drug labels and
    will always return 0 results.
    """
    variants = _company_search_variants(company)
    normalized = _normalize_company(company)
    first_word = (normalized.split() or [""])[0].lower()

    for i, search_name in enumerate(variants):
        # First tries: phrase match (quoted). Later: unquoted for partial match.
        use_phrase = i < 2
        if use_phrase:
            search = f'openfda.manufacturer_name:"{search_name}"'
        else:
            search = f"openfda.manufacturer_name:{search_name.replace(' ', '+')}"
        params = {"search": search, "limit": 10}
        try:
            resp = await client.get(BASE_URL, params=params, timeout=15)

            if resp.status_code == 404:
                continue
            if resp.status_code != 200:
                logger.warning("OpenFDA: HTTP %s for company=%s search=%s", resp.status_code, company, search_name)
                continue

            data = resp.json()
            results = data.get("results", [])
            if not results:
                continue

            drugs = []
            for result in results:
                parsed = _parse_drug(result)
                if not parsed:
                    continue
                api_manufacturer = (parsed.get("manufacturer") or "").lower()
                # When using fallback (non-exact) search, only keep drugs whose FDA manufacturer matches this company
                if i >= 2 and api_manufacturer:
                    if first_word not in api_manufacturer and normalized.lower() not in api_manufacturer:
                        continue
                parsed["manufacturer"] = company
                drugs.append(parsed)
            if drugs:
                logger.info("OpenFDA: %d drugs for company=%s (matched via %s)", len(drugs), company, search_name)
                return drugs
        except Exception as exc:
            logger.debug("OpenFDA: try company=%s variant=%s: %s", company, search_name, exc)
            continue

    logger.info("OpenFDA: no drugs for company=%s (device/GPO or name mismatch)", company)
    return []


async def fetch_drugs(company_names: list[str]) -> list[dict]:
    """
    Fetch drugs and parsed conditions for a list of pharma company names.
    Deduplicates by drug ID. Returns [] on total failure.
    """
    drugs: dict[str, dict] = {}  # keyed by drug ID to deduplicate

    async with httpx.AsyncClient() as client:
        for i, company in enumerate(company_names):
            if i > 0:
                await asyncio.sleep(0.3)  # stay under 40 req/min unauthenticated limit

            results = await _fetch_drugs_for_company(client, company)
            for drug in results:
                if drug["id"] not in drugs:
                    drugs[drug["id"]] = drug

    all_drugs = list(drugs.values())
    logger.info("OpenFDA: %d unique drugs total across %d companies", len(all_drugs), len(company_names))
    return all_drugs
