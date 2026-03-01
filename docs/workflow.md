# Data Flow & Graph Build Workflow

## Overview

```
Browser (state + year)
        ↓
  FastAPI /api/graph/{state}/{year}
        ↓
  ┌─────────────────────────────┐
  │  Cache hit? → return early  │
  └─────────────────────────────┘
        ↓ cache miss
  ┌──────────────────────────────────────────────┐
  │  PARALLEL                                    │
  │  fetch_open_payments(state, year)  ──────┐   │
  │  fetch_npi_physicians(state)        ─────┤   │
  └──────────────────────────────────────────┘   │
        ↓ both complete                          │
  fetch_drugs(company_names from payments)        │
        ↓
  build_graph(payments, physicians, drugs)
        ↓
  GraphResponse → JSON → Browser
```

---

## Step 1 — Request Validation (`main.py`)

The user picks a state and year in the frontend. The browser sends:
```
GET http://localhost:8000/api/graph/GA/2023
```

FastAPI validates:
- `state` must be a valid 2-letter US state code
- `year` must be in `{2020, 2021, 2022, 2023}`

Invalid inputs return `400` before any external API calls are made.

If the `(state, year)` pair was fetched in the last hour, the cached `GraphResponse`
is returned immediately (< 200ms). Otherwise, the three fetchers run.

---

## Step 2 — Parallel Fetch (CMS + NPI)

Two fetchers run at the same time via `asyncio.gather`:

### `fetch_open_payments(state, year)` — `fetchers/open_payments.py`

POSTs to CMS Open Payments API with:
```json
{
  "conditions": [{ "property": "recipient_state", "value": "GA", "operator": "=" }],
  "limit": 500,
  "offset": 0,
  "sort": [{ "property": "total_amount_of_payment_usdollars", "order": "desc" }]
}
```

Returns up to 500 payment records, each parsed into:
```python
{
  "npi": "1234567890",
  "physician_first": "LISA",
  "physician_last": "CHEN",
  "company": "Pfizer",
  "drug": "Eliquis",
  "amount": 12500.0,
  "nature": "Speaking Fee",
  "date": "2023-06-14"
}
```

Dataset IDs are hardcoded per year (verified against the CMS metastore). If an ID
returns 404, the fetcher hits the metastore list endpoint to resolve the correct ID.

### `fetch_npi_physicians(state)` — `fetchers/npi.py`

Makes 5 sequential GET requests to the NPI Registry — one per specialty:
`Cardiology, Endocrinology, Internal Medicine, Oncology, Neurology`

Waits 0.4s between each call to stay under the ~3 req/sec rate limit.
Deduplicates results by NPI number (same doctor can appear in multiple specialty queries).

Returns physicians parsed into:
```python
{
  "npi": "1234567890",
  "first": "Lisa",
  "last": "Chen",
  "full_name": "Dr. Lisa Chen",
  "specialty": "Cardiology",
  "city": "Atlanta",
  "state": "GA"
}
```

---

## Step 3 — Serial Drug Fetch

After payments return, we extract all unique company names:
```python
company_names = list({p["company"] for p in payments})
```

### `fetch_drugs(company_names)` — `fetchers/openfda.py`

Loops each company name, GETs OpenFDA:
```
GET https://api.fda.gov/drug/label.json?search=openfda.manufacturer_name:"Pfizer"&limit=10
```

Parses `indications_and_usage[0]` (free-text) against a keyword map to extract conditions:
```python
CONDITION_MAP = {
    "atrial fibrillation": ("Atrial Fibrillation", "I48"),
    "type 2 diabetes":     ("Type 2 Diabetes",     "E11"),
    ...
}
```

Returns drugs parsed into:
```python
{
  "id": "drug_eliquis",
  "brand": "Eliquis",
  "generic": "apixaban",
  "manufacturer": "Pfizer",
  "conditions": [
    {"name": "Atrial Fibrillation", "icd10": "I48"},
    {"name": "DVT", "icd10": "I82"}
  ]
}
```

Note: OpenFDA is a drug database. Medical device companies (e.g. Kestra, Choice Spine)
return 404 — this is expected and handled silently.

---

## Step 4 — Graph Assembly (`graph/builder.py`)

Uses `networkx.DiGraph` — a directed graph where nodes are circles and edges are arrows.

### Node creation order

| Function | Input | Creates |
|---|---|---|
| `_add_pharma_nodes` | payments | One node per unique company. Aggregates `total_paid` and `num_physicians`. |
| `_add_drug_nodes` | drugs | One node per drug. |
| `_add_condition_nodes` | drugs | One node per unique ICD-10 condition found across all drugs. |
| `_add_physician_nodes` | physicians + payments | NPI Registry physicians first (rich data). Then any physician in payments who wasn't in NPI results (minimal data — name + NPI only). |

### Edge creation order

| Function | Creates | Logic |
|---|---|---|
| `_add_manufactures_edges` | pharma → drug | Slugify drug's manufacturer name, check if pharma node exists, draw edge. |
| `_add_indicated_for_edges` | drug → condition | For each condition inside a drug, draw edge to its condition node. |
| `_add_specializes_in_edges` | physician → condition | Look up physician's specialty in `TAXONOMY_CONDITION_MAP`, draw edges to matching condition nodes. |
| `_add_paid_edges` | pharma → physician | One edge per unique pharma↔physician pair. Multiple payments between same pair accumulate as weight. |
| `_add_received_for_edges` | physician → drug | Match payment's drug name against drug brand/generic lookup. Accumulates weight. |
| `_add_peer_of_edges` | physician ↔ physician | Two physicians get this edge if: same specialty AND both paid by the same pharma company. Capped at 100 edges. |

---

## Step 5 — Truncation & Serialization

NetworkX's graph object can't be sent as JSON directly. `_serialize()` manually
converts every node and edge into Pydantic `Node` / `Edge` objects.

If over limits (200 nodes, 400 edges):

**Node truncation** — anchor nodes (pharma, drug, condition) are always kept in full.
Only physicians are truncated, sorted by `total_received` descending so the highest-paid
physicians are kept.

**Edge truncation** — structural edges (MANUFACTURES, INDICATED_FOR, SPECIALIZES_IN,
RECEIVED_FOR, PEER_OF) are kept in full. PAID edges are sorted by weight and truncated
last.

The final `GraphResponse` is cached and returned as JSON.

---

## Known Limitations (Demo Scope)

| Limitation | Reason |
|---|---|
| Only 5 specialties queried from NPI | Full NPI taxonomy has 800+ codes |
| Only 8 conditions in keyword map | Full clinical NLP would use ICD-10 database + medical NLP |
| 500 payment record cap | GA/TX/CA have tens of thousands of records |
| OpenFDA misses device companies | FDA drug label database doesn't include medical devices |
| PEER_OF rarely fires | Requires NPI specialty overlap with CMS payment recipients |
