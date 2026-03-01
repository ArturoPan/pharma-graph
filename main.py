import asyncio
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from fetchers.npi import fetch_npi_physicians
from fetchers.open_payments import fetch_open_payments
from fetchers.openfda import fetch_drugs
from graph.builder import build_graph
from graph.models import GraphResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# In-memory cache: { (state, year): { "data": GraphResponse, "ts": float } }
# ---------------------------------------------------------------------------
_cache: dict[tuple[str, int], dict] = {}
CACHE_TTL = 3600  # 1 hour in seconds

VALID_YEARS = {2020, 2021, 2022, 2023}
US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC",
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Impiricus Graph API starting up")
    yield
    logger.info("Impiricus Graph API shutting down")


app = FastAPI(
    title="Impiricus Clinical Intelligence Graph",
    description="Visualizes the pharma-physician influence network using public US government data.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/graph/{state}/{year}", response_model=GraphResponse)
async def get_graph(state: str, year: int) -> GraphResponse:
    state = state.upper()

    if state not in US_STATES:
        raise HTTPException(status_code=400, detail=f"Invalid state code: {state}")
    if year not in VALID_YEARS:
        raise HTTPException(status_code=400, detail=f"Year must be one of {sorted(VALID_YEARS)}")

    # Return cached response if still fresh
    cache_key = (state, year)
    cached = _cache.get(cache_key)
    if cached and (time.time() - cached["ts"]) < CACHE_TTL:
        logger.info("Cache hit for state=%s year=%s", state, year)
        return cached["data"]

    logger.info("Cache miss — fetching data for state=%s year=%s", state, year)

    # Step 1: fetch payments and physicians in parallel
    payments, physicians = await asyncio.gather(
        fetch_open_payments(state, year),
        fetch_npi_physicians(state),
    )

    # Step 2: fetch drugs based on company names from payments
    company_names = list({p["company"] for p in payments})
    drugs = await fetch_drugs(company_names)

    logger.info(
        "Fetched: %d payments, %d physicians, %d drugs for state=%s year=%s",
        len(payments), len(physicians), len(drugs), state, year,
    )

    # Step 3: assemble graph
    graph = build_graph(payments, physicians, drugs, state, year)

    # Store in cache
    _cache[cache_key] = {"data": graph, "ts": time.time()}

    return graph


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "cached_queries": len(_cache)}
