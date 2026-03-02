import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from fetchers.npi import fetch_npi_physicians
from fetchers.open_payments import fetch_open_payments
from fetchers.openfda import fetch_drugs
from graph.builder import build_graph
from graph.models import GraphResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Disk cache — persists across server restarts
# In-memory cache — avoids re-reading disk within the same session
# ---------------------------------------------------------------------------
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

_memory_cache: dict[tuple[str, int], GraphResponse] = {}

VALID_YEARS = {2018, 2019, 2020, 2021, 2022, 2023, 2024}  # years with confirmed CMS dataset IDs
US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC",
}


# Bump when graph schema changes (e.g. added device nodes) so old disk cache is ignored
CACHE_VERSION = 2


def _disk_path(state: str, year: int) -> Path:
    return DATA_DIR / f"{state}_{year}.json"


def _load_from_disk(state: str, year: int) -> GraphResponse | None:
    path = _disk_path(state, year)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        if data.get("cache_version") != CACHE_VERSION:
            logger.info("Disk cache outdated (version %s) for state=%s year=%s — will refetch", data.get("cache_version"), state, year)
            return None
        graph = GraphResponse(**data["graph"])
        logger.info("Disk cache hit for state=%s year=%s", state, year)
        return graph
    except Exception as exc:
        logger.warning("Failed to load disk cache for %s/%s: %s", state, year, exc)
        return None


def _save_to_disk(state: str, year: int, graph: GraphResponse) -> None:
    try:
        payload = {"cache_version": CACHE_VERSION, "graph": graph.model_dump()}
        _disk_path(state, year).write_text(json.dumps(payload))
        logger.info("Saved to disk cache: %s_%s.json", state, year)
    except Exception as exc:
        logger.warning("Failed to save disk cache: %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Impiricus Graph API starting up")
    cached = list(DATA_DIR.glob("*.json"))
    if cached:
        logger.info("Disk cache contains: %s", [f.name for f in cached])
    yield
    logger.info("Impiricus Graph API shutting down")


app = FastAPI(
    title="Impiricus Clinical Intelligence Graph",
    description="Visualizes the pharma-physician influence network using public US government data.",
    version="1.0.0",
    lifespan=lifespan,
    openapi_url="/api/openapi.json",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
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

    cache_key = (state, year)

    # 1. In-memory cache (fastest)
    if cache_key in _memory_cache:
        logger.info("Memory cache hit for state=%s year=%s", state, year)
        return _memory_cache[cache_key]

    # 2. Disk cache (survives server restarts)
    graph = _load_from_disk(state, year)
    if graph:
        _memory_cache[cache_key] = graph
        return graph

    # 3. Live fetch from APIs
    logger.info("No cache — fetching live data for state=%s year=%s", state, year)

    payments, physicians = await asyncio.gather(
        fetch_open_payments(state, year),
        fetch_npi_physicians(state),
    )

    company_names = list({p["company"] for p in payments})
    drugs = await fetch_drugs(company_names)

    logger.info(
        "Fetched: %d payments, %d physicians, %d drugs for state=%s year=%s",
        len(payments), len(physicians), len(drugs), state, year,
    )

    graph = build_graph(payments, physicians, drugs, state, year)

    _memory_cache[cache_key] = graph
    _save_to_disk(state, year, graph)

    return graph


@app.get("/health")
async def health() -> dict:
    cached = [f.stem for f in DATA_DIR.glob("*.json")]
    return {"status": "ok", "memory_cache": len(_memory_cache), "disk_cache": cached}


# Serve frontend — must be mounted last so API routes take priority
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
