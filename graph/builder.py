import logging
import networkx as nx

from graph.models import Edge, GraphMeta, GraphResponse, Node

logger = logging.getLogger(__name__)

MAX_NODES = 200
MAX_EDGES = 400
MAX_PEER_EDGES = 100

TAXONOMY_CONDITION_MAP: dict[str, list[str]] = {
    "Cardiology":        ["I48", "I50"],
    "Endocrinology":     ["E11"],
    "Internal Medicine": ["I10", "E11"],
    "Oncology":          ["C80"],
    "Neurology":         ["G35", "I63"],
}


def _slugify(name: str) -> str:
    return name.lower().strip().replace(" ", "_").replace("/", "_")


# ---------------------------------------------------------------------------
# Node builders
# ---------------------------------------------------------------------------

def _add_pharma_nodes(G: nx.DiGraph, payments: list[dict]) -> None:
    company_stats: dict[str, dict] = {}
    for p in payments:
        company = p["company"]
        slug = _slugify(company)
        if slug not in company_stats:
            company_stats[slug] = {"label": company, "total_paid": 0.0, "physicians": set()}
        company_stats[slug]["total_paid"] += p["amount"]
        company_stats[slug]["physicians"].add(p["npi"])

    for slug, stats in company_stats.items():
        G.add_node(
            slug,
            type="pharma",
            label=stats["label"],
            props={
                "total_paid": round(stats["total_paid"], 2),
                "num_physicians": len(stats["physicians"]),
            },
        )


def _add_drug_nodes(G: nx.DiGraph, drugs: list[dict]) -> None:
    for drug in drugs:
        G.add_node(
            drug["id"],
            type="drug",
            label=drug["brand"] or drug["generic"],
            props={
                "generic_name": drug["generic"],
                "conditions": [c["name"] for c in drug["conditions"]],
            },
        )


def _add_condition_nodes(G: nx.DiGraph, drugs: list[dict]) -> None:
    seen: set[str] = set()
    for drug in drugs:
        for cond in drug["conditions"]:
            node_id = f"cond_{cond['icd10']}"
            if node_id not in seen:
                G.add_node(
                    node_id,
                    type="condition",
                    label=cond["name"],
                    props={"icd10_code": cond["icd10"]},
                )
                seen.add(node_id)


def _add_physician_nodes(G: nx.DiGraph, physicians: list[dict], payments: list[dict]) -> None:
    # Aggregate total received per NPI from payments
    totals: dict[str, float] = {}
    for p in payments:
        totals[p["npi"]] = totals.get(p["npi"], 0.0) + p["amount"]

    # Add physicians from NPI Registry (rich data — specialty, city, state)
    npi_seen: set[str] = set()
    for ph in physicians:
        node_id = f"npi_{ph['npi']}"
        G.add_node(
            node_id,
            type="physician",
            label=ph["full_name"],
            props={
                "npi": ph["npi"],
                "specialty": ph["specialty"],
                "city": ph["city"],
                "state": ph["state"],
                "total_received": round(totals.get(ph["npi"], 0.0), 2),
            },
        )
        npi_seen.add(ph["npi"])

    # Also add physicians from payments who weren't in the NPI query results
    # (e.g. different specialty, different state of practice)
    for p in payments:
        if p["npi"] in npi_seen:
            continue
        node_id = f"npi_{p['npi']}"
        if not G.has_node(node_id):
            full_name = f"Dr. {p['physician_first']} {p['physician_last']}".strip()
            G.add_node(
                node_id,
                type="physician",
                label=full_name if full_name != "Dr." else f"Dr. (NPI {p['npi']})",
                props={
                    "npi": p["npi"],
                    "specialty": "",
                    "city": "",
                    "state": "",
                    "total_received": round(totals.get(p["npi"], 0.0), 2),
                },
            )
            npi_seen.add(p["npi"])


# ---------------------------------------------------------------------------
# Edge builders
# ---------------------------------------------------------------------------

def _add_manufactures_edges(G: nx.DiGraph, drugs: list[dict]) -> None:
    for drug in drugs:
        pharma_slug = _slugify(drug["manufacturer"])
        if G.has_node(pharma_slug) and G.has_node(drug["id"]):
            G.add_edge(pharma_slug, drug["id"], type="MANUFACTURES", weight=1.0, props={})


def _add_indicated_for_edges(G: nx.DiGraph, drugs: list[dict]) -> None:
    for drug in drugs:
        for cond in drug["conditions"]:
            cond_id = f"cond_{cond['icd10']}"
            if G.has_node(drug["id"]) and G.has_node(cond_id):
                G.add_edge(drug["id"], cond_id, type="INDICATED_FOR", weight=1.0, props={})


def _add_specializes_in_edges(G: nx.DiGraph, physicians: list[dict]) -> None:
    for ph in physicians:
        physician_id = f"npi_{ph['npi']}"
        specialty = ph.get("specialty", "")
        # Match specialty string to condition IDs via taxonomy map
        for taxonomy_key, icd10_list in TAXONOMY_CONDITION_MAP.items():
            if taxonomy_key.lower() in specialty.lower():
                for icd10 in icd10_list:
                    cond_id = f"cond_{icd10}"
                    if G.has_node(physician_id) and G.has_node(cond_id):
                        G.add_edge(physician_id, cond_id, type="SPECIALIZES_IN", weight=1.0, props={})


def _add_paid_edges(G: nx.DiGraph, payments: list[dict]) -> None:
    for p in payments:
        pharma_slug = _slugify(p["company"])
        physician_id = f"npi_{p['npi']}"
        if G.has_node(pharma_slug) and G.has_node(physician_id):
            # If multiple payments exist between same pair, accumulate weight
            if G.has_edge(pharma_slug, physician_id):
                G[pharma_slug][physician_id]["weight"] += p["amount"]
            else:
                G.add_edge(
                    pharma_slug,
                    physician_id,
                    type="PAID",
                    weight=p["amount"],
                    props={
                        "drug": p["drug"],
                        "nature": p["nature"],
                        "date": p["date"],
                    },
                )


def _add_received_for_edges(G: nx.DiGraph, payments: list[dict], drugs: list[dict]) -> None:
    # Build a lookup: normalized drug name → drug node ID
    drug_lookup: dict[str, str] = {}
    for drug in drugs:
        for name in [drug["brand"], drug["generic"]]:
            if name:
                drug_lookup[name.lower()] = drug["id"]

    for p in payments:
        if not p["drug"]:
            continue
        drug_id = drug_lookup.get(p["drug"].lower())
        physician_id = f"npi_{p['npi']}"
        if drug_id and G.has_node(physician_id) and G.has_node(drug_id):
            if G.has_edge(physician_id, drug_id):
                G[physician_id][drug_id]["weight"] += p["amount"]
            else:
                G.add_edge(
                    physician_id,
                    drug_id,
                    type="RECEIVED_FOR",
                    weight=p["amount"],
                    props={},
                )


def _add_peer_of_edges(G: nx.DiGraph, payments: list[dict], physicians: list[dict]) -> None:
    # Map NPI → specialty
    specialty_map: dict[str, str] = {ph["npi"]: ph["specialty"] for ph in physicians}

    # Map NPI → set of pharma slugs that paid them
    payers_map: dict[str, set[str]] = {}
    for p in payments:
        npi = p["npi"]
        slug = _slugify(p["company"])
        payers_map.setdefault(npi, set()).add(slug)

    npi_list = list(specialty_map.keys())
    peer_count = 0

    for i in range(len(npi_list)):
        if peer_count >= MAX_PEER_EDGES:
            break
        for j in range(i + 1, len(npi_list)):
            if peer_count >= MAX_PEER_EDGES:
                break
            a, b = npi_list[i], npi_list[j]
            same_specialty = specialty_map.get(a) == specialty_map.get(b) and specialty_map.get(a)
            shared_payer = payers_map.get(a, set()) & payers_map.get(b, set())
            if same_specialty and shared_payer:
                node_a, node_b = f"npi_{a}", f"npi_{b}"
                if G.has_node(node_a) and G.has_node(node_b):
                    G.add_edge(node_a, node_b, type="PEER_OF", weight=1.0, props={})
                    peer_count += 1


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------

def _serialize(G: nx.DiGraph, state: str, year: int) -> GraphResponse:
    all_nodes = [
        Node(
            id=node_id,
            type=attrs["type"],
            label=attrs["label"],
            props=attrs.get("props", {}),
        )
        for node_id, attrs in G.nodes(data=True)
    ]

    all_edges = [
        Edge(
            source=u,
            target=v,
            type=attrs["type"],
            weight=attrs.get("weight", 1.0),
            props=attrs.get("props", {}),
        )
        for u, v, attrs in G.edges(data=True)
    ]

    # --- Node truncation ---
    # Always keep anchor nodes (pharma, drug, condition) — they are few and
    # structurally important. Only truncate physicians when over the limit.
    anchor_nodes = [n for n in all_nodes if n.type != "physician"]
    physician_nodes = sorted(
        [n for n in all_nodes if n.type == "physician"],
        key=lambda n: n.props.get("total_received", 0),
        reverse=True,
    )
    physician_slots = MAX_NODES - len(anchor_nodes)
    nodes = anchor_nodes + physician_nodes[:max(physician_slots, 0)]
    kept_ids = {n.id for n in nodes}

    # --- Edge truncation ---
    # Keep all structural edges whose endpoints survived node truncation.
    # For PAID edges (potentially hundreds), sort by weight and cap separately.
    structural_edges = [
        e for e in all_edges
        if e.type != "PAID" and e.source in kept_ids and e.target in kept_ids
    ]
    paid_edges = sorted(
        [e for e in all_edges if e.type == "PAID" and e.source in kept_ids and e.target in kept_ids],
        key=lambda e: e.weight,
        reverse=True,
    )
    edges = structural_edges + paid_edges
    if len(edges) > MAX_EDGES:
        edges = edges[:MAX_EDGES]

    return GraphResponse(
        nodes=nodes,
        edges=edges,
        meta=GraphMeta(
            node_count=len(nodes),
            edge_count=len(edges),
            state=state,
            year=year,
            sources=["NPI Registry", "OpenFDA", "CMS Open Payments"],
        ),
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def build_graph(
    payments: list[dict],
    physicians: list[dict],
    drugs: list[dict],
    state: str,
    year: int,
) -> GraphResponse:
    G = nx.DiGraph()

    # 1. Add all nodes
    _add_pharma_nodes(G, payments)
    _add_drug_nodes(G, drugs)
    _add_condition_nodes(G, drugs)
    _add_physician_nodes(G, physicians, payments)

    # 2. Add explicit edges
    _add_manufactures_edges(G, drugs)
    _add_indicated_for_edges(G, drugs)
    _add_specializes_in_edges(G, physicians)
    _add_paid_edges(G, payments)
    _add_received_for_edges(G, payments, drugs)

    # 3. Derive PEER_OF edges
    _add_peer_of_edges(G, payments, physicians)

    logger.info(
        "Graph built: %d nodes, %d edges for state=%s year=%s",
        G.number_of_nodes(), G.number_of_edges(), state, year,
    )

    return _serialize(G, state, year)
