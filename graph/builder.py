import logging
import networkx as nx

from graph.models import Edge, GraphMeta, GraphResponse, Node

logger = logging.getLogger(__name__)

MAX_EDGES = 1200
MAX_PEER_EDGES = 400
MAX_DEVICE_NODES = 60   # Cap device nodes; keep by total payment volume
MAX_DRUG_NODES   = 80   # Cap drug nodes; keep those with most condition links
MAX_PHYSICIAN_NODES = 200

TAXONOMY_CONDITION_MAP: dict[str, list[str]] = {
    "Cardiology":        ["I48", "I50"],
    "Endocrinology":     ["E11"],
    "Internal Medicine": ["I10", "E11"],
    "Oncology":          ["C80"],
    "Neurology":         ["G35", "I63"],
}


def _slugify(name: str) -> str:
    s = name.lower().strip().replace(" ", "_").replace("/", "_")
    for c in ".,'":
        s = s.replace(c, "")
    return s or "unknown"


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


def _build_drug_lookup(drugs: list[dict]) -> dict[str, str]:
    """Normalized product name -> drug node id (for matching payment product to drug)."""
    lookup: dict[str, str] = {}
    for drug in drugs:
        for name in [drug.get("brand"), drug.get("generic")]:
            if name:
                lookup[name.lower().strip()] = drug["id"]
    return lookup


def _add_device_nodes(G: nx.DiGraph, payments: list[dict], drugs: list[dict]) -> dict[tuple[str, str], str]:
    """
    Add device/product nodes from CMS payment product field when it's not a known drug.
    Returns lookup (company_slug, product_name_lower) -> device_id for RECEIVED_FOR edges.
    """
    drug_lookup = _build_drug_lookup(drugs)
    device_agg: dict[tuple[str, str], tuple[str, str, float]] = {}
    for p in payments:
        product = (p.get("drug") or "").strip()
        if not product:
            continue
        key_lower = product.lower()
        if key_lower in drug_lookup:
            continue
        company = p["company"]
        c_slug = _slugify(company)
        key = (c_slug, key_lower)
        if key not in device_agg:
            device_agg[key] = (company, product, 0.0)
        company_label, product_label, total = device_agg[key]
        device_agg[key] = (company_label, product_label, total + p["amount"])

    sorted_devices = sorted(device_agg.items(), key=lambda x: -x[1][2])[:MAX_DEVICE_NODES]
    device_lookup: dict[tuple[str, str], str] = {}
    for (c_slug, key_lower), (company_label, product_label, total) in sorted_devices:
        device_id = f"device_{c_slug}_{_slugify(product_label)}"
        G.add_node(
            device_id,
            type="device",
            label=product_label,
            props={
                "manufacturer": company_label,
                "total_payments": round(total, 2),
            },
        )
        device_lookup[(c_slug, key_lower)] = device_id
    return device_lookup


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


def _add_manufactures_device_edges(G: nx.DiGraph) -> None:
    for node_id, attrs in G.nodes(data=True):
        if attrs.get("type") != "device":
            continue
        manufacturer = (attrs.get("props") or {}).get("manufacturer", "")
        if not manufacturer:
            continue
        pharma_slug = _slugify(manufacturer)
        if G.has_node(pharma_slug):
            G.add_edge(pharma_slug, node_id, type="MANUFACTURES", weight=1.0, props={})


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


def _add_received_for_edges(G: nx.DiGraph, payments: list[dict], drugs: list[dict], device_lookup: dict[tuple[str, str], str] | None = None) -> None:
    drug_lookup = _build_drug_lookup(drugs)
    if device_lookup is None:
        device_lookup = {}

    for p in payments:
        if not p["drug"]:
            continue
        product_lower = p["drug"].lower()
        physician_id = f"npi_{p['npi']}"
        drug_id = drug_lookup.get(product_lower)
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
            continue
        c_slug = _slugify(p["company"])
        device_id = device_lookup.get((c_slug, product_lower))
        if device_id and G.has_node(physician_id) and G.has_node(device_id):
            if G.has_edge(physician_id, device_id):
                G[physician_id][device_id]["weight"] += p["amount"]
            else:
                G.add_edge(
                    physician_id,
                    device_id,
                    type="RECEIVED_FOR",
                    weight=p["amount"],
                    props={},
                )


def _add_peer_of_edges(G: nx.DiGraph, physicians: list[dict]) -> None:
    specialty_map: dict[str, str] = {ph["npi"]: ph["specialty"] for ph in physicians}
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
            if same_specialty:
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
    # Priority: all pharma, all conditions, top drugs (by condition count),
    # top devices (by payments), then top physicians (by total_received).
    pharma_nodes    = [n for n in all_nodes if n.type == "pharma"]
    condition_nodes = [n for n in all_nodes if n.type == "condition"]
    drug_nodes = sorted(
        [n for n in all_nodes if n.type == "drug"],
        key=lambda n: len(n.props.get("conditions", [])),
        reverse=True,
    )[:MAX_DRUG_NODES]
    device_nodes = sorted(
        [n for n in all_nodes if n.type == "device"],
        key=lambda n: n.props.get("total_payments", 0),
        reverse=True,
    )[:MAX_DEVICE_NODES]
    physician_nodes = sorted(
        [n for n in all_nodes if n.type == "physician"],
        key=lambda n: n.props.get("total_received", 0),
        reverse=True,
    )[:MAX_PHYSICIAN_NODES]

    nodes = pharma_nodes + condition_nodes + drug_nodes + device_nodes + physician_nodes
    kept_ids = {n.id for n in nodes}

    # --- Edge truncation ---
    # Keep all structural edges whose endpoints survived node truncation.
    # For PAID edges: reserve up to 20 per pharma (by weight) so each pharma keeps physicians;
    # then fill remaining slots with highest-weight PAID edges overall.
    structural_edges = [
        e for e in all_edges
        if e.type != "PAID" and e.source in kept_ids and e.target in kept_ids
    ]
    paid_all = [
        e for e in all_edges
        if e.type == "PAID" and e.source in kept_ids and e.target in kept_ids
    ]
    by_pharma: dict[str, list] = {}
    for e in paid_all:
        by_pharma.setdefault(e.source, []).append(e)
    paid_per_pharma: list[Edge] = []
    for _source, es in by_pharma.items():
        paid_per_pharma.extend(sorted(es, key=lambda x: x.weight, reverse=True)[:20])
    paid_edges = sorted(paid_per_pharma, key=lambda e: e.weight, reverse=True)
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
    device_lookup = _add_device_nodes(G, payments, drugs)
    _add_physician_nodes(G, physicians, payments)

    # 2. Add explicit edges
    _add_manufactures_edges(G, drugs)
    _add_manufactures_device_edges(G)
    _add_indicated_for_edges(G, drugs)
    _add_specializes_in_edges(G, physicians)
    _add_paid_edges(G, payments)
    _add_received_for_edges(G, payments, drugs, device_lookup)

    # 3. Derive PEER_OF edges
    _add_peer_of_edges(G, physicians)

    logger.info(
        "Graph built: %d nodes, %d edges for state=%s year=%s",
        G.number_of_nodes(), G.number_of_edges(), state, year,
    )

    return _serialize(G, state, year)
