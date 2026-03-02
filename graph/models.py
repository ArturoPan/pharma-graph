from pydantic import BaseModel
from typing import Any, Literal

NodeType = Literal["pharma", "drug", "condition", "physician", "device"]
EdgeType = Literal["MANUFACTURES", "INDICATED_FOR", "SPECIALIZES_IN", "PAID", "RECEIVED_FOR", "PEER_OF"]


class Node(BaseModel):
    id: str
    type: NodeType
    label: str
    props: dict[str, Any] = {}


class Edge(BaseModel):
    source: str
    target: str
    type: EdgeType
    weight: float = 1.0
    props: dict[str, Any] = {}


class GraphMeta(BaseModel):
    node_count: int
    edge_count: int
    state: str
    year: int
    sources: list[str]


class GraphResponse(BaseModel):
    nodes: list[Node]
    edges: list[Edge]
    meta: GraphMeta
