"""A data flow (or pipeline) as a graph of nodes and port-to-port edges.

OCI-DI links ports, not nodes: a ``FlowNode`` has ``outputLinks`` (each on one
of its operator's output ports, listing the keys of downstream ``InputLink``s
in ``toLinks``) and ``inputLinks`` (each on one of its input ports, naming the
upstream ``OutputLink`` in ``fromLink``). Either side alone is enough; both
are read so an export that fills in only one still links.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from ..di import mt, node_label


class GraphError(Exception):
    pass


@dataclass
class Edge:
    src: str               # node key
    src_port: str          # output port key ("" if unknown)
    dst: str
    dst_port: str          # input port key ("" if unknown)
    field_map: Optional[dict] = None
    condition: Optional[dict] = None   # CONDITIONAL_INPUT_LINK condition (pipelines)


@dataclass
class GNode:
    key: str
    label: str
    op_type: str
    raw: dict
    operator: dict
    in_ports: list = field(default_factory=list)
    out_ports: list = field(default_factory=list)
    names: set = field(default_factory=set)      # upper-cased names this node is referred to by

    def in_port_index(self, port_key: str) -> int:
        for i, p in enumerate(self.in_ports):
            if p.get("key") == port_key:
                return i
        return len(self.in_ports)

    def data_out_ports(self) -> list:
        ports = [p for p in self.out_ports if (p.get("portType") or "DATA") == "DATA"
                 and mt(p) in ("OUTPUT_PORT", "CONDITIONAL_OUTPUT_PORT", "")]
        return ports or self.out_ports


class FlowGraph:
    def __init__(self, flow: dict):
        self.flow = flow
        self.nodes: dict = {}
        self.edges: list = []
        for raw in flow.get("nodes") or []:
            op = raw.get("operator") or {}
            key = raw.get("key") or op.get("key") or node_label(raw)
            if key in self.nodes:
                raise GraphError(f"duplicate node key {key!r}")
            names = {n.upper() for n in (raw.get("name"), op.get("identifier"), op.get("name"))
                     if isinstance(n, str) and n}
            self.nodes[key] = GNode(key=key, label=node_label(raw), op_type=mt(op), raw=raw,
                                    operator=op, in_ports=list(op.get("inputPorts") or []),
                                    out_ports=list(op.get("outputPorts") or []), names=names)
        self._link()

    def _link(self) -> None:
        out_links, in_links = {}, {}
        for node in self.nodes.values():
            for ol in node.raw.get("outputLinks") or []:
                out_links[ol.get("key")] = (node.key, ol)
            for il in node.raw.get("inputLinks") or []:
                in_links[il.get("key")] = (node.key, il)
        seen = set()
        for in_key, (dst, il) in in_links.items():
            src = out_links.get(il.get("fromLink"))
            if src is None:
                continue
            src_node, ol = src
            pair = (ol.get("key"), in_key)
            seen.add(pair)
            self.edges.append(Edge(src_node, ol.get("port") or "", dst, il.get("port") or "",
                                   il.get("fieldMap"), il.get("condition")))
        for out_key, (src_node, ol) in out_links.items():
            for to in ol.get("toLinks") or []:
                if (out_key, to) in seen or to not in in_links:
                    continue
                dst, il = in_links[to]
                self.edges.append(Edge(src_node, ol.get("port") or "", dst, il.get("port") or "",
                                       il.get("fieldMap"), il.get("condition")))

    # ---------------------------------------------------------------- queries
    def inputs(self, key: str) -> list:
        node = self.nodes[key]
        edges = [e for e in self.edges if e.dst == key]
        return sorted(edges, key=lambda e: (node.in_port_index(e.dst_port),
                                            list(self.nodes).index(e.src)))

    def outputs(self, key: str) -> list:
        return [e for e in self.edges if e.src == key]

    def order(self) -> list:
        """Topological order, stable by the order nodes appear in the JSON."""
        position = {k: i for i, k in enumerate(self.nodes)}
        indeg = {k: 0 for k in self.nodes}
        for e in self.edges:
            indeg[e.dst] += 1
        ready = sorted((k for k, d in indeg.items() if d == 0), key=position.get)
        out = []
        while ready:
            k = ready.pop(0)
            out.append(self.nodes[k])
            for e in self.outputs(k):
                indeg[e.dst] -= 1
                if indeg[e.dst] == 0:
                    ready.append(e.dst)
                    ready.sort(key=position.get)
        if len(out) != len(self.nodes):
            stuck = sorted(self.nodes[k].label for k, d in indeg.items() if d > 0)
            raise GraphError(f"the flow has a cycle through {', '.join(stuck)}")
        return out

    def ancestors(self, key: str) -> set:
        seen, stack = set(), [key]
        while stack:
            k = stack.pop()
            for e in self.edges:
                if e.dst == k and e.src not in seen:
                    seen.add(e.src)
                    stack.append(e.src)
        return seen

    def names_through(self, key: str) -> set:
        """Upper-cased names of ``key`` and every node upstream of it."""
        names = set(self.nodes[key].names)
        for a in self.ancestors(key):
            names |= self.nodes[a].names
        return names
