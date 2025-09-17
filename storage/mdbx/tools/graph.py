from typing import Generator

from mdbx.mdbx import TXN

from domain.netex.services.model_typing import Tid
from storage.mdbx.core.implementation import (
    MdbxStorage,
    DB_ID_IDX,
    DB_REFERENCE_OUTWARD,
)

from collections import defaultdict

from collections import defaultdict, deque
from typing import Dict, Set, List, Iterable, Any, Tuple

# --- 1) graph bouwen (nodes uit DB_ID_IDX, edges uit DB_REFERENCE_OUTWARD) ---
def build_graph(txn: TXN, storage: "MdbxStorage", include_referenced_nodes: bool = True) -> Dict[bytes, Set[bytes]]:
    """
    Build adjacency list G where G[u] is the set of v such that u -> v (u references v).
    - include_referenced_nodes: if True, zorg dat ook target nodes van edges in de map verschijnen.
    """
    graph: Dict[bytes, Set[bytes]] = defaultdict(set)

    db_ids = txn.open_map(DB_ID_IDX)
    cursor = txn.cursor(db_ids)
    for full_key, _ in cursor:
        graph.setdefault(full_key, set())

    db_refs = txn.open_map(DB_REFERENCE_OUTWARD)
    cursor_r = txn.cursor(db_refs)
    for referencing_key, reference_key in cursor_r:
        # zorg dat refererende knoop in graph staat (soms kan referencing_key niet in DB_ID_IDX voorkomen)
        if referencing_key not in graph:
            graph.setdefault(referencing_key, set())
        graph[referencing_key].add(reference_key)
        if include_referenced_nodes and reference_key not in graph:
            graph.setdefault(reference_key, set())

    return graph

# --- 2) Tarjan's SCC (standaard) ---
def strongly_connected_components(graph: Dict[bytes, Set[bytes]]) -> List[List[bytes]]:
    index = 0
    indices: Dict[bytes, int] = {}
    lowlink: Dict[bytes, int] = {}
    onstack: Set[bytes] = set()
    stack: List[bytes] = []
    result: List[List[bytes]] = []

    def strongconnect(v: bytes):
        nonlocal index
        indices[v] = index
        lowlink[v] = index
        index += 1
        stack.append(v)
        onstack.add(v)

        for w in graph.get(v, ()):
            if w not in indices:
                strongconnect(w)
                lowlink[v] = min(lowlink[v], lowlink[w])
            elif w in onstack:
                lowlink[v] = min(lowlink[v], indices[w])

        if lowlink[v] == indices[v]:
            comp: List[bytes] = []
            while True:
                w = stack.pop()
                onstack.remove(w)
                comp.append(w)
                if w == v:
                    break
            result.append(comp)

    # deterministic traversal: sort keys for stable output
    for v in sorted(graph.keys()):
        if v not in indices:
            strongconnect(v)

    return result

# --- 3) condenseer naar SCC-DAG ---
def build_scc_graph(graph: Dict[bytes, Set[bytes]], sccs: List[List[bytes]]) -> Tuple[Dict[int, Set[int]], Dict[bytes, int]]:
    node_to_scc: Dict[bytes, int] = {}
    for i, comp in enumerate(sccs):
        for node in comp:
            node_to_scc[node] = i

    dag: Dict[int, Set[int]] = defaultdict(set)
    for u, vs in graph.items():
        cu = node_to_scc[u]
        for v in vs:
            cv = node_to_scc.get(v)
            if cv is None:
                continue
            if cu != cv:
                dag[cu].add(cv)
    # ensure all scc indices in dag
    for i in range(len(sccs)):
        dag.setdefault(i, set())

    return dag, node_to_scc

# --- 4) topologische sort (Kahn) op dag ---
def topo_sort(dag: Dict[int, Set[int]]) -> List[int]:
    indeg = {u: 0 for u in dag}
    for u, vs in dag.items():
        for v in vs:
            indeg[v] = indeg.get(v, 0) + 1

    q = deque([u for u, d in indeg.items() if d == 0])
    res: List[int] = []
    while q:
        u = q.popleft()
        res.append(u)
        for v in dag.get(u, ()):
            indeg[v] -= 1
            if indeg[v] == 0:
                q.append(v)
    return res

# --- helper: fallback sort binnen SCC (snelle optie) ---
def sort_scc_by_internal_indegree(members: Iterable[bytes], graph: Dict[bytes, Set[bytes]], storage) -> List[bytes]:
    members_set = set(members)
    indeg = {n: 0 for n in members_set}
    for u in members_set:
        for v in graph.get(u, ()):
            if v in members_set:
                indeg[v] += 1
    # sort: eerst hoge interne indegree (veel anderen verwijzen naar deze), dan type, dan stable key
    return sorted(members_set, key=lambda n: (-indeg[n], storage.serializer.full_key_to_idx(n)[0], n))

# --- helper: greedy lookahead binnen kleine SCCs (minimize forward refs) ---
def greedy_minimize_forward_within_scc(members: Iterable[bytes], graph: Dict[bytes, Set[bytes]], storage, max_size: int = 500) -> List[bytes]:
    members = list(members)
    if len(members) > max_size:
        return sort_scc_by_internal_indegree(members, graph, storage)

    remaining = set(members)
    order: List[bytes] = []
    # precompute outgoing edges inside SCC
    out_inside = {n: set(v for v in graph.get(n, ()) if v in remaining) for n in remaining}

    while remaining:
        # score = number of outgoing edges to still-remaining nodes (we want minimal)
        # tie-break: prefer nodes with same type to cluster
        best = None
        best_score = None
        for n in remaining:
            score = len(out_inside[n])
            cls_idx = storage.serializer.full_key_to_idx(n)[0]
            key = (score, cls_idx, n)  # smaller score better
            if best is None or key < best_score:
                best = n
                best_score = key

        # place best
        order.append(best)
        remaining.remove(best)
        # update out_inside: remove best from others' outgoing sets if present
        for u in list(remaining):
            if best in out_inside[u]:
                out_inside[u].remove(best)
    return order

# --- 5) hoofdorde functie (corrigeert richting door reverse topo) ---
def order_graph(graph: Dict[bytes, Set[bytes]], storage, scc_lookahead_threshold: int = 500) -> List[bytes]:
    sccs = strongly_connected_components(graph)
    dag, node_to_scc = build_scc_graph(graph, sccs)
    topo = topo_sort(dag)  # topo on original DAG where edges mean "A references B": A -> B
    # Important: we want referenced SCCs before referencing SCCs, so reverse topo
    scc_order = list(reversed(topo))

    result: List[bytes] = []
    for scc_idx in scc_order:
        members = sccs[scc_idx]
        if len(members) == 1:
            result.extend(members)
        else:
            # prefer the greedy minimization for small SCCs
            if len(members) <= scc_lookahead_threshold:
                ordered = greedy_minimize_forward_within_scc(members, graph, storage, max_size=scc_lookahead_threshold)
            else:
                ordered = sort_scc_by_internal_indegree(members, graph, storage)
            result.extend(ordered)
    return result

# --- 6) teller en diagnostiek ---
def count_forward_refs(order: List[bytes], graph: Dict[bytes, Set[bytes]]) -> int:
    pos = {node: i for i, node in enumerate(order)}
    cnt = 0
    for u, vs in graph.items():
        pu = pos.get(u)
        if pu is None:
            # u not in order -> ignore or treat as forward
            continue
        for v in vs:
            pv = pos.get(v)
            if pv is None:
                # referenced node not in order: treat as forward (optional)
                cnt += 1
            else:
                if pu < pv:
                    # u appears before v => forward reference
                    cnt += 1
    return cnt

def list_forward_examples(order: List[bytes], graph: Dict[bytes, Set[bytes]], limit: int = 20) -> List[Tuple[bytes, bytes]]:
    pos = {node: i for i, node in enumerate(order)}
    out = []
    for u, vs in graph.items():
        pu = pos.get(u)
        if pu is None:
            continue
        for v in vs:
            pv = pos.get(v)
            if pv is None or pu < pv:
                out.append((u, v))
                if len(out) >= limit:
                    return out
    return out

# --- 7) streaming export generator ---
def export_objects(txn: TXN, storage: "MdbxStorage") -> Iterable[Any]:
    graph = build_graph(txn, storage, include_referenced_nodes=True)
    order = order_graph(graph, storage)
    # quick diagnostics (log when you want)
    total_forwards = count_forward_refs(order, graph)
    # optional: print or log
    print(f"[export_objects] nodes={len(order)} forward_refs={total_forwards}")
    # stream objects
    for full_key in order:
        yield storage.load_object_by_full_key(txn, full_key)

