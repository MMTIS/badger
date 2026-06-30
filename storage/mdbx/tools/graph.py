import logging
from typing import Generator

from mdbx.mdbx import TXN

from domain.netex.services.model_typing import Tid
from utils.aux_logging import log_all
from storage.mdbx.core.implementation import (
    MdbxStorage,
    DB_ID_IDX,
    DB_REFERENCE_OUTWARD, DB_ID_IDX_FLAGS, DB_REFERENCE_OUTWARD_FLAGS,
)

from collections import defaultdict

from collections import defaultdict, deque
from typing import Dict, Set, List, Iterable, Any, Tuple

# --- 1) graph bouwen (nodes uit DB_ID_IDX, edges uit DB_REFERENCE_OUTWARD) ---
def build_graph(txn: TXN) -> Dict[bytes, Set[bytes]]:
    """
    Build adjacency list G where G[u] is the set of v such that u -> v (u references v).
    - include_referenced_nodes: if True, zorg dat ook target nodes van edges in de map verschijnen.
    """
    graph: Dict[bytes, Set[bytes]] = defaultdict(set)

    db_ids = txn.open_map(DB_ID_IDX, flags=DB_ID_IDX_FLAGS)
    cursor = txn.cursor(db_ids)
    for _, full_idx in cursor:
        graph.setdefault(full_idx, set())

    db_refs = txn.open_map(DB_REFERENCE_OUTWARD, flags=DB_REFERENCE_OUTWARD_FLAGS)
    cursor_r = txn.cursor(db_refs)
    for referencing_key, reference_key in cursor_r:
        # zorg dat refererende knoop in graph staat (soms kan referencing_key niet in DB_ID_IDX voorkomen)
        if referencing_key not in graph:
            graph.setdefault(referencing_key, set())
        graph[referencing_key].add(reference_key)
        if False and reference_key not in graph:
            graph.setdefault(reference_key, set())

    return graph


# -------------------------
# 2) Tarjan SCC (deterministisch)
# -------------------------
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

    for v in sorted(graph.keys()):  # sort voor determinisme
        if v not in indices:
            strongconnect(v)
    return result

# -------------------------
# 3) Condenseer naar SCC-DAG
# -------------------------
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
    # ensure all indices exist
    for i in range(len(sccs)):
        dag.setdefault(i, set())
    return dag, node_to_scc

# -------------------------
# Helpers: binnen-SCC ordering
# -------------------------
def sort_scc_by_internal_indegree(members: Iterable[bytes], graph: Dict[bytes, Set[bytes]], storage) -> List[bytes]:
    members_set = set(members)
    indeg = {n: 0 for n in members_set}
    for u in members_set:
        for v in graph.get(u, ()):
            if v in members_set:
                indeg[v] += 1
    # eerst hoge interne indegree (veel anderen verwijzen naar deze), dan type, dan key
    return sorted(members_set, key=lambda n: (-indeg[n], storage.serializer.full_key_to_idx(n)[0], n))

def greedy_minimize_forward_within_scc(members: Iterable[bytes], graph: Dict[bytes, Set[bytes]], storage, max_size: int = 500) -> List[bytes]:
    members = list(members)
    if len(members) > max_size:
        return sort_scc_by_internal_indegree(members, graph, storage)

    remaining = set(members)
    order: List[bytes] = []
    out_inside = {n: set(v for v in graph.get(n, ()) if v in remaining) for n in remaining}

    while remaining:
        best = None
        best_key = None
        for n in remaining:
            score = len(out_inside[n])                          # minder is beter
            cls_idx = storage.serializer.full_key_to_idx(n)[0]
            key = (score, cls_idx, n)
            if best is None or key < best_key:
                best = n
                best_key = key

        order.append(best)
        remaining.remove(best)
        # verwijder 'best' uit outgoing-sets van anderen
        for u in remaining:
            if best in out_inside[u]:
                out_inside[u].remove(best)
    return order

# -------------------------
# 4) Main: order_graph met Kahn op reversed DAG + class-based batching
# -------------------------
def order_graph(graph: Dict[bytes, Set[bytes]], storage, scc_lookahead_threshold: int = 500) -> List[bytes]:
    # SCCs en SCC-DAG
    sccs = strongly_connected_components(graph)
    dag, node_to_scc = build_scc_graph(graph, sccs)

    # Precompute preds (predecessors in original DAG) -- handig om indeg_rev updates te doen
    preds: Dict[int, Set[int]] = defaultdict(set)
    for u, vs in dag.items():
        for v in vs:
            preds[v].add(u)
    for i in range(len(sccs)):
        preds.setdefault(i, set())

    # indeg_rev = outdegree(original dag)  (we run Kahn on reversed DAG)
    indeg_rev: Dict[int, int] = {i: len(dag[i]) for i in range(len(sccs))}

    # Precompute per-SCC metadata and internal ordering
    scc_meta = {}
    for i, members in enumerate(sccs):
        # compute min class_idx as representative priority
        class_idxs = [storage.serializer.full_key_to_idx(n)[0] for n in members]
        min_class = min(class_idxs) if class_idxs else 0
        size = len(members)
        if size == 1:
            # singleton: deterministic single-member list (still sort by class_idx,key for stability)
            ordered_members = sorted(members, key=lambda n: (storage.serializer.full_key_to_idx(n)[0], n))
        else:
            if size <= scc_lookahead_threshold:
                ordered_members = greedy_minimize_forward_within_scc(members, graph, storage, max_size=scc_lookahead_threshold)
            else:
                ordered_members = sort_scc_by_internal_indegree(members, graph, storage)
        scc_meta[i] = {
            "members": members,
            "ordered_members": ordered_members,
            "min_class": min_class,
            "size": size
        }

    # initial available SCCs (those met indeg_rev == 0) -> sinks in original DAG
    available: Set[int] = {i for i, d in indeg_rev.items() if d == 0}

    result: List[bytes] = []
    # process while available
    while available:
        # determine current minimal class among available SCCs
        # (we pick smallest min_class to cluster that class first)
        class_to_sccs = defaultdict(list)
        for s in available:
            class_to_sccs[scc_meta[s]["min_class"]].append(s)
        current_class = min(class_to_sccs.keys())

        # process in a loop: keep processing SCCs with this current_class as they become available
        while True:
            # gather candidates currently available with this class
            candidates = [s for s in list(available) if scc_meta[s]["min_class"] == current_class]
            if not candidates:
                break
            # deterministic ordering among candidates (small SCCs first, then scc index)
            candidates_sorted = sorted(candidates, key=lambda s: (scc_meta[s]["size"], s))
            for s in candidates_sorted:
                # remove from available and append its ordered members
                available.remove(s)
                result.extend(scc_meta[s]["ordered_members"])
                # when 'removing' s, we decrement indeg_rev of its predecessors (in original graph)
                for p in preds[s]:
                    indeg_rev[p] -= 1
                    if indeg_rev[p] == 0:
                        available.add(p)
            # loop: there may be newly added SCCs with the same class -> will be processed in next iteration

        # after exhausting this class, continue outer loop to pick next class among available

    return result

# -------------------------
# 5) Diagnostiek: forward refs tellen (optioneel)
# -------------------------
def count_forward_refs(order: List[bytes], graph: Dict[bytes, Set[bytes]]) -> int:
    pos = {node: i for i, node in enumerate(order)}
    cnt = 0
    for u, vs in graph.items():
        pu = pos.get(u)
        if pu is None:
            continue
        for v in vs:
            pv = pos.get(v)
            # if pv is None: treat as forward (or ignore)
            if pv is None or pu < pv:
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
def export_objects(txn: TXN, storage: MdbxStorage) -> Iterable[Any]:
    log_all(logging.INFO, "[export_objects] building export order...")
    graph = build_graph(txn)
    order = order_graph(graph, storage)
    total_forwards = count_forward_refs(order, graph)
    total = len(order)
    log_all(logging.INFO, f"[export_objects] exporting {total} objects ({total_forwards} forward references)")
    # stream objects (consumed lazily during XML serialisation)
    for i, full_key in enumerate(order, start=1):
        if i % 100_000 == 0:
            log_all(logging.INFO, f"[export_objects] {i}/{total} objects exported...")
        yield storage.load_object_by_full_key(txn, full_key)
    log_all(logging.INFO, f"[export_objects] {total} objects exported")

