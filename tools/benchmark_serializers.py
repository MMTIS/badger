from __future__ import annotations

import argparse
from pathlib import Path
import sys
import time
from typing import Any, List

from storage.objectserializer.cloudpickle.serializer import CloudPickleSerializer
from storage.objectserializer.codecs.lz4 import Lz4Codec
from storage.objectserializer.msgspec.serializer import MsgspecSerializer
from storage.objectserializer.pipeline import PipelineSerializer


def benchmark_entity_list(entities: List[Any], label: str = "Entities") -> None:
    count = len(entities)
    if count == 0:
        print("No entities found to benchmark.")
        return

    print(f"\n================================================================================")
    print(f"🚀 Badger Serialization Benchmark: {label} ({count:,} objects)")
    print(f"   Comparing CloudPickle vs. Msgspec (MessagePack) with LZ4 Compression")
    print(f"================================================================================\n")

    pipelines = [
        ("CloudPickle + LZ4 (Production Baseline)", PipelineSerializer(CloudPickleSerializer(), [Lz4Codec()])),
        ("Msgspec + LZ4 (Proposed)", PipelineSerializer(MsgspecSerializer(), [Lz4Codec()])),
        ("Msgspec Raw (No LZ4)", MsgspecSerializer()),
    ]

    results = []
    base_dumps_ops = 0.0
    base_size = 0.0

    for name, serializer in pipelines:
        # Measure Dumps (Serialization)
        t0 = time.perf_counter()
        payloads = [serializer.dumps(e) for e in entities]
        dumps_time = time.perf_counter() - t0

        total_bytes = sum(len(p) for p in payloads)
        avg_size = total_bytes / count
        dumps_ops = count / dumps_time
        dumps_mb = (total_bytes / (1024 * 1024)) / dumps_time

        # Measure Loads (Deserialization)
        t0 = time.perf_counter()
        _ = [serializer.loads(p) for p in payloads]
        loads_time = time.perf_counter() - t0

        loads_ops = count / loads_time
        loads_mb = (total_bytes / (1024 * 1024)) / loads_time

        if "CloudPickle" in name:
            base_dumps_ops = dumps_ops
            base_size = avg_size

        speedup = f"{dumps_ops / base_dumps_ops:.1f}x" if base_dumps_ops else "1.0x"
        savings = f"{(1 - avg_size / base_size) * 100:.1f}%" if base_size else "0.0%"

        results.append({
            "name": name,
            "dumps_ops": dumps_ops,
            "dumps_mb": dumps_mb,
            "loads_ops": loads_ops,
            "loads_mb": loads_mb,
            "avg_size": avg_size,
            "speedup": speedup,
            "savings": savings,
        })

    # Print results table in GitHub-flavored Markdown
    print("| Serializer Pipeline | Dumps (Ops/s) | Dumps (MB/s) | Loads (Ops/s) | Payload Size | Dumps Speedup | Size Savings |")
    print("| :--- | :---: | :---: | :---: | :---: | :---: | :---: |")
    for r in results:
        print(f"| **{r['name']}** | {r['dumps_ops']:,.0f} ops/s | {r['dumps_mb']:.1f} MB/s | {r['loads_ops']:,.0f} ops/s | {r['avg_size']:.0f} B | **{r['speedup']}** | **{r['savings']}** |")

    print("\n================================================================================\n")


def generate_sample_entities(count: int = 10_000) -> List[Any]:
    """Generate synthetic NeTEx ScheduledStopPoint entities for benchmarking."""
    from domain.netex.model import ScheduledStopPoint, MultilingualString, TextType

    print(f"Generating {count:,} synthetic ScheduledStopPoint entities...")
    entities = []
    for i in range(count):
        stop = ScheduledStopPoint(
            id=f"SP_{i + 1}",
            version="1",
            name=MultilingualString(
                content=[
                    TextType(value=f"Station Stop {i + 1}", lang="de"),
                    TextType(value=f"Gare Central {i + 1}", lang="fr"),
                ]
            ),
        )
        entities.append(stop)
    return entities


def benchmark_mdbx_file(db_path: Path, max_samples: int = 10_000) -> None:
    from mdbx import MDBXDBFlags
    from storage.mdbx.core.implementation import MdbxStorage

    print(f"Opening MDBX database at: {db_path}...")
    entities = []

    with MdbxStorage(db_path, readonly=True) as storage:
        with storage.env.ro_transaction() as txn:
            db_names = storage.db_names(txn)
            for db_name, clazz in db_names.items():
                db = txn.open_map(db_name, flags=MDBXDBFlags.MDBX_DB_DEFAULTS)
                with txn.cursor(db) as cursor:
                    for _key, value in cursor.iter():
                        if value is not None:
                            try:
                                obj = storage.serializer.unmarshall(value, clazz)
                                entities.append(obj)
                                if len(entities) >= max_samples:
                                    break
                            except Exception:
                                pass
                if len(entities) >= max_samples:
                    break

    benchmark_entity_list(entities, label=f"MDBX Entities from {db_path.name}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark Badger ObjectSerializers (CloudPickle vs. Msgspec)")
    parser.add_argument("database", nargs="?", type=Path, help="Optional path to an existing .mdbx database")
    parser.add_argument("--count", type=int, default=10_000, help="Number of entities to benchmark (default: 10,000)")
    args = parser.parse_args()

    if args.database and args.database.exists():
        benchmark_mdbx_file(args.database, max_samples=args.count)
    else:
        entities = generate_sample_entities(args.count)
        benchmark_entity_list(entities, label="Synthetic NeTEx ScheduledStopPoints")


if __name__ == "__main__":
    main()
