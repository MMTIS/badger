# MDBX Usage Guide for Badger Developers

## Overview

This document is a **practical, code-focused guide** for developers working with MDBX storage in Badger. It explains how to build fixes and transforms, covering loading patterns, generators, reference handling, key building, database organization, and performance optimization.

**Target Audience**: Developers new to MDBX who need to write their own fixes, update existing ones, or understand the transform patterns in Badger.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [MDBX Database Organization](#mdbx-database-organization)
3. [Key Concepts](#key-concepts)
4. [Loading Patterns](#loading-patterns)
5. [Writing Data](#writing-data)
6. [Reference Handling](#reference-handling)
7. [Generators and Streaming](#generators-and-streaming)
8. [Common Transform Patterns](#common-transform-patterns)
9. [Performance Optimization](#performance-optimization)
10. [Practical Examples](#practical-examples)
11. [Debugging and Troubleshooting](#debugging-and-troubleshooting)
12. [Best Practices](#best-practices)

---

## Quick Start

### Basic Usage Pattern

```python
from pathlib import Path
from storage.mdbx.core.implementation import MdbxStorage
from domain.netex.model import Line, StopPlace

# Open database (context manager handles open/close)
with MdbxStorage("my_database.lmdb", readonly=False) as storage:
    # Read-only transaction for reading
    with storage.env.ro_transaction() as txn:
        # Read objects
        for line in storage.iter_only_objects(txn, Line):
            print(f"Line: {line.id}")
    
    # Read-write transaction for writing
    with storage.env.rw_transaction() as txn:
        # Write objects
        new_line = Line(id="NEW:Line:001", version="1.0")
        storage.insert_any_object_on_queue(txn, [new_line])
        txn.commit()  # Don't forget to commit!
```

### The Golden Rule

**Always use context managers (`with` statements)** for:
- `MdbxStorage` - handles database open/close
- `env.ro_transaction()` - read-only transactions
- `env.rw_transaction()` - read-write transactions

This ensures proper resource cleanup and prevents database corruption.

---

## MDBX Database Organization

### Database Structure

MDBX uses a **multi-database approach** within a single environment file (`.lmdb`). Badger organizes data as follows:

```
MDBX Environment (single .lmdb file)
├── System Databases (prefixed with _)
│   ├── _class_idx        - Maps class index (2 bytes) to class name
│   ├── _id_idx           - Maps encoded (id, version, class) keys to full keys
│   ├── _unresolved       - Tracks unresolved references (source_key → target_key)
│   └── _reference_outward - Tracks resolved references (source_key → [target_keys])
│
└── Class Databases (one per NeTEx class)
    ├── <class_idx_0>     - StopPlace objects
    ├── <class_idx_1>     - Line objects
    ├── <class_idx_2>     - ServiceJourney objects
    └── ...
```

### Class Index Mapping

Each NeTEx class gets a **2-byte index** (0-65535):

```python
# In MdbxStorage.__init__():
self.class_idx = {}  # type[EntityStructure] -> bytes (2 bytes)
self.idx_class = {}  # bytes (2 bytes) -> type[EntityStructure]
self.class_name_idx = {}  # str -> bytes (2 bytes)

# Example:
# self.class_idx[Line] = b'\x00\x05'  # Line is class index 5
# self.idx_class[b'\x00\x05'] = Line
```

### Key Structure

MDBX uses **composite keys** to efficiently store and retrieve objects:

```
Full Key (8 bytes) = [Class Index (2 bytes)][Local Key (4 bytes)]
Local Key (4 bytes) = Auto-incremented integer per class
```

### Special Databases

| Database | Purpose | Key Type | Value Type |
|----------|---------|----------|------------|
| `_class_idx` | Class name → class index | class name (str) | class index (2 bytes) |
| `_id_idx` | Encoded ID → full key | encoded id (bytes) | full key (8 bytes) |
| `_unresolved` | Unresolved refs | source full key | target encoded id |
| `_reference_outward` | Resolved refs | source full key | target full key |

---

## Key Concepts

### Full Key vs Local Key

```python
# Full key (8 bytes): class_idx (2) + local_key (4) + padding (2)
full_key = b'\x05\x00\x00\x00\x00\x00\x01\x00'  # class 5, local key 1

# Local key (4 bytes): just the object's index within its class
local_key = b'\x00\x00\x00\x01'  # first object of this class

# Conversion functions (in ByteSerializer):
class_idx, local_key = ByteSerializer.full_key_to_idx(full_key)
full_key = ByteSerializer.idx_full_key(class_idx, local_key)
```

### Encoded Keys

Used for ID-based lookups in `_id_idx`:

```python
from storage.mdbx.serialization.byteserializer import ByteSerializer

# Format: ID\0VERSION\0CLASS_IDX
# Note: Version "any" is treated specially (omitted)

serializer = storage.serializer

# Encode a key for a Line with id="NL:Line:123", version="1.0"
encoded_key = serializer.encode_key(
    id="NL:Line:123",
    version="1.0", 
    clazz=Line,
    include_clazz=True
)
# Result: b'NL-LINE-123\x001.0\x00\x05\x00' (approximately)
```

### String Encoding

`ByteSerializer.encode_string()` transforms IDs for storage:
- Converts to **UPPERCASE**
- Replaces special characters (non-alphanumeric) with `*` (0x2A)
- Uses `-` (0x2D) as separator between ID parts

```python
# Examples:
"NL:SA:12345" → b'NL-SA-12345'
"stop@place#1" → b'STOP*PLACE*1'
"UIC:8400001" → b'UIC-8400001'
```

---

## Loading Patterns

### Pattern 1: Iterate Over All Objects of a Class

```python
from domain.netex.model import Line

with MdbxStorage("database.lmdb", readonly=True) as storage:
    with storage.env.ro_transaction() as txn:
        # Returns Generator[Line, None, None]
        for line in storage.iter_only_objects(txn, Line):
            print(f"Line {line.id} has {len(line.points_in_sequence)} points")
```

**Internals**:
- Opens the class-specific database (using `self.class_idx[Line]`)
- Iterates over all keys, deserializes each value
- Uses **CloudPickle + LZ4 compression** for serialization

### Pattern 2: Iterate With Keys

```python
# Get both keys and objects
for key, line in storage.iter_objects(txn, Line):
    print(f"Key: {key.hex()}, Line: {line.id}")

# key is the local key (4 bytes)
# This is more efficient if you need the key for later operations
```

### Pattern 3: Load Single Object by ID

```python
from domain.netex.model import StopPlace

# By id and version
full_key, stop_place = storage.load_object_by_id_version(
    txn, 
    id="NL:SA:12345", 
    clazz=StopPlace,
    version="1.0"
)

# If version is None, it searches for any version
full_key, stop_place = storage.load_object_by_id_version(
    txn, 
    id="NL:SA:12345", 
    clazz=StopPlace,
    version=None  # Returns first match
)
```

### Pattern 4: Load Object by Full Key

```python
# If you have a full key from a reference
full_key = b'\x05\x00\x00\x00\x00\x00\x01\x00'
obj = storage.load_object_by_full_key(txn, full_key)

# This is very efficient - direct lookup in the class database
```

### Pattern 5: Scan with Prefix (Partial ID Match)

```python
from storage.mdbx.serialization.byteserializer import ByteSerializer

# Find all objects with ID starting with "NL:SA:"
prefix = ByteSerializer.encode_string("NL:SA:") + bytes([ByteSerializer.SEPARATOR])

with storage.env.ro_transaction() as txn:
    # Use DB_ID_IDX to find matching encoded keys
    db_id_idx = txn.open_map(name=DB_ID_IDX, flags=DB_ID_IDX_FLAGS)
    cursor = txn.cursor(db_id_idx)
    
    for encoded_key, full_key in cursor.iter(prefix):
        if encoded_key.startswith(prefix):
            obj = storage.load_object_by_full_key(txn, full_key)
            print(f"Found: {obj.id}")
```

### Pattern 6: Load All Objects (For Small Datasets)

```python
# WARNING: Only for small datasets - loads everything into memory
with MdbxStorage("database.lmdb", readonly=True) as storage:
    with storage.env.ro_transaction() as txn:
        all_lines = list(storage.iter_only_objects(txn, Line))
        all_stop_places = list(storage.iter_only_objects(txn, StopPlace))
```

---

## Writing Data

### Pattern 1: Insert Single Object

```python
from domain.netex.model import Line

with MdbxStorage("database.lmdb", readonly=False) as storage:
    with storage.env.rw_transaction() as txn:
        new_line = Line(
            id="NEW:Line:001",
            version="1.0",
            # ... other fields
        )
        
        # Insert using the queue method
        storage.insert_any_object_on_queue(txn, [new_line])
        txn.commit()
```

### Pattern 2: Insert Multiple Objects (Batched)

```python
# Create objects
new_objects = [
    Line(id=f"NEW:Line:{i:03d}", version="1.0")
    for i in range(1000)
]

with MdbxStorage("database.lmdb", readonly=False) as storage:
    with storage.env.rw_transaction() as txn:
        # Single call inserts all objects efficiently
        storage.insert_any_object_on_queue(txn, new_objects)
        txn.commit()
```

### Pattern 3: Update Existing Object

```python
# Load, modify, re-insert
with MdbxStorage("database.lmdb", readonly=False) as storage:
    with storage.env.rw_transaction() as txn:
        # Load the object
        full_key, line = storage.load_object_by_id_version(
            txn, id="NL:Line:001", clazz=Line, version="1.0"
        )
        
        if line:
            # Modify
            line.version = "2.0"
            line.description = "Updated description"
            
            # Re-insert (will overwrite existing)
            storage.insert_any_object_on_queue(txn, [line])
        
        txn.commit()
```

### Pattern 4: Copy Objects Between Databases

```python
from pathlib import Path

source_path = Path("source.lmdb")
target_path = Path("target.lmdb")

with MdbxStorage(target_path, readonly=False) as target:
    with target.env.rw_transaction() as txn_write:
        with MdbxStorage(source_path, readonly=True) as source:
            with source.env.ro_transaction() as txn_read:
                # Copy all Line objects
                for line in source.iter_only_objects(txn_read, Line):
                    # Optionally transform
                    # line = transform(line)
                    pass
                
                # Batch insert
                lines = list(source.iter_only_objects(txn_read, Line))
                target.insert_any_object_on_queue(txn_write, lines)
        
        txn_write.commit()
```

### Pattern 5: Use copy_map for Direct Copy

```python
# Most efficient way to copy unchanged classes
with MdbxStorage(target_path, readonly=False) as target:
    with target.env.rw_transaction() as txn_write:
        with MdbxStorage(source_path, readonly=True) as source:
            with source.env.ro_transaction() as txn_read:
                # Direct copy - no deserialization/serialization
                source.copy_map(txn_read, target, txn_write, Line)
                source.copy_map(txn_read, target, txn_write, StopPlace)
        
        txn_write.commit()
```

### Pattern 6: Insert With Chunking (Large Datasets)

```python
from typing import Generator

def generate_lines(count: int) -> Generator[Line, None, None]:
    """Generate Line objects - uses generator to avoid memory issues"""
    for i in range(count):
        yield Line(id=f"Line:{i:06d}", version="1.0")

CHUNK_SIZE = 10000

with MdbxStorage("database.lmdb", readonly=False) as storage:
    for chunk_start in range(0, 1000000, CHUNK_SIZE):
        with storage.env.rw_transaction() as txn:
            chunk = list(generate_lines(CHUNK_SIZE))
            storage.insert_any_object_on_queue(txn, chunk)
            txn.commit()
            print(f"Inserted {len(chunk)} objects")
```

---

## Reference Handling

### Understanding References

NeTEx uses `VersionOfObjectRefStructure` for references:

```python
@dataclass
class VersionOfObjectRefStructure:
    ref: str              # Required - the target ID
    version: str | None  # Optional - target version (can be "any")
    name_of_ref_class: str | None  # Optional - target class name
```

Badger tracks references in two system databases:
- `_unresolved`: References that couldn't be resolved (source → target encoded key)
- `_reference_outward`: Resolved references (source full key → target full key)

### Pattern 1: Follow References from an Object

```python
from domain.netex.model import ServiceJourney

with MdbxStorage("database.lmdb", readonly=True) as storage:
    with storage.env.ro_transaction() as txn:
        for sj in storage.iter_only_objects(txn, ServiceJourney):
            # Get the Line reference
            if sj.line_ref:
                line = storage.load_object_by_reference(txn, sj.line_ref)
                if line:
                    print(f"ServiceJourney {sj.id} uses Line {line.id}")
            
            # Get all StopPoints from the journey pattern
            if sj.journey_pattern_ref:
                jp = storage.load_object_by_reference(txn, sj.journey_pattern_ref)
                if jp and jp.points_in_journey_pattern:
                    for point in jp.points_in_journey_pattern:
                        if point.stop_point_ref:
                            stop = storage.load_object_by_reference(txn, point.stop_point_ref)
                            print(f"  Stop: {stop.id}")
```

### Pattern 2: Find All Objects Referencing a Target

```python
# Find all objects that reference a specific StopPlace
with MdbxStorage("database.lmdb", readonly=True) as storage:
    with storage.env.ro_transaction() as txn:
        # First, get the target StopPlace's full key
        _, target_sp = storage.load_object_by_id_version(
            txn, id="NL:SA:12345", clazz=StopPlace, version=None
        )
        
        if target_sp and hasattr(target_sp, 'idx'):
            # Get all objects that reference this StopPlace
            for ref_class, ref_key in storage.load_references_by_object(
                txn, target_sp, inwards=True
            ):
                # ref_class is the class of the referencing object
                # ref_key is the local key of the referencing object
                ref_obj = storage.load_object(txn, ref_class, ref_key)
                print(f"{ref_class.__name__} {ref_obj.id} references {target_sp.id}")
```

### Pattern 3: Resolve References During Insert

When you insert an object, `insert_any_object_on_queue` automatically:
1. Extracts all references from the object
2. Checks if target exists in `_id_idx`
3. If exists: adds to `_reference_outward`
4. If not exists: adds to `_unresolved`

```python
# The serializer helps extract references
from domain.netex.services.recursive_attributes import only_references

obj = Line(id="NEW:Line:001", version="1.0")

# Get all references from object
for ref_class, ref_id, ref_version in only_references(obj, storage.serializer):
    print(f"References: {ref_class.__name__} {ref_id}@{ref_version}")
```

### Pattern 4: Manual Reference Resolution

```python
# Manually resolve and update a reference
with MdbxStorage("database.lmdb", readonly=False) as storage:
    with storage.env.rw_transaction() as txn:
        # Find object with unresolved reference
        db_unresolved = txn.open_map(name=DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
        
        for source_full_key, target_encoded_key in db_unresolved.cursor().iter():
            # Try to resolve
            target_full_key = db_id_idx.get(txn, target_encoded_key)
            if target_full_key:
                # Reference is now resolved
                db_reference_outward.put(txn, source_full_key, target_full_key)
                db_unresolved.delete(txn, source_full_key, target_encoded_key)
        
        txn.commit()
```

### Pattern 5: Fix Missing References

```python
from domain.netex.model import ServiceJourney

def fix_missing_line_refs(storage_path: Path) -> None:
    """Fix ServiceJourneys with missing Line references"""
    
    with MdbxStorage(storage_path, readonly=False) as storage:
        with storage.env.rw_transaction() as txn:
            # Find all ServiceJourneys
            for sj in storage.iter_only_objects(txn, ServiceJourney):
                if sj.line_ref is None:
                    # Try to infer from ServiceJourneyPattern
                    if sj.journey_pattern_ref:
                        jp = storage.load_object_by_reference(txn, sj.journey_pattern_ref)
                        if jp and jp.line_ref:
                            sj.line_ref = jp.line_ref
                            # Re-insert to update
                            storage.insert_any_object_on_queue(txn, [sj])
            
            txn.commit()
```

---

## Generators and Streaming

### Why Generators?

**Generators are CRITICAL for performance** in Badger:
- Minimize memory usage (don't load entire datasets)
- Enable streaming between databases
- Allow processing of datasets larger than memory

### Pattern 1: Generator for Object Transformation

```python
from typing import Generator
from domain.netex.model import Line

def transform_lines(source_db: MdbxStorage, txn: TXN) -> Generator[Line, None, None]:
    """Transform Line objects - returns generator for streaming"""
    for line in source_db.iter_only_objects(txn, Line):
        # Create new Line with modifications
        new_line = Line(
            id=line.id,
            version=line.version or "1.0",  # Ensure version
            description=line.description or f"Line {line.id}",
            # Copy other fields...
        )
        yield new_line

# Usage in transform
with MdbxStorage(target_path, readonly=False) as target:
    with target.env.rw_transaction() as txn_write:
        with MdbxStorage(source_path, readonly=True) as source:
            with source.env.ro_transaction() as txn_read:
                # Stream transformed objects directly to target
                target.insert_any_object_on_queue(
                    txn_write, 
                    transform_lines(source, txn_read)
                )
        txn_write.commit()
```

### Pattern 2: Generator with Reference Lookup

```python
def enrich_lines_with_stop_count(source_db: MdbxStorage, txn: TXN) -> Generator[Line, None, None]:
    """Add stop count to Lines based on their ServiceJourneyPatterns"""
    
    for line in source_db.iter_only_objects(txn, Line):
        # Count stops in all patterns for this line
        stop_count = 0
        for sj in source_db.iter_only_objects(txn, ServiceJourney):
            if sj.line_ref and sj.line_ref.ref == line.id:
                if sj.journey_pattern_ref:
                    jp = source_db.load_object_by_reference(txn, sj.journey_pattern_ref)
                    if jp:
                        stop_count += len(jp.points_in_journey_pattern or [])
        
        # Create enriched line
        enriched = Line(
            id=line.id,
            version=line.version,
            # ... copy other fields
            # Add custom field (if schema allows)
        )
        yield enriched
```

### Pattern 3: Chained Generators

```python
def filter_lines(source_db: MdbxStorage, txn: TXN) -> Generator[Line, None, None]:
    """Filter Lines - only yield active ones"""
    for line in source_db.iter_only_objects(txn, Line):
        if line.status == "active":
            yield line

def transform_to_epip(source_db: MdbxStorage, txn: TXN) -> Generator[Line, None, None]:
    """Transform to EPIP profile"""
    for line in source_db.iter_only_objects(txn, Line):
        yield Line(
            id=line.id,
            version=line.version or "1.0",
            # Apply EPIP-specific transformations
        )

# Chain: filter -> transform
with MdbxStorage(target_path, readonly=False) as target:
    with target.env.rw_transaction() as txn_write:
        with MdbxStorage(source_path, readonly=True) as source:
            with source.env.ro_transaction() as txn_read:
                # Chain generators - no intermediate lists
                lines = transform_to_epip(source, txn_read)
                filtered_lines = filter_lines(source, txn_read)
                
                # BAD - creates intermediate list:
                # target.insert_any_object_on_queue(txn_write, list(filter_lines(...)))
                
                # GOOD - streams directly:
                target.insert_any_object_on_queue(txn_write, filtered_lines)
        txn_write.commit()
```

### Pattern 4: Generator with Caching

```python
def transform_with_cache(source_db: MdbxStorage, txn: TXN) -> Generator[Line, None, None]:
    """Transform with caching for frequently accessed data"""
    
    # Cache for operators
    operator_cache: dict[str, Operator] = {}
    
    for line in source_db.iter_only_objects(txn, Line):
        # Get operator - use cache
        operator_id = line.operator_ref.ref if line.operator_ref else None
        
        if operator_id:
            if operator_id not in operator_cache:
                operator = source_db.load_object_by_reference(txn, line.operator_ref)
                operator_cache[operator_id] = operator
            
            operator = operator_cache[operator_id]
            
            # Use cached operator in transformation
            if operator:
                line.operator_ref.name_of_ref_class = type(operator).__name__
        
        yield line
```

---

## Common Transform Patterns

### Pattern 1: Simple Profile Transformation

```python
def simple_transform(source_path: Path, target_path: Path) -> None:
    """Copy all objects with minimal transformation"""
    
    with MdbxStorage(target_path, readonly=False) as target:
        with target.env.rw_transaction() as txn_write:
            with MdbxStorage(source_path, readonly=True) as source:
                with source.env.ro_transaction() as txn_read:
                    # Copy all classes directly
                    for clazz in [Line, StopPlace, ServiceJourney]:
                        source.copy_map(txn_read, target, txn_write, clazz)
            
            txn_write.commit()
```

### Pattern 2: Transform with Reference Resolution

```python
def transform_with_resolved_refs(source_path: Path, target_path: Path) -> None:
    """Transform ensuring all references are resolved"""
    
    # First, ensure source has resolved references
    with MdbxStorage(source_path, readonly=False) as source:
        # This resolves all references in the source
        from storage.mdbx.core.references import resolve, resolve_embeddings
        resolve(source)
        resolve_embeddings(source)
    
    # Now transform
    with MdbxStorage(target_path, readonly=False) as target:
        with target.env.rw_transaction() as txn_write:
            with MdbxStorage(source_path, readonly=True) as source:
                with source.env.ro_transaction() as txn_read:
                    # All references should now be resolvable
                    for line in source.iter_only_objects(txn_read, Line):
                        # Verify references exist
                        if line.operator_ref:
                            op = source.load_object_by_reference(txn_read, line.operator_ref)
                            if not op:
                                print(f"Warning: Operator {line.operator_ref.ref} not found")
                        yield line
            
            txn_write.commit()
```

### Pattern 3: Inference Transform (Direction, Coordinates)

```python
def infer_and_apply(source_path: Path, target_path: Path) -> None:
    """Infer missing data and apply to objects"""
    
    from transformers.direction import infer_directions_from_sjps_and_apply
    from transformers.projection import reprojection_update
    
    # First copy data
    with MdbxStorage(target_path, readonly=False) as target:
        with target.env.rw_transaction() as txn_write:
            with MdbxStorage(source_path, readonly=True) as source:
                with source.env.ro_transaction() as txn_read:
                    # Copy base data
                    for clazz in [Line, ServiceJourneyPattern]:
                        source.copy_map(txn_read, target, txn_write, clazz)
            
            txn_write.commit()
    
    # Then apply inference on target
    with MdbxStorage(target_path, readonly=False) as target:
        with target.env.rw_transaction() as txn:
            # Infer directions
            infer_directions_from_sjps_and_apply(target, txn, defaults)
            
            # Reproject coordinates
            reprojection_update(target, txn, "urn:ogc:def:crs:EPSG::4326")
            
            txn.commit()
```

### Pattern 4: Filter Transform

```python
def filter_by_region(source_path: Path, target_path: Path, polygon: Polygon) -> None:
    """Filter objects by geographic region"""
    
    from filter.objects_in_polygon import is_in_polygon
    
    with MdbxStorage(target_path, readonly=False) as target:
        with target.env.rw_transaction() as txn_write:
            with MdbxStorage(source_path, readonly=True) as source:
                with source.env.ro_transaction() as txn_read:
                    # Only copy StopPlaces in the polygon
                    for sp in source.iter_only_objects(txn_read, StopPlace):
                        if has_coordinates(sp) and is_in_polygon(sp, polygon):
                            yield sp
                    
                    # Copy Lines that use these StopPlaces
                    # (This requires tracking which Lines reference which StopPlaces)
            
            txn_write.commit()
```

### Pattern 5: Aggregation Transform

```python
def aggregate_stop_assignments(source_path: Path, target_path: Path) -> None:
    """Aggregate data from multiple objects into new objects"""
    
    from collections import defaultdict
    
    with MdbxStorage(source_path, readonly=True) as source:
        with source.env.ro_transaction() as txn:
            # Group PassengerStopAssignments by Quay
            psas_by_quay: dict[str, list[PassengerStopAssignment]] = defaultdict(list)
            
            for psa in source.iter_only_objects(txn, PassengerStopAssignment):
                if psa.quay_ref:
                    quay_id = psa.quay_ref.ref
                    psas_by_quay[quay_id].append(psa)
            
            # Create aggregated objects
            aggregated = []
            for quay_id, psas in psas_by_quay.items():
                # Create new object that aggregates the PSAs
                # (This is just an example - actual implementation depends on needs)
                aggregated.append(create_aggregated_object(quay_id, psas))
    
    # Write aggregated objects
    with MdbxStorage(target_path, readonly=False) as target:
        with target.env.rw_transaction() as txn:
            target.insert_any_object_on_queue(txn, aggregated)
            txn.commit()
```

---

## Performance Optimization

### 1. Use Read-Only Transactions for Reading

```python
# GOOD - read-only transaction
with storage.env.ro_transaction() as txn:
    for obj in storage.iter_only_objects(txn, Line):
        process(obj)

# BAD - unnecessary read-write transaction
with storage.env.rw_transaction() as txn:
    for obj in storage.iter_only_objects(txn, Line):
        process(obj)
# ro_transaction is faster and allows concurrent reads
```

### 2. Batch Inserts

```python
# GOOD - single batch insert
objects = [Line(id=f"Line:{i}", version="1.0") for i in range(10000)]
storage.insert_any_object_on_queue(txn, objects)

# BAD - individual inserts
for i in range(10000):
    obj = Line(id=f"Line:{i}", version="1.0")
    storage.insert_any_object_on_queue(txn, [obj])  # Creates overhead
```

### 3. Use copy_map for Unchanged Classes

```python
# GOOD - direct copy, no deserialization
source.copy_map(txn_read, target, txn_write, Line)

# BAD - manual copy with deserialization/serialization
for line in source.iter_only_objects(txn_read, Line):
    target.insert_any_object_on_queue(txn_write, [line])
```

`copy_map` is **10-100x faster** for unchanged data because it copies raw bytes.

### 4. Cache Frequently Accessed Objects

```python
# GOOD - cache operators
operator_cache: dict[str, Operator] = {}
for line in storage.iter_only_objects(txn, Line):
    if line.operator_ref:
        op_id = line.operator_ref.ref
        if op_id not in operator_cache:
            operator_cache[op_id] = storage.load_object_by_reference(txn, line.operator_ref)
        # Use cached operator

# BAD - repeated lookups
for line in storage.iter_only_objects(txn, Line):
    if line.operator_ref:
        operator = storage.load_object_by_reference(txn, line.operator_ref)  # Slow!
```

### 5. Chunk Large Operations

```python
# GOOD - process in chunks
CHUNK_SIZE = 10000
all_objects = storage.iter_only_objects(txn, Line)

for chunk in chunked(all_objects, CHUNK_SIZE):
    with storage.env.rw_transaction() as txn:
        # Process chunk
        for obj in chunk:
            process(obj)
        txn.commit()

# BAD - single large transaction
with storage.env.rw_transaction() as txn:
    for obj in all_objects:
        process(obj)  # Can cause memory issues
    txn.commit()
```

### 6. Prefetch References

```python
# GOOD - prefetch all needed objects first
with storage.env.ro_transaction() as txn:
    # Get all Line IDs first
    line_ids = [line.id for line in storage.iter_only_objects(txn, Line)]
    
    # Prefetch all Operators in one pass
    operators = {}
    for line_id in line_ids:
        # This is still slow - better to scan the database once
        pass
    
    # Better: scan DB_REFERENCE_OUTWARD once to get all operator references
    # Then load all operators at once

# EVEN BETTER - use fetch_all_references_by_class
with storage.env.ro_transaction() as txn:
    referenced_operators = storage.fetch_all_references_by_class(
        txn, {Operator}, skip_existing=False
    )
    # Now all operators are in a dict/list for fast access
```

### 7. Avoid Deserialization When Possible

```python
# GOOD - work with serialized data if possible
with storage.env.ro_transaction() as txn:
    db = txn.open_map(name=storage.class_idx[Line])
    with txn.cursor(db) as cursor:
        # Just count objects without deserializing
        count = 0
        for key, value in cursor.iter():
            count += 1
    print(f"Total lines: {count}")

# BAD - deserialize everything just to count
count = len(list(storage.iter_only_objects(txn, Line)))
```

### 8. Use Generators, Not Lists

```python
# GOOD - generator
def process_objects() -> Generator[Line, None, None]:
    for i in range(1000000):
        yield Line(id=f"Line:{i}", version="1.0")

# BAD - list
def process_objects() -> list[Line]:
    return [Line(id=f"Line:{i}", version="1.0") for i in range(1000000)]
# Creates 1M objects in memory immediately
```

### 9. Index Optimization

The `_id_idx` database uses **prefix-based keys**, so:

```python
# GOOD - prefix queries are fast
# All objects with ID starting with "NL:SA:" can be found quickly

# BAD - arbitrary queries are slow
# Finding all Lines with a specific name requires full scan
```

### 10. Transaction Size

- **Small transactions**: Fast, low memory, can be retried
- **Large transactions**: Slow, high memory, risk of timeout
- **Rule of thumb**: Keep transactions under 100,000 objects

---

## Practical Examples

### Example 1: Fix Missing Versions

```python
from pathlib import Path
from domain.netex.model import Line

def fix_missing_versions(database_path: Path) -> None:
    """Ensure all Line objects have a version"""
    
    with MdbxStorage(database_path, readonly=False) as storage:
        with storage.env.rw_transaction() as txn:
            count = 0
            for line in storage.iter_only_objects(txn, Line):
                if not line.version:
                    line.version = "1.0"
                    count += 1
            
            if count > 0:
                # Re-insert all modified objects
                lines = list(storage.iter_only_objects(txn, Line))
                storage.insert_any_object_on_queue(txn, lines)
            
            txn.commit()
            print(f"Fixed {count} Lines with missing versions")

# Usage
fix_missing_versions(Path("my_database.lmdb"))
```

### Example 2: Create New Table (Class)

```python
from domain.netex.model import Line, StopPlace

def create_line_summary(database_path: Path) -> None:
    """Create summary objects (this creates a new 'table' in MDBX)"""
    
    from domain.netex.model import Notice  # Reusing existing class
    
    with MdbxStorage(database_path, readonly=False) as storage:
        with storage.env.rw_transaction() as txn:
            # Create summary objects
            summaries = []
            
            for line in storage.iter_only_objects(txn, Line):
                stop_count = 0
                for sp in storage.iter_only_objects(txn, StopPlace):
                    # Check if this StopPlace is used by this Line
                    # (This is simplified - actual check would follow references)
                    if is_used_by_line(sp, line, storage, txn):
                        stop_count += 1
                
                # Create summary as a Notice object (reusing existing class)
                summary = Notice(
                    id=f"SUMMARY:{line.id}",
                    version="1.0",
                    text=f"Line {line.id} has {stop_count} stops"
                )
                summaries.append(summary)
            
            storage.insert_any_object_on_queue(txn, summaries)
            txn.commit()
            print(f"Created {len(summaries)} summary objects")
```

### Example 3: Transform GTFS to NeTEx

```python
from pathlib import Path

def gtfs_to_netex(gtfs_path: Path, netex_path: Path) -> None:
    """Simplified GTFS to NeTEx conversion"""
    
    from domain.netex.model import (
        Line, StopPlace, ScheduledStopPoint, Operator,
        ServiceJourney, ServiceJourneyPattern
    )
    
    # Step 1: Load GTFS (using DuckDB or other method)
    # gtfs_data = load_gtfs(gtfs_path)
    
    # Step 2: Convert to NeTEx objects
    def generate_netex_objects() -> Generator[EntityStructure, None, None]:
        # Convert GTFS routes to NeTEx Lines
        for route in gtfs_data.routes:
            yield Line(
                id=f"Line:{route.route_id}",
                version="1.0",
                name=route.route_long_name or route.route_short_name
            )
        
        # Convert GTFS stops to NeTEx StopPlaces and ScheduledStopPoints
        for stop in gtfs_data.stops:
            stop_place = StopPlace(
                id=f"StopPlace:{stop.stop_id}",
                version="1.0",
                name=stop.stop_name
            )
            yield stop_place
            
            scheduled_stop = ScheduledStopPoint(
                id=f"ScheduledStopPoint:{stop.stop_id}",
                version="1.0",
                name=stop.stop_name,
                # Add location
            )
            yield scheduled_stop
        
        # Convert GTFS trips to NeTEx ServiceJourneys
        for trip in gtfs_data.trips:
            sj = ServiceJourney(
                id=f"ServiceJourney:{trip.trip_id}",
                version="1.0",
                # Set references
            )
            yield sj
    
    # Step 3: Store in MDBX
    with MdbxStorage(netex_path, readonly=False) as storage:
        with storage.env.rw_transaction() as txn:
            storage.insert_any_object_on_queue(txn, generate_netex_objects())
        txn.commit()
```

### Example 4: Update Existing Transform

```python
# Let's enhance the EPIP line generator

def enhanced_epip_line_generator(
    source_db: MdbxStorage, 
    txn: TXN, 
    defaults: dict
) -> Generator[Line, None, None]:
    """Enhanced EPIP line generator with additional fields"""
    
    for line in source_db.iter_only_objects(txn, Line):
        # Ensure version
        version = line.version or defaults.get("version", "1.0")
        
        # Ensure codespace
        if not hasattr(line, 'codespace_ref') or line.codespace_ref is None:
            codespace = defaults.get("codespace")
            if codespace:
                line.codespace_ref = getRef(codespace, Codespace)
        
        # Add EPIP-specific fields
        epip_line = Line(
            id=line.id,
            version=version,
            name=line.name,
            description=line.description,
            # Copy all other fields
            **{f: getattr(line, f) for f in line.__dataclass_fields__}
        )
        
        # Add EPIP-specific processing
        if epip_line.short_name is None and epip_line.name:
            # Generate short name from long name
            epip_line.short_name = generate_short_name(epip_line.name)
        
        yield epip_line
```

### Example 5: Fix Reference Issues

```python
def fix_operator_references(database_path: Path) -> None:
    """Fix ServiceJourneys with missing or invalid operator references"""
    
    from domain.netex.model import ServiceJourney, Operator
    
    with MdbxStorage(database_path, readonly=False) as storage:
        with storage.env.rw_transaction() as txn:
            # Get all Operators first
            operators = list(storage.iter_only_objects(txn, Operator))
            operator_map = {op.id: op for op in operators}
            
            # Find default operator
            default_operator = operator_map.get("DEFAULT") or operators[0] if operators else None
            
            # Fix ServiceJourneys
            modified = []
            for sj in storage.iter_only_objects(txn, ServiceJourney):
                needs_update = False
                
                if sj.operator_ref is None:
                    if default_operator:
                        sj.operator_ref = getRef(default_operator)
                        needs_update = True
                elif sj.operator_ref.ref not in operator_map:
                    # Operator doesn't exist - use default
                    if default_operator:
                        sj.operator_ref = getRef(default_operator)
                        needs_update = True
                
                if needs_update:
                    modified.append(sj)
            
            if modified:
                storage.insert_any_object_on_queue(txn, modified)
            
            txn.commit()
            print(f"Fixed {len(modified)} ServiceJourneys")
```

---

## Debugging and Troubleshooting

### Inspect Database Contents

```python
def inspect_database(database_path: Path) -> None:
    """Print database statistics"""
    
    with MdbxStorage(database_path, readonly=True) as storage:
        with storage.env.ro_transaction() as txn:
            print(f"Database: {database_path}")
            print(f"Readonly: {storage.readonly}")
            print()
            
            # Count objects per class
            for clazz in storage.db_names(txn).values():
                count = 0
                try:
                    for _ in storage.iter_only_objects(txn, clazz):
                        count += 1
                except:
                    count = 0
                print(f"{clazz.__name__:30s}: {count:8d} objects")
            
            print()
            
            # Check unresolved references
            db_unresolved = txn.open_map(name=DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
            unresolved_count = db_unresolved.get_stat(txn).ms_entries
            print(f"Unresolved references: {unresolved_count}")
            
            # Sample unresolved references
            if unresolved_count > 0:
                print("\nSample unresolved references:")
                cursor = txn.cursor(db_unresolved)
                for i, (source_key, target_key) in enumerate(cursor.iter()):
                    if i >= 10:
                        break
                    source_clazz_idx, source_local = ByteSerializer.full_key_to_idx(source_key)
                    source_clazz = storage.idx_class.get(source_clazz_idx, b"UNKNOWN")
                    print(f"  {source_clazz} -> {target_key}")
```

### Verify Reference Integrity

```python
def verify_references(database_path: Path) -> list[str]:
    """Verify all references can be resolved"""
    
    errors = []
    
    with MdbxStorage(database_path, readonly=True) as storage:
        with storage.env.ro_transaction() as txn:
            # Check all objects have valid references
            for clazz in storage.db_names(txn).values():
                for obj in storage.iter_only_objects(txn, clazz):
                    # Get all references from object
                    for ref_class, ref_id, ref_version in only_references(obj, storage.serializer):
                        # Try to resolve
                        try:
                            target = storage.load_object_by_id_version(
                                txn, ref_id, ref_class, ref_version
                            )
                            if target[1] is None:
                                errors.append(
                                    f"{clazz.__name__} {obj.id}: "
                                    f"ref to {ref_class.__name__} {ref_id} not found"
                                )
                        except Exception as e:
                            errors.append(
                                f"{clazz.__name__} {obj.id}: "
                                f"error resolving {ref_class.__name__} {ref_id}: {e}"
                            )
    
    return errors
```

### Check Transaction Status

```python
# Always check if transaction is still valid
with storage.env.rw_transaction() as txn:
    try:
        # Do work
        pass
    except Exception as e:
        print(f"Transaction failed: {e}")
        # Transaction will be rolled back automatically
        raise
    finally:
        # txn.commit() is called automatically by context manager
        pass
```

### Common Errors and Solutions

| Error | Cause | Solution |
|-------|-------|----------|
| `MDBXErrorExc: MDBX_NOTFOUND` | Database or key not found | Check database path, verify key exists |
| `KeyError` in `idx_class` | Class not registered | Run `_restore_class_idx()` or check schema |
| `AssertionError: rc == 0` | Database corruption | Delete and recreate database |
| `MemoryError` | Transaction too large | Chunk operations, use smaller transactions |
| `TypeError: 'NoneType' not callable` | Missing reference target | Check data, use `resolve()` first |
| `cloudpickle error` | Non-serializable object | Use simple types, no lambdas in objects |

---

## Best Practices

### DO

✅ **Always use context managers** (`with` statements) for databases and transactions
✅ **Use generators** for streaming large datasets
✅ **Use `copy_map`** for unchanged classes - it's much faster
✅ **Use read-only transactions** when only reading
✅ **Batch inserts** - use `insert_any_object_on_queue` with lists/generators
✅ **Cache frequently accessed objects** to avoid repeated lookups
✅ **Chunk large operations** to avoid memory issues
✅ **Handle errors properly** - don't catch bare `Exception`
✅ **Use type hints** for better code maintainability
✅ **Test with small datasets** first, then scale up

### DON'T

❌ **Don't use bare `pickle`** - use `serializer.marshall/unmarshall` (CloudPickle + LZ4)
❌ **Don't forget to commit** transactions (use context managers to avoid this)
❌ **Don't load entire datasets** into memory (use generators)
❌ **Don't use long-running write transactions** (chunk instead)
❌ **Don't modify objects** that are being iterated over
❌ **Don't ignore unresolved references** - they will cause problems later
❌ **Don't use `readonly=False`** when you only need to read
❌ **Don't create large intermediate lists** - use generators/chaining

### Always Remember

1. **MDBX is ACID compliant** - transactions are atomic
2. **MDBX is single-writer** - only one process can write at a time
3. **MDBX is memory-mapped** - file size limits apply (default 8GB)
4. **References must be resolved** - unresolved references break transformations
5. **Generators save memory** - use them for large datasets

---

## Summary

This guide has covered:

1. **Database organization** - How MDBX stores NeTEx objects with class-specific databases
2. **Key concepts** - Full keys, local keys, encoded keys, class indexes
3. **Loading patterns** - Various ways to read data efficiently
4. **Writing patterns** - How to insert, update, and copy data
5. **Reference handling** - Working with NeTEx references in MDBX
6. **Generators** - Critical for performance and memory efficiency
7. **Transform patterns** - Common patterns for fixes and transformations
8. **Performance** - Optimization techniques for speed and memory
9. **Examples** - Practical, working code examples
10. **Debugging** - How to inspect and troubleshoot databases
11. **Best practices** - Dos and don'ts for MDBX development

### Next Steps

1. **Start small** - Write a simple fix for a known issue
2. **Test with real data** - Use actual NeTEx files, not just test data
3. **Profile performance** - Use `time` or `cProfile` to find bottlenecks
4. **Read existing code** - Study `conv/epip_db_to_db.py` and other modules
5. **Experiment** - Try different approaches and measure the results

### Resources

- [MDBX Documentation](https://github.com/erthink/libmdbx)
- [LMDB Documentation](https://lmdb.readthedocs.io/) (MDBX is a fork)
- [Badger Architecture](badger-architecture.md)
- [MDBX Usage Analysis](mdbx-usage-analysis.md)
- [Reference Approach Analysis](mdbx-reference-approach-analysis.md)

---

*This document is a practical guide. If you find errors or have suggestions for improvements, please contribute back to the project.*
