# Introduction for New Contributors to Badger

## Welcome! 👋

Thank you for your interest in contributing to **Badger** - an extremely fast, state-of-the-art timetable conversion system. This document will help you understand the project structure, development practices, and how to contribute effectively.

## What is Badger?

**Badger** is a high-performance timetable conversion system that:

- Transforms various transportation data formats (GTFS, NeTEx, IFF, etc.) into **NeTEx objects**
- Uses **NeTEx as the canonical intermediate format** - all data is converted to NeTEx first
- Supports **multiple output profiles**: EPIP, Dutch, Nordic, VDV462, Swiss, Italian, GTFS
- Is designed for **streaming and performance** while maintaining reproducibility
- Employs **SAX-based XML parsing**, task queues, and optimized database access

### Key Design Principles

1. **NeTEx as Intermediate**: All data flows through NeTEx objects
2. **Streaming Architecture**: Uses generators to minimize memory usage
3. **Audit Trail**: All information preserved as-is, no proprietary intermediate representation
4. **Schema Compliance**: Uses xsData for XML Schema compliance via Python Data Classes
5. **High Performance**: SAX parsing, task queues, LZ4 compression, LMDB/MDBX storage

## Project Structure

```
badger/
├── conv/                      # Conversion modules
│   ├── netex_to_db.py         # NeTEx XML → MDBX database
│   ├── gtfs_import_to_db.py   # GTFS ZIP → DuckDB import
│   ├── gtfs_convert_to_db.py  # DuckDB → MDBX (NeTEx GeneralFrame)
│   ├── epip_db_to_db.py       # MDBX → MDBX (EPIP profile transformation)
│   ├── epip_db_to_xml.py      # MDBX → NeTEx XML export
│   ├── netex_db_to_generalframe.py  # Normalize to GeneralFrame
│   └── ...
│
├── domain/                    # Domain models and business logic
│   ├── gtfs/                  # GTFS domain
│   │   ├── model/             # GTFS data models
│   │   ├── services/          # GTFS conversion services
│   │   └── transform/         # GTFS transformation logic
│   │
│   ├── netex/                 # NeTEx domain (CORE)
│   │   ├── model/             # Generated NeTEx data classes (xsData)
│   │   ├── conf/              # Configuration files
│   │   ├── indexes/           # Indexing utilities
│   │   ├── schema/            # XML Schema definitions
│   │   ├── services/          # NeTEx service layer
│   │   └── model/             # Additional NeTEx models
│   │
│   └── trout/                 # Trout format support
│
├── storage/                   # Storage backends
│   ├── lmdb/                  # LMDB storage (legacy)
│   │   ├── core/              # Core LMDB operations
│   │   └── serialization/     # Serialization utilities
│   │
│   ├── mdbx/                  # MDBX storage (PRIMARY)
│   │   ├── core/              # MDBX implementation
│   │   │   ├── implementation.py  # MdbxStorage class
│   │   │   ├── references.py     # Reference resolution
│   │   │   └── ...
│   │   └── serialization/     # Byte serialization
│   │
│   └── lxml/                  # LXML/SAX parsing
│       ├── core/              # XML parsing core
│       └── serialization/     # XML serialization
│
├── transformers/              # Format transformation logic
│   ├── epip.py                # EPIP profile transformers
│   ├── gtfs.py                # GTFS-specific transformers
│   ├── direction.py           # Direction inference
│   ├── projection.py          # Coordinate projection
│   ├── scheduledstoppoint.py  # Stop point inference
│   ├── nordicprofile.py       # Nordic profile
│   ├── dutch.py               # Dutch profile
│   ├── callsprofile.py        # Calls handling
│   └── ...
│
├── filter/                    # Data filtering
│   └── objects_in_polygon.py  # Geographic filtering
│
├── fix/                       # Data correction utilities
│   ├── day_offset.py          # Day offset corrections
│   ├── direction.py           # Direction fixes
│   ├── relational_constraints.py
│   └── ...
│
├── tools/                     # Command-line tools
│   ├── script_runner.py       # Batch processing
│   ├── gtfs_map_visualisation.py
│   ├── gtfs_validator.py
│   ├── netex_preproc_cleanup.py
│   └── tool_scripts/           # Predefined processing scripts
│
├── utils/                     # Utility functions
│   ├── aux_logging.py         # Logging utilities
│   ├── netex_monkeypatching.py
│   ├── refs.py                # Reference handling
│   └── utils.py               # General utilities
│
├── netexio/                   # Legacy/compatibility layer
│
├── gui/                       # Graphical user interface
│
├── configuration.py           # Global configuration
├── README.md                  # Project overview
├── badger-architecture.md     # Architecture documentation
└── pyproject.toml             # Project configuration
```

## Getting Started

### Prerequisites

1. **Python 3.11+** (recommended: 3.11 or 3.12)
2. **Git** - for version control
3. **uv** - Python package manager (recommended)
   ```bash
   pip install uv
   ```

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/MMTIS/badger.git
cd badger

# Create virtual environment using uv
uv venv

# Install dependencies
uv sync

# Run setup script (Linux/Mac)
sh scripts/setup.sh

# For Windows, run the commands from setup.sh manually:
# (See scripts/setup.sh for details)
```

**Windows Users**: See the [README](README.md) for Windows-specific instructions and a helpful [video tutorial](https://www.youtube.com/watch?v=Kz2p4yQn5rw).

### Environment Variables

Set these environment variables for development:

```bash
# Linux/Mac (add to .bashrc or .zshrc)
export BADGER_HOME=/path/to/badger
export PYTHONPATH="$BADGER_HOME:$PYTHONPATH"
export PATH="$BADGER_HOME/.venv/bin:$PATH"
export BADGER_DATA_DIR="~/data/badger"

# Windows (set in System Environment Variables)
# BADGER_HOME=C:\path\to\badger
# PYTHONPATH=C:\path\to\badger;%PYTHONPATH%
# PATH=C:\path\to\badger\.venv\Scripts;%PATH%
# BADGER_DATA_DIR=C:\data\badger
```

### Generate XML Schema Classes

Badger uses **xsData** to generate Python data classes from NeTEx XML schemas:

```bash
sh scripts/generate-schema.sh
```

This generates the `domain/netex/model/` classes from XML Schema definitions.

### Verify Setup

```bash
# Test that the environment is working
uv run python -c "import badger; print('Badger imported successfully')"

# Run a simple conversion test
uv run python -m conv.netex_to_db --help
```

## Development Workflow

### Understanding the Pipeline

Badger follows an **ETL (Extract, Transform, Load)** paradigm:

```
Input Data → Extract → NeTEx Objects → Transform → Target Format
```

#### Common Conversion Pipelines

**NeTEx to NeTEx (EPIP profile):**
```bash
# Step 1: Import NeTEx XML to MDBX
uv run python -m conv.netex_to_db input.xml.gz input.lmdb

# Step 2: Convert to GeneralFrame (optional)
uv run python -m conv.netex_db_to_generalframe input.lmdb generalframe.lmdb

# Step 3: Transform to EPIP profile
uv run python -m conv.epip_db_to_db generalframe.lmdb output_epip.lmdb

# Step 4: Export to XML
uv run python -m conv.epip_db_to_xml output_epip.lmdb output_epip.xml.gz
```

**GTFS to NeTEx EPIP:**
```bash
# Step 1: Import GTFS to DuckDB
uv run python -m conv.gtfs_import_to_db gtfs.zip gtfs.duckdb

# Step 2: Convert DuckDB to MDBX (NeTEx GeneralFrame)
uv run python -m conv.gtfs_convert_to_db gtfs.duckdb intermediate.lmdb

# Step 3: Transform to EPIP profile
uv run python -m conv.epip_db_to_db intermediate.lmdb output_epip.lmdb

# Step 4: Export to XML
uv run python -m conv.epip_db_to_xml output_epip.lmdb output_epip.xml.gz
```

### Key Concepts to Understand

#### 1. **MDBX Storage**

- **Primary storage backend** for NeTEx objects
- **Memory-mapped key-value store** (based on LMDB)
- **ACID compliant** with transaction support
- Uses **CloudPickle + LZ4 compression** for object serialization
- **ByteSerializer** handles key encoding: `ID\0VERSION\0CLASS_IDX`

#### 2. **Reference Handling**

- NeTEx uses **VersionOfObjectRefStructure** with optional version and class
- Badger resolves references through:
  - `DB_ID_IDX`: ID to full key lookup
  - `DB_UNRESOLVED`: Tracks unresolved references
  - `DB_REFERENCE_OUTWARD`: Tracks resolved reference relationships
- **Critical**: Understand `resolve()` and `resolve_embeddings()` functions

#### 3. **Transformers**

- Each profile (EPIP, Dutch, Nordic) has specific transformers
- Transformers **read from source MDBX, write to target MDBX**
- Common transformers:
  - `epip_line_generator`: Creates EPIP-compliant Lines
  - `epip_service_journey_generator`: Creates EPIP ServiceJourneys
  - `infer_directions_from_sjps_and_apply`: Infer Direction from patterns
  - `reprojection_update`: Project coordinates to target CRS

#### 4. **NeTEx as Intermediate Format**

- **All data is converted to NeTEx objects first**
- **No proprietary intermediate representation**
- **NeTEx IS the intermediate representation**
- This ensures:
  - Audit trail preservation
  - Schema compliance
  - Interoperability

### Code Style and Conventions

#### Python Style

- Follow **PEP 8** conventions
- Use **type hints** extensively
- Prefer **dataclasses** for data structures
- Use **f-strings** for string formatting
- **Docstrings** for public functions and classes

#### Naming Conventions

| Type | Convention | Example |
|------|------------|---------|
| Module | lowercase_with_underscores | `epip_db_to_db.py` |
| Class | PascalCase | `MdbxStorage`, `ServiceJourney` |
| Function | lowercase_with_underscores | `resolve_references`, `epip_line_generator` |
| Variable | lowercase_with_underscores | `source_db`, `target_transaction` |
| Constant | UPPER_CASE_WITH_UNDERSCORES | `DEFAULT_VERSION`, `MAX Batch_SIZE` |
| Type Variable | PascalCase starting with T | `T`, `Tid`, `EntityType` |

#### Logging

- Use the **logging module** (not print statements)
- Import from `utils.aux_logging`: `log_all`, `log_once`, `log_print`
- Log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL

```python
import logging
from utils.aux_logging import log_all, prepare_logger

# Prepare logger
logger = prepare_logger(logging.INFO, "my_module.log")

# Log messages
log_all(logging.INFO, "Processing started")
log_all(logging.DEBUG, f"Processing object {obj.id}")
log_all(logging.WARNING, "Reference not resolved")
log_all(logging.ERROR, "Failed to process")
```

#### Error Handling

- Use **specific exception types** when possible
- Include **context in error messages**
- **Don't catch bare exceptions** - catch specific ones
- Use `try/except` with proper logging

```python
# Good
try:
    obj = storage.load_object(txn, clazz, key)
except KeyError as e:
    log_all(logging.WARNING, f"Object not found: {key} - {e}")
    return None

# Avoid
try:
    obj = storage.load_object(txn, clazz, key)
except Exception as e:
    pass  # Silent failure - BAD!
```

### Making Changes

#### 1. **Understand Before Changing**

- Read existing code and tests
- Understand the data flow
- Identify dependencies
- Check if similar functionality already exists

#### 2. **Follow Existing Patterns**

- Match the **naming conventions**
- Use the **same parameter names**
- Follow the **same error handling** patterns
- Use the **same logging** style

#### 3. **Test Your Changes**

```bash
# Run existing tests
uv run pytest test/ -v

# Run specific test file
uv run pytest test/test_something.py -v

# Run with coverage
uv run pytest test/ --cov=domain --cov=conv --cov=transformers
```

#### 4. **Add Tests for New Functionality**

- Test **happy path** (normal operation)
- Test **edge cases** (empty data, boundary conditions)
- Test **error conditions** (invalid input, missing data)
- Test **performance** for critical paths

#### 5. **Update Documentation**

- Update **README.md** if adding new features
- Update **architecture docs** if changing data flow
- Add **code comments** for complex logic
- Update **type hints** for new functions

### Common Tasks

#### Adding a New Profile

1. Create new transformer module in `transformers/`:
   ```bash
   touch transformers/newprofile.py
   ```

2. Implement profile-specific transformers:
   ```python
   # transformers/newprofile.py
   from domain.netex.model import Line, ServiceJourney
   from storage.mdbx.core.implementation import MdbxStorage
   from mdbx.mdbx import TXN
   from typing import Generator
   
   def newprofile_line_generator(source_db: MdbxStorage, txn: TXN, defaults: dict) -> Generator[Line, None, None]:
       """Generate NewProfile-compliant Line objects."""
       for line in source_db.iter_objects(txn, Line):
           # Apply NewProfile-specific rules
           new_line = Line(
               id=line.id,
               version=line.version or defaults["version"],
               # ... NewProfile-specific fields
           )
           yield new_line
   ```

3. Create conversion module in `conv/`:
   ```python
   # conv/newprofile_db_to_db.py
   from transformers.newprofile import newprofile_line_generator
   
   def newprofile_db_to_db(source: Path, target: Path) -> None:
       # Similar pattern to epip_db_to_db.py
       ...
   ```

4. Add CLI entry point (optional):
   ```python
   # In conv/newprofile_db_to_db.py
   def main(source: str, target: str) -> None:
       newprofile_db_to_db(Path(source), Path(target))
   
   if __name__ == "__main__":
       import argparse
       parser = argparse.ArgumentParser()
       parser.add_argument("source", type=str)
       parser.add_argument("target", type=str)
       args = parser.parse_args()
       main(args.source, args.target)
   ```

#### Adding a New Input Format

1. Create parser in `storage/` or `netexio/`:
2. Create conversion module in `conv/`:
3. Follow the ETL pattern: Extract → NeTEx Objects → Load to MDBX

#### Fixing a Bug

1. **Reproduce** the issue with a minimal test case
2. **Identify** the root cause (which component, which function)
3. **Fix** the issue with minimal changes
4. **Add test** to prevent regression
5. **Verify** the fix doesn't break existing functionality

### Debugging Tips

#### Logging

```python
import logging

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)

# Or use badger's logging
from utils.aux_logging import log_all, prepare_logger
logger = prepare_logger(logging.DEBUG, "debug.log")
```

#### Inspecting MDBX Databases

```python
from storage.mdbx.core.implementation import MdbxStorage
from domain.netex.model import Line

# Open database for inspection
with MdbxStorage("database.lmdb", readonly=True) as storage:
    with storage.env.ro_transaction() as txn:
        # Count objects
        line_count = len(list(storage.iter_objects(txn, Line)))
        print(f"Lines: {line_count}")
        
        # Inspect specific object
        for line in storage.iter_objects(txn, Line):
            print(f"Line: {line.id} v{line.version}")
            break
```

#### Common Issues and Solutions

| Issue | Likely Cause | Solution |
|-------|--------------|----------|
| Reference not resolved | Missing target object | Check source data, use `resolve()` |
| Version mismatch | Version "any" or mismatch | Handle in `resolve()` fallback logic |
| Class not found | Missing import or type | Check `get_interesting_classes()` |
| Serialization error | Non-serializable object | Use simple types, no lambdas |
| Memory error | Large dataset | Use streaming, chunk processing |
| Slow performance | Too many lookups | Use caching, batch operations |

### Performance Considerations

#### Do's

✅ **Use generators** for streaming processing:
```python
def process_objects() -> Generator[MyObject, None, None]:
    for obj in large_dataset:
        yield transform(obj)  # Don't collect in list
```

✅ **Batch database operations**:
```python
# Instead of:
for obj in objects:
    storage.insert_object(txn, obj)

# Use:
storage.insert_any_object_on_queue(txn, objects)  # Batched
```

✅ **Use read-only transactions** for reading:
```python
with storage.env.ro_transaction() as txn:  # Read-only
    for obj in storage.iter_objects(txn, MyClass):
        process(obj)
```

✅ **Cache frequently accessed data**:
```python
# Cache reference lookups
ref_cache: dict[str, bytes] = {}
for obj in objects:
    if obj.id not in ref_cache:
        ref_cache[obj.id] = storage.get_full_key(txn, obj)
```

#### Don'ts

❌ **Don't load entire datasets into memory**:
```python
# BAD:
all_objects = list(storage.iter_objects(txn, MyClass))  # Loads all

# GOOD:
for obj in storage.iter_objects(txn, MyClass):  # Streams
    process(obj)
```

❌ **Don't use long-running write transactions**:
```python
# BAD:
with storage.env.rw_transaction() as txn:
    for i in range(1000000):  # Too long
        process_and_insert(txn, i)

# GOOD:
for i in range(0, 1000000, 10000):
    with storage.env.rw_transaction() as txn:
        for j in range(i, min(i+10000, 1000000)):
            process_and_insert(txn, j)
```

❌ **Don't use `pickle` directly**:
```python
# BAD:
import pickle
data = pickle.dumps(obj)  # Not compatible

# GOOD:
# Use storage.serializer.marshall() which uses CloudPickle + LZ4
```

### Git Workflow

#### Branching Strategy

1. **Create feature branch** from appropriate base:
   ```bash
   # For new features
   git checkout -b feature/your-feature-name main
   
   # For bug fixes
   git checkout -b fix/your-bug-name main
   
   # For documentation
   git checkout -b docs/your-docs main
   ```

2. **Commit messages** should be descriptive:
   ```bash
   git commit -m "Add EPIP line generator transformer
   
   - Implement epip_line_generator function
   - Add support for EPIP-specific Line fields
   - Update epip_db_to_db to use new generator
   - Add tests for line generation
   
   Generated by Mistral Vibe.
   Co-Authored-By: Mistral Vibe <vibe@mistral.ai>"
   ```

3. **Pull Request process**:
   - Create PR to `main` branch
   - Include **description** of changes
   - Include **screenshots** if UI changes
   - Include **test results**
   - Request **review** from maintainers

#### Commit Message Format

```
<type>(<scope>): <subject>

<body>

<footer>
```

Where:
- **type**: feat, fix, docs, style, refactor, test, chore
- **scope**: module or component affected
- **subject**: Brief description (50 chars or less)
- **body**: Detailed description (wrap at 72 chars)
- **footer**: Metadata (Generated by, Co-Authored-By, etc.)

### Community and Communication

#### Getting Help

1. **Read the documentation** first:
   - `README.md` - Project overview
   - `badger-architecture.md` - Architecture details
   - This document - Contributing guide

2. **Check existing issues** on GitHub:
   - https://github.com/MMTIS/badger/issues
   - Your question might already be answered

3. **Ask questions** in discussions:
   - https://github.com/MMTIS/badger/discussions
   - Be specific about what you're trying to do

4. **Join the community**:
   - Check for Slack/Discord/Matrix channels
   - Attend community meetings if available

#### Contributing Back

1. **Report bugs**:
   - Include **steps to reproduce**
   - Include **error messages**
   - Include **data samples** if possible (sanitized)

2. **Suggest features**:
   - Explain the **use case**
   - Describe the **expected behavior**
   - Provide **examples** if possible

3. **Submit PRs**:
   - Follow the **coding standards**
   - Include **tests**
   - Update **documentation**
   - Keep PRs **focused** (one feature/bug per PR)

4. **Review others' PRs**:
   - Be **constructive**
   - Focus on **code quality**, not personal style
   - Suggest **improvements**, don't just approve

### Useful Resources

#### Internal Documentation

| File | Description |
|------|-------------|
| `README.md` | Project overview, setup, usage |
| `badger-architecture.md` | Detailed architecture analysis |
| `mdbx-usage-analysis.md` | MDBX storage analysis |
| `mdbx-reference-approach-analysis.md` | Reference handling analysis |
| `pipeline.md` | Pipeline architecture (this repo) |
| `conversion.md` | Conversion call graphs (this repo) |
| `fix_or_transform.md` | Fix vs transform guidance (this repo) |

#### External Resources

| Resource | Description |
|----------|-------------|
| [NeTEx CEN Standard](https://standards.cen.eu/) | Official NeTEx specifications |
| [xsData Documentation](https://xsdata.readthedocs.io/) | XML Schema to Python classes |
| [LMDB Documentation](https://lmdb.readthedocs.io/) | LMDB database documentation |
| [MDBX Documentation](https://github.com/erthink/libmdbx) | MDBX (enhanced LMDB) |
| [DuckDB Documentation](https://duckdb.org/docs) | DuckDB database |
| [CloudPickle](https://github.com/cloudpipe/cloudpickle) | Enhanced pickle for complex objects |

#### Example Data

- Check `data/` directory for sample files
- National timetable data (if available)
- Test fixtures in `test/` directory

### Testing Your Setup

Run these commands to verify your environment:

```bash
# Test basic import
uv run python -c "from storage.mdbx.core.implementation import MdbxStorage; print('MDBX OK')"

# Test xsData models
uv run python -c "from domain.netex.model import Line, ServiceJourney; print('NeTEx models OK')"

# Test a simple conversion (if you have sample data)
uv run python -m conv.netex_to_db --help
```

### Troubleshooting

#### Common Setup Issues

| Issue | Solution |
|-------|----------|
| `uv: command not found` | Install uv: `pip install uv` |
| Python version not supported | Use Python 3.11+ |
| Missing dependencies | Run `uv sync` |
| xsData not generating models | Run `sh scripts/generate-schema.sh` |
| MDBX DLL not found (Windows) | Check .venv/Lib/site-packages for DLL |
| Permission errors on .lmdb files | Run as administrator / check directory permissions |

#### Windows-Specific Issues

1. **Long path errors**:
   - Enable long paths in Windows: Group Policy → System → Filesystem → Enable Win32 long paths
   - Or use shorter paths for development

2. **File locking**:
   - Close file explorers that might have files open
   - Use `readonly=True` when only reading

3. **DLL loading**:
   - Ensure MDBX DLL is in PATH or same directory
   - Check `mdbx` package installation

### Long-Term Development Tips

#### Understand the Architecture First

Before making significant changes:
1. Read `badger-architecture.md` thoroughly
2. Understand the **data flow** through the pipeline
3. Know the **key components**: Storage, Transformers, Domain
4. Understand **reference handling** - it's critical

#### Learn the Domain

- **NeTEx**: European standard for public transport data exchange
- **GTFS**: General Transit Feed Specification (Google's format)
- **EPIP**: European Passenger Information Profile (NeTEx subset)
- **VDV462**: German public transport standard
- **Nordic Profile**: NeTEx profile for Nordic countries

#### Follow the Development

- Watch the repository for changes
- Read commit messages and PR descriptions
- Check for architecture discussions in issues/PRs
- Attend community meetings if available

#### Start Small

- Begin with **small, focused changes**
- Fix **bugs** before adding **features**
- Add **tests** for your changes
- **Document** your work

## Summary Checklist for New Contributors

- [ ] Set up development environment with `uv`
- [ ] Run `uv sync` to install dependencies
- [ ] Generate schema classes with `scripts/generate-schema.sh`
- [ ] Understand the ETL pipeline: Extract → NeTEx → Transform → Output
- [ ] Learn MDBX storage and reference handling
- [ ] Follow existing code patterns and naming conventions
- [ ] Use proper logging (not print statements)
- [ ] Test changes before committing
- [ ] Add tests for new functionality
- [ ] Update documentation for changes
- [ ] Follow Git workflow (feature branches, descriptive commits)
- [ ] Engage with the community (questions, PRs, discussions)

## Need More Help?

If you're still stuck after reading this guide:

1. **Check the issues**: https://github.com/MMTIS/badger/issues
2. **Start a discussion**: https://github.com/MMTIS/badger/discussions
3. **Look at existing PRs**: https://github.com/MMTIS/badger/pulls
4. **Examine test files**: `test/` directory has many examples
5. **Study the architecture docs**: `badger-architecture.md`

## Thank You! 🙏

Your contributions help make Badger better for everyone. We appreciate your time, effort, and expertise. Happy coding! 🚀

---

*This document is a work in progress. Please help improve it by suggesting additions or corrections.*
