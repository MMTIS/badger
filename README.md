# Badger

An extremely fast, state of the art, and consistent approach for timetable conversions.

__Note:__ This project is in _technical preview_.
Most of the source code of this project may not have been cleaned up.
There are certain references to earlier generations of the software design.
If you would like to participate in the development we would love to get you up to speed.

### Setup

Install `uv` as package management tool.
For example via `pip install uv`.

```sh
uv venv
uv sync
sh scripts/setup.sh
```
*For Microsoft Windows users, see the commands in the [shell file](scripts/setup.sh).*

### Update schemas
```sh
sh scripts/generate-schema.sh
```
*For Microsoft Windows users, see the commands in the [shell file](scripts/generate-schema.sh).*

## Architecture

Badger takes concepts of various timetable formats and converts them in NeTEx objects.
These NeTEx objects can then be used in later conversions towards a different standard or NeTEx Profile.
As such it can also process arbitrary NeTEx objects and (try) to sense out of them.
In this technology preview we will incrementally support various profiles, including GTFS, IFF, NeTEx (profiles: Dutch, EPIP, Italian, Nordic and VDV462).
When the data is extracted (and potentially transformed) into NeTEx it will retain all its defined properties.
All information is kept as-is, there is no proprietary intermediate representation, NeTEx *is* the intermediate presentation.
As such, the processing has an audit trail.

Our (de)serialisation takes place using [xsData](https://xsdata.readthedocs.io/en/latest/), via Python Data Classes.
This guarantees us XML Schema compliance.
Our intermediate presentation is serialised using [cloudpickle](https://github.com/cloudpipe/cloudpickle) and is stored in compressed [lz4](https://lmdb.readthedocs.io/en/release/) form.
During the development of this software we have evaluated various database technologies, for our intermediate computing requirements [lmdb](https://lmdb.readthedocs.io/en/release/) is used.
The processing of other CSV-based formats such as GTFS is mediated via  [DuckDB](https://duckdb.org/docs/stable/clients/python/overview.html).

### Extract, Transform, Load
The interaction with source data follows the paradigm of common ETL.
A source is extracted, optionally transformed in NeTEx, and loaded into a key-value database.
This database should be observed as a _NeTEx GeneralFrame_.
All first class objects are represented as-is.
Considering the use of public use of NeTEx, we have experienced that the use is different from what was academically intended.
For this reason we denormalise the FrameDefaults, inherit versions, and make sure that geographic projections as explicitly set.
The goal is that the processing later can trust fields are set with sane defaults.

Now a NeTEx database is available, this database is read by an implementation of a NeTEx target profile.
Within this step the software infers, projects and transforms source concepts in the mandatory elements defined by the target profile.
For example, it can transform a *ServiceJourneyPattern* and a *TimeDemandType* into *TimetabledPassingTimes*.

With the output of the previous step, an XML-export can be created, meeting the final requirement of the target profile.
In the case a different format is required, our intermediate NeTEx presentation is still used as source, but the output mechanism is then not XML.

#### NeTEx EPIP example
```sh
uv run python -m conv.netex_db_to_generalframe path_to_input_netex path_to_intermediate_presentation.xml.gz
uv run python -m conv.epip_db_to_db path_to_input_netex.lmdb path_to_output_epip.lmdb
uv run python -m conv.epip_db_to_xml path_to_output_epip.lmdb path_to_output_epip.xml.gz
```

#### GTFS to NeTEx EPIP example
```sh
uv run python -m conv.gtfs_import_to_db path_to_gtfs.zip path_to_gtfs.duckdb
uv run python -m conv.gtfs_convert_to_db path_to_gtfs.duckdb path_to_intermediate.lmdb
uv run python -m conv.epip_db_to_db path_to_intermediate.lmdb path_to_output_epip.lmdb
uv run python -m conv.epip_db_to_xml path_to_output_epip.lmdb path_to_output_epip.xml.gz
```

#### NeTEx EPIP to GTFS example
```sh
uv run python -m conv.netex_to_db path_to_xml.gz path_to_input_netex.lmdb
uv run python -m conv.gtfs_db_to_db path_to_input_netex.lmdb path_to_output_gtfs.lmdb
uv run python -m conv.gtfs_db_to_gtfs path_to_output_gtfs.lmdb path_to_output_gtfs.zip
```


### Performance
The software has carefully been designed with streaming and performance in mind, while meeting the reproducibility requirements.
We limit the initial memory usage by employing SAX based XML parsing.
The writing of objects is done via a task queue in different thread, allowing for further parallelisation in the future.
When transforming objects we do so in a streaming fashion.
A read-only database is used for reading, which can be access from various processes.
In this step we ideally use generators that do not require intermediate working memory or have interdependencies on previously processed objects.
An object in, would be ideally flow as a new object towards the second database.
For performance reasons we cannot always follow this pattern, because we would like to prevent database access and minimise computing power for already created (similar) objects.
There are some custom compression patterns we use to retain prefix encoding, with fewer bytes used as table key, this only works for ASCII based ids.
For writing towards XML we create generators that would be called exactly when the data is being serialised towards XML at element level, just in time.