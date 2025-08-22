from pathlib import Path
import duckdb

from domain.gtfs.transform.codespace import getCodespace
from domain.gtfs.transform.datasource import getDataSource
from domain.gtfs.transform.line import getLines
from domain.gtfs.transform.operator import getOperators
from domain.gtfs.transform.validbetween import getValidBetween
from domain.gtfs.transform.version import getVersion
from domain.netex.model import Codespace, DataSource, Operator, Line
from storage.interface import Storage


def to_storage(database_file: Path, storage: Storage):
    with duckdb.connect(database=database_file.resolve(), read_only=True) as con:
        version = getVersion(con)
        valid_between = getValidBetween(con)
        codespace = getCodespace(con)
        datasource = getDataSource(con, codespace, version)
        storage.insert_objects_on_queue(Codespace, [codespace])
        storage.insert_objects_on_queue(DataSource, [datasource])
        storage.insert_objects_on_queue(Operator, getOperators(con, codespace, version))
        storage.insert_objects_on_queue(Line, getLines(con, codespace, version))