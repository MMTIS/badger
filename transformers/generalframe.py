# This code is written to export all data into a NeTEx GeneralFrame this should be our complete database state
from typing import Generator  # noqa: F401

from mdbx.mdbx import TXN
from xsdata.models.datatype import XmlDateTime

from domain.netex.model import (
    PublicationDelivery,
    ParticipantRef,
    DataObjectsRelStructure,
    GeneralFrame,
    GeneralFrameMembersRelStructure,
    EntityStructure,
)  # noqa: F401
from storage.mdbx.core.implementation import MdbxStorage
from storage.mdbx.tools.graph import export_objects

from utils.utils import chain


def export_to_general_frame(storage: MdbxStorage, txn: TXN, optimal: bool = True) -> PublicationDelivery:
    if optimal:
        # Our graph based sort makes sure that the objects are exported so an importer has all the referenced objects available.
        publication_delivery = PublicationDelivery(
            version="ntx:1.1",
            publication_timestamp=XmlDateTime.now(),
            participant_ref=ParticipantRef(value="PyNeTExConv"),
            data_objects=DataObjectsRelStructure(
                choice=[GeneralFrame(id="Database", version="1", members=GeneralFrameMembersRelStructure(choice=export_objects(txn, storage)))]  # type: ignore
            ),
        )

    else:
        # This is the naive, but fast implementation. It groups the classes.
        tables: dict[bytes, type[EntityStructure]] = dict(
            sorted(storage.db_names(txn).items(), key=lambda item: item[1].__name__)
        )  # To ensure predictable order
        iterables = [(t for _, t in storage.iter_objects(txn, clazz)) for clazz in tables.values()]
        publication_delivery = PublicationDelivery(
            version="ntx:1.1",
            publication_timestamp=XmlDateTime.now(),
            participant_ref=ParticipantRef(value="PyNeTExConv"),
            data_objects=DataObjectsRelStructure(
                choice=[GeneralFrame(id="Database", version="1", members=GeneralFrameMembersRelStructure(choice=chain(*iterables)))]  # type: ignore
            ),
        )

    return publication_delivery
