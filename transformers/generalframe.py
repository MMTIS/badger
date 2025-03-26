# This code is written to export all data into a NeTEx GeneralFrame this should be our complete database state
from typing import Generator  # noqa: F401

from xsdata.models.datatype import XmlDateTime

from netex import PublicationDelivery, ParticipantRef, DataObjectsRelStructure, GeneralFrame, GeneralFrameMembersRelStructure, EntityStructure  # noqa: F401
from netexio.database import Database
from netexio.dbaccess import load_generator

from utils.utils import chain


def export_to_general_frame(db: Database) -> PublicationDelivery:
    # TODO: This is not the correct way of loading a module by name
    iterables = [load_generator(db, clazz, embedding=False) for clazz in db.tables()]  # type: list[Generator[EntityStructure, None, None]]

    publication_delivery = PublicationDelivery(
        version="ntx:1.1",
        publication_timestamp=XmlDateTime.now(),
        participant_ref=ParticipantRef(value="PyNeTExConv"),
        data_objects=DataObjectsRelStructure(
            choice=[GeneralFrame(id="Database", version="1", members=GeneralFrameMembersRelStructure(choice=chain(*iterables)))]  # type: ignore
        ),
    )

    return publication_delivery
