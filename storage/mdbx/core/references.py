import logging

from mdbx import MDBXCursorOp, MDBXDBFlags

from domain.netex.indexes.inverse_class import collect_classes_index
from utils.aux_logging import log_all
from domain.netex.services.model_typing import Tid
from domain.netex.model import EntityStructure, Network, NameOfClass
from domain.netex.services.recursive_attributes import only_reference_objects, only_embedding, embedding_obj_iter
from domain.utils import get_object_name
from storage.mdbx.core.implementation import (
    MdbxStorage,
    DB_UNRESOLVED,
    DB_REFERENCE_OUTWARD,
    DB_ID_IDX,
    DB_EMBEDDED_ID_IDX,
    DB_UNRESOLVED_FLAGS,
    DB_REFERENCE_OUTWARD_FLAGS,
    DB_ID_IDX_FLAGS,
    DB_EMBEDDED_ID_IDX_FLAGS,
)
from storage.mdbx.serialization.byteserializer import ByteSerializer
from mdbx.mdbx import TXN
from typing import Optional, Generator


def resolve_embeddings_iterable(
    storage: MdbxStorage, txn: TXN, clazz: type[EntityStructure], interesting_classes: Optional[set[Tid]] = None, ignore: Optional[set[Tid]] = None
) -> Generator[tuple[bytes, type[EntityStructure], type[EntityStructure]], None, None]:
    """
    In resolve_embeddings we are creating a lookup from an existing instance to the location an embedded object remains.
    Hence, it is not 'you can find this embedded object there' but 'this object has a relationship with that object'.
    When exporting or processing data for a specific profile, we may be interested in the exact locations of these
    references. An example could be a DayType, which may be part of a ServiceCalendar.

    If for every lookup we must deserialise the entire database this is unfeasible. The problem, taking the not naive
    approaches:

    1. When we would store the identifiers of all possible objects embedded within this specific object,
    this potentially causes a huge table. With O(1) access via ids, with the chance we would never ever
    require such individual access ever. The further downside is that upon insert we must maintain such
    table, hence for every write such thing must be checked.

    2. When we would consider embedded object "not a good fit" we could deembed them, regardless of the NeTEx-schema
    allowing it to be a first class object. We could rewrite the original object to take a reference (if possible).
    If the reference is possible we could cleanly store the deembedded object, and manipulate it. If not, we must
    update the new location, and the initial embedding.

    3. When we would do our computation using collect_classes_index and get the reverse index of all potential
    objects and would iterate over all potential "parent" candidates, which are already limited to
    "just iterate over all objects" would still cause the effect that for every individual query we would
    deserialise a parent-type. Hence, if we would be looking for Quays and later Entrances, we would be serialising
    StopPlaces twice. We could overcome this by registering which object types we are interested in, do the full
    scan (limited to the collect_classes_index via the classes of interest) and then either create a lookup or
    a direct materialised view. The latter assumes we are not changing the embedded object, but only query it once.
    """

    for key, obj in storage.iter_objects(txn, clazz):
        for candidate in embedding_obj_iter(storage.serializer, obj, interesting_classes, ignore):
            yield key, obj, candidate


def variant_of_candidate_in_list(storage, candidate, obj, unresolved_pairs: dict[bytes, set[bytes]]):
    parts = storage.serializer.split_key(candidate)
    class_change = False
    version_change = False
    resolved_idx = None
    result = None

    # TODO: Abstract the separator, so it is uniform through the code
    separator = bytes([ByteSerializer.SEPARATOR])

    if candidate in unresolved_pairs:
        return candidate, False, False, candidate

    else:
        class_part = separator + parts[-1]
        parts.pop()
        prefix_id_version = separator.join(parts) + separator
        parts.pop()
        prefix_id = separator.join(parts) + separator

        for check_key in unresolved_pairs.keys():
            if check_key.startswith(prefix_id_version):
                resolved_idx = candidate
                class_change = NameOfClass(get_object_name(obj.__class__)) # Warning, see other affraid
                version_change = False
                result = check_key
                break
            elif check_key.startswith(prefix_id) and check_key.endswith(class_part):
                resolved_idx = candidate
                class_change = False
                version_change = obj.version
                result = check_key
                break
            elif check_key.startswith(prefix_id):
                resolved_idx = candidate
                class_change = NameOfClass(get_object_name(obj.__class__)) # Warning, see other affraid
                version_change = obj.version
                result = check_key
                # Search if we get something better

    return resolved_idx, version_change, class_change, result


def resolve_embeddings(storage: MdbxStorage):
    """
    We have a list of unresolved elements, from this elements we know their "origin" meaning the objects in which this reference appears.
    In our worst case example multiple missing references are part of the origin object we are searching in.
    We want to avoid at all costs that we will be rewriting objects prior to all references of that object have been acknowledged.
    """

    missing_classes = set([])
    unresolved_pairs: dict[bytes, set[bytes]] = {}
    references_to_fix: list[tuple] = []

    with storage.env.rw_transaction() as txn:
        db_unresolved = txn.open_map(DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
        db_reference_outward = txn.open_map(DB_REFERENCE_OUTWARD, flags=DB_REFERENCE_OUTWARD_FLAGS)

        unresolved_cursor = txn.cursor(db=db_unresolved)
        for idx, value in unresolved_cursor.iter():
            # print(idx, value)
            parts = storage.serializer.split_key(value)
            unresolved_pairs.setdefault(value, set()).add(idx)
            missing_classes.add(storage.idx_class[parts[-1]])

        # TODO: computer the superset of missing_classes.

        # TODO: wat hier ontbreekt is dat we hier kijken naar de nameOfRefClass, onder de aanname "dit zou het kunnen zijn"
        # wat we hier moeten doen is alle kind varianten ook meenemen, waar kan deze reference nog meer naar verwijzen, gegeven dat de class foutief of niet is bepaald.
        # Dus voor OperatingPeriodRef niet alleen OperingPeriod, maar ook UicOperatingPeriod.

        # TODO: Here we compute where we could find the missing_classes, as embedded objects. But we must not forget that there may be sub classes referenced as well,
        # hence these subclasses must also be computed. In case of a fully invalid class ref, this won't hold, because we don't have a clue what it should be.
        # This should already be covered by our upstream, setting the default or most generic class.
        used_classes_in_database = set(storage.db_names(txn).values())
        index = collect_classes_index(used_classes_in_database, scope_classes=missing_classes)
        clazzes: set[type] = set().union(*index.values())

        class_count: dict[type, int] = {}
        for clazz in clazzes:
            db = txn.open_map(storage.class_idx[clazz], flags=MDBXDBFlags.MDBX_DB_DEFAULTS)
            class_count[clazz] = db.get_stat(txn).ms_entries

        almost_resolved_queue = {}

        log_all(logging.INFO, f"[resolve_embeddings] {len(unresolved_pairs)} unresolved refs, "
                              f"scanning {len(class_count)} class maps for {len(missing_classes)} missing classes")

        for clazz, count in sorted(class_count.items(), key=lambda item: item[1]):
            log_all(logging.INFO, f"[resolve_embeddings] scanning {clazz.__name__} ({count} objects)")
            db = txn.open_map(storage.class_idx[clazz], flags=MDBXDBFlags.MDBX_DB_DEFAULTS)
            with txn.cursor(db) as cur:
                for idx, value in cur.iter():
                    obj: Tid = storage.serializer.unmarshall(value, clazz)
                    write = False
                    # TODO: None should be replaced with the set of potential sub classes, hence the superset of missing_classes.
                    for candidate, obj in only_embedding(storage.serializer, obj, None): # we zouden hier direct het path binnen het object ook kunnen opslaan
                        # for candidate in only_embedding(storage.serializer, obj, missing_classes):
                        # TODO: deze operatie faalt op het moment dat a) de nameOfRefClass foutief is, b) version foutief is c) het verwacht ook dat we alle classen zoeken, terwijl we eigenlijk zouden willen limiteren op de missende classen

                        # Misschien willen we hier iets doen door de exacte matchende candidaten rechtstreeks af te handelen,
                        # Maar te wachten totdat we er zeker van zijn dat er geen betere match bestaat.
                        resolved_id, version_change, class_change, unresolved = variant_of_candidate_in_list(storage, candidate, obj, unresolved_pairs)

                        if resolved_id:
                            if not version_change and not class_change:
                                # Dit is simpel: in principe kunnen we nu direct alles wegschrijven
                                full_key = ((int.from_bytes(storage.class_idx[clazz], 'little') << 32) | int.from_bytes(
                                    idx, 'little')).to_bytes(8, 'little')
                                for resolved_index in unresolved_pairs[resolved_id]:
                                    # Bij deze twee schrijfacties ontstaat build/lib/mdb.c:2156: Assertion 'rc == 0' failed in mdb_page_dirty()
                                    db_reference_outward.put(txn, resolved_index, full_key)
                                    db_unresolved.delete(txn, resolved_index, resolved_id)
                                del unresolved_pairs[unresolved]
                                if unresolved in almost_resolved_queue:  # TODO: more elegant?
                                    del almost_resolved_queue[unresolved]
                                write = True

                            else:
                                # In deze situatie kunnen er een aantal varianten optreden:
                                # We gaan later (dus in een ander object) een betere match vinden, waar of de class of de version wel gelijk is.
                                # Daarom slaan we deze "kandidaat match" wel op, we slaan natuurlijk ook waar we het object hebben gevonden.
                                # TODO: almost resolved queue
                                if resolved_id not in almost_resolved_queue:
                                    parent_full_idx = ((int.from_bytes(storage.class_idx[clazz], 'little') << 32) | int.from_bytes(idx, 'little')).to_bytes(8, 'little')
                                    almost_resolved_queue[unresolved] = (resolved_id, version_change, class_change, parent_full_idx)

        if len(almost_resolved_queue) > 0:
            # Er zijn dus net-niet matches, maar dat betekent ook dat de referenties naar deze matches niet juist zijn in de bron
            # We willen nu voorkomen dat we voor iedere referentie het bron object opnieuw gaan openen.
            # Waar we voor de unresolve_paired lijst de matches groepeerden naar gelijke uitgaande referenties, willen we juist dat alle referenties binnen een object worden gegroepperd.

            unresolved_pairs_inverted = {}
            for missing_idx, part_of_objects in unresolved_pairs.items():
                for obj_idx in part_of_objects:
                    unresolved_pairs_inverted.setdefault(obj_idx, set()).add(missing_idx)

            for referencing_idx, unresolved in unresolved_pairs_inverted.items():
                # idx is hier het object waarin de referentie staat
                referencing_obj = storage.load_object_by_full_key(txn, referencing_idx)
                write = False

                # we lopen hier alle referenties weer langs
                # TODO: we zouden hier ook kunnen kiezen om het path te nemen en dan de lookup direct te doen, dat hebben we immers vanuit de vorige only_reference_objects
                for reference in only_reference_objects(referencing_obj):
                    if isinstance(reference.name_of_ref_class, str):
                        # TODO: I think we want to write to the console that a NameOfRefClass has been specified that does not match the natural scope of the Reference.
                        # print(reference.name_of_ref_class, reference)
                        if reference.name_of_ref_class not in storage.serializer.name_object:
                            # This should already be cleaned upon import. Should never happen
                            # TODO: assert
                            continue
                        name_of_ref_class = reference.name_of_ref_class

                    elif reference.name_of_ref_class.value not in storage.serializer.name_object:
                        # TODO: Add a warning.
                        name_of_ref_class = reference.name_of_ref_class.value

                    else:
                        name_of_ref_class = reference.name_of_ref_class.value

                    cmp_value = storage.serializer.encode_key(
                        reference.ref, getattr(reference, "version", "any"),
                        storage.serializer.name_object[name_of_ref_class], True
                    )

                    if cmp_value in almost_resolved_queue:
                        resolved_id, version_change, class_change, parent_full_idx = almost_resolved_queue[cmp_value]

                        if class_change:
                            reference.name_of_ref_class = class_change

                        if version_change:
                            reference.version = version_change

                        # TODO
                        db_reference_outward.put(txn, referencing_idx, parent_full_idx)
                        try:
                            db_unresolved.delete(txn, referencing_idx, cmp_value) # Deze is correct...
                        except:
                            pass
                        write = True

                if write:
                    referenced_class_idx, referenced_key = storage.serializer.full_key_to_idx(referencing_idx)
                    db = txn.open_map(referenced_class_idx, flags=MDBXDBFlags.MDBX_DB_DEFAULTS)
                    db.put(txn, referenced_key, storage.serializer.marshall(referencing_obj, referencing_obj.__class__))

            # unmarchal
                # only_references


                    # Hier hebben we nu uitgevonden dat er een relatie bestaat tussen een missende referentie die in een aantal idx'en bestaat
                    # En nu willen we eigenlijk alle schrijf acties bundelen per bron object, de idx die in de unresolved_pairs lijst staat

                    # if write:
                    #    db.put(txn, idx, storage.serializer.marshall(obj, obj.__class__))

        txn.commit()


def resolve(storage: MdbxStorage) -> None:
    if storage.readonly:
        raise
        
    separator = bytes([ByteSerializer.SEPARATOR])

    seen_count = 0

    with storage.env.rw_transaction() as txn:
        db_unresolved = txn.open_map(DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
        db_id_idx = txn.open_map(DB_ID_IDX, flags=DB_ID_IDX_FLAGS)
        db_reference_forward = txn.open_map(DB_REFERENCE_OUTWARD, flags=DB_REFERENCE_OUTWARD_FLAGS)

        log_all(logging.INFO, "[resolve] resolving references")
        unresolved_cursor = txn.cursor(db=db_unresolved)
        for it in unresolved_cursor.iter_dupsort_rows():
            references_to_fix: list[tuple] = []

            for idx, value in it:
                seen_count += 1
                if seen_count % 1_000_000 == 0:
                    log_all(logging.INFO, f"[resolve] {seen_count} references processed...")
                parts: list[str] | None = None
                prefix: str | None = None
                resolved_idx = db_id_idx.get(txn, value)  # This will be the id + version + class check
                class_change = False
                version_change = False
                if not resolved_idx:
                    cursor = txn.cursor(db=db_id_idx)

                    parts = storage.serializer.split_key(value)
                    class_part = separator + parts[-1]

                    # Alternative 1, id + version exists, class does not match
                    parts.pop()
                    prefix = separator.join(parts) + separator
                    for check_key, check_idx in cursor.iter(prefix):
                        if check_key.startswith(prefix):
                            class_change = check_idx
                            resolved_idx = check_idx
                        break

                    if not resolved_idx:
                        # Alternative 2, id exists
                        parts.pop()

                        prefix = separator.join(parts) + separator
                        for check_key, check_idx in cursor.iter(prefix):
                            if check_key.startswith(prefix) and check_key.endswith(class_part):
                                class_change = False
                                version_change = check_idx
                                resolved_idx = check_idx
                                break

                            elif check_key.startswith(prefix):
                                class_change = check_idx
                                version_change = check_idx
                                resolved_idx = check_idx
                                # Continue to find a better match

                            else:
                                break

                if resolved_idx:
                    if version_change or class_change:
                        references_to_fix.append((resolved_idx, value, version_change, class_change))

                    # f = storage.load_object_by_full_key(txn, idx)
                    # t = storage.load_object_by_full_key(txn, resolved_idx)

                    # print(f"{f.id} {f.__class__} -> {t.id} {t.__class__}")

                    db_reference_forward.put(txn, idx, resolved_idx)
                    unresolved_cursor.delete(MDBXCursorOp.MDBX_PREV)

                # else:
                #    print("unresolved", value, idx)

            # In this situation the original reference was incomplete
            if len(references_to_fix) > 0:
                referencing_class_idx, referencing_key = storage.serializer.full_key_to_idx(idx)
                referencing_class = storage.idx_class[referencing_class_idx]
                referencing_obj: Tid = storage.load_object(txn, referencing_class, referencing_key)

                for resolved_idx, value, version_change, class_change in references_to_fix:
                    referenced_class_idx, referenced_key = storage.serializer.full_key_to_idx(resolved_idx)

                    for reference in only_reference_objects(referencing_obj):
                        if isinstance(reference.name_of_ref_class, str):
                            # TODO: I think we want to write to the console that a NameOfRefClass has been specified that does not match the natural scope of the Reference.
                            # print(reference.name_of_ref_class, reference)
                            if reference.name_of_ref_class not in storage.serializer.name_object:
                                reference.name_of_ref_class = 'DataManagedObject'
                            name_of_ref_class = reference.name_of_ref_class

                        elif reference.name_of_ref_class.value not in storage.serializer.name_object:
                            # TODO: Add a warning.
                            reference.name_of_ref_class = 'DataManagedObject'

                        else:
                            name_of_ref_class = reference.name_of_ref_class.value

                        cmp_value = storage.serializer.encode_key(
                            reference.ref, getattr(reference, "version", "any"), storage.serializer.name_object[name_of_ref_class], True
                        )
                        if value == cmp_value:
                            if class_change:
                                referenced_class = storage.idx_class[referenced_class_idx]
                                reference.name_of_ref_class = NameOfClass(
                                    get_object_name(referenced_class)
                                )  # I am very afraid how this might be handled in terms of comparisons later.
                            if version_change:
                                referenced_clazz = storage.idx_class[referenced_class_idx]
                                referenced_obj: Tid = storage.load_object(txn, referenced_clazz, referenced_key)
                                reference.version = referenced_obj.version

                # TODO: buffer this write to ~10000 objects of the same type?
                db = txn.open_map(referencing_class_idx, flags=MDBXDBFlags.MDBX_DB_DEFAULTS)
                db.put(txn, referencing_key, storage.serializer.marshall(referencing_obj, referencing_obj.__class__))

        txn.commit()

def resolve_embeddings_index(storage: MdbxStorage):
    missing_classes = set([])
    unresolved_pairs: dict[bytes, set[bytes]] = {}
    references_to_fix: list[tuple] = []

    separator = bytes([ByteSerializer.SEPARATOR])

    if storage.readonly:
        raise

    with storage.env.rw_transaction() as txn:
        db_unresolved = txn.open_map(DB_UNRESOLVED, flags=DB_UNRESOLVED_FLAGS)
        db_id_idx = txn.create_map(DB_EMBEDDED_ID_IDX, flags=DB_EMBEDDED_ID_IDX_FLAGS)
        db_reference_forward = txn.open_map(DB_REFERENCE_OUTWARD, flags=DB_REFERENCE_OUTWARD_FLAGS)

        unresolved_cursor = txn.cursor(db=db_unresolved)
        for idx, value in unresolved_cursor.iter():
            # print(idx, value)
            parts = storage.serializer.split_key(value)
            unresolved_pairs.setdefault(value, set()).add(idx)
            missing_classes.add(storage.idx_class[parts[-1]])

        log_all(logging.INFO, f"[unresolved references] {db_unresolved.get_stat(txn).ms_entries}")

        used_classes_in_database = set(storage.db_names(txn).values())
        index = collect_classes_index(used_classes_in_database, scope_classes=missing_classes)
        clazzes: set[type] = set().union(*index.values())

        for clazz in clazzes:
            this_class_idx = storage.class_idx[clazz]
            db = txn.open_map(this_class_idx, flags=MDBXDBFlags.MDBX_DB_DEFAULTS)
            with txn.cursor(db) as cur:
                for idx, value in cur.iter():
                    full_key = idx + this_class_idx.ljust(4, b'\x00')
                    obj: Tid = storage.serializer.unmarshall(value, clazz)

                    # TODO: None should be replaced with the set of potential sub classes, hence the superset of missing_classes
                    for candidate, _obj in only_embedding(storage.serializer, obj, None):  # we zouden hier direct het path binnen het object ook kunnen opslaan
                        db_id_idx.put(txn, candidate, full_key)

        # TODO: If this works, deduplicate the code with the regular one
        unresolved_cursor = txn.cursor(db=db_unresolved)
        for it in unresolved_cursor.iter_dupsort_rows():
            references_to_fix: list[tuple] = []

            for idx, value in it:
                parts: list[str] | None = None
                prefix: str | None = None
                resolved_idx = db_id_idx.get(txn, value)  # This will be the id + version + class check
                class_change = False
                version_change = False
                if not resolved_idx:
                    cursor = txn.cursor(db=db_id_idx)

                    parts = storage.serializer.split_key(value)
                    class_part = separator + parts[-1]

                    # Alternative 1, id + version exists, class does not match
                    parts.pop()
                    prefix = separator.join(parts) + separator
                    for check_key, check_idx in cursor.iter(prefix):
                        if check_key.startswith(prefix):
                            class_change = check_idx
                            resolved_idx = check_idx
                        break

                    if not resolved_idx:
                        # Alternative 2, id exists
                        parts.pop()

                        prefix = separator.join(parts) + separator
                        for check_key, check_idx in cursor.iter(prefix):
                            if check_key.startswith(prefix) and check_key.endswith(class_part):
                                class_change = False
                                version_change = check_idx
                                resolved_idx = check_idx
                                break

                            elif check_key.startswith(prefix):
                                class_change = check_idx
                                version_change = check_idx
                                resolved_idx = check_idx
                                # Continue to find a better match

                            else:
                                break

                if resolved_idx:
                    if version_change or class_change:
                        references_to_fix.append((resolved_idx, value, version_change, class_change))

                    # f = storage.load_object_by_full_key(txn, idx)
                    # t = storage.load_object_by_full_key(txn, resolved_idx)

                    # print(f"{f.id} {f.__class__} -> {t.id} {t.__class__}")

                    db_reference_forward.put(txn, idx, resolved_idx)
                    unresolved_cursor.delete(MDBXCursorOp.MDBX_PREV)

                # else:
                #    print("unresolved", value, idx)

            # In this situation the original reference was incomplete
            if len(references_to_fix) > 0:
                referencing_class_idx, referencing_key = storage.serializer.full_key_to_idx(idx)
                referencing_class = storage.idx_class[referencing_class_idx]
                referencing_obj: Tid = storage.load_object(txn, referencing_class, referencing_key)

                for resolved_idx, value, version_change, class_change in references_to_fix:
                    referenced_class_idx, referenced_key = storage.serializer.full_key_to_idx(resolved_idx)

                    for reference in only_reference_objects(referencing_obj):
                        if isinstance(reference.name_of_ref_class, str):
                            # TODO: I think we want to write to the console that a NameOfRefClass has been specified that does not match the natural scope of the Reference.
                            # print(reference.name_of_ref_class, reference)
                            if reference.name_of_ref_class not in storage.serializer.name_object:
                                reference.name_of_ref_class = 'DataManagedObject'
                            name_of_ref_class = reference.name_of_ref_class

                        elif reference.name_of_ref_class.value not in storage.serializer.name_object:
                            # TODO: Add a warning.
                            reference.name_of_ref_class = 'DataManagedObject'

                        else:
                            name_of_ref_class = reference.name_of_ref_class.value

                        cmp_value = storage.serializer.encode_key(
                            reference.ref, getattr(reference, "version", "any"),
                            storage.serializer.name_object[name_of_ref_class], True
                        )
                        if value == cmp_value:
                            if class_change:
                                referenced_class = storage.idx_class[referenced_class_idx]
                                reference.name_of_ref_class = NameOfClass(
                                    get_object_name(referenced_class)
                                )  # I am very afraid how this might be handled in terms of comparisons later.
                            if version_change:
                                referenced_clazz = storage.idx_class[referenced_class_idx]
                                referenced_obj: Tid = storage.load_object(txn, referenced_clazz, referenced_key)
                                reference.version = referenced_obj.version

                # TODO: buffer this write to ~10000 objects of the same type?
                db = txn.open_map(referencing_class_idx, flags=MDBXDBFlags.MDBX_DB_DEFAULTS)
                db.put(txn, referencing_key, storage.serializer.marshall(referencing_obj, referencing_obj.__class__))

        log_all(logging.INFO, f"[unresolved references] {db_unresolved.get_stat(txn).ms_entries}")

        db_id_idx.drop(txn, delete=True)
        txn.commit()
