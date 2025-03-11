from typing import Iterable, Dict, Generator, Set

from netexio.database import Database
from netexio.dbaccess import write_objects, load_generator, update_generator, load_local
from netex import ServiceJourneyPattern, Direction, MultilingualString, ResponsibilitySet, ResponsibilityRoleAssignment, \
    StakeholderRoleTypeEnumeration, Line, Operator, OperatorRef, ServiceJourney, TemplateServiceJourney, LineRef
from utils.refs import getId, getRef, getIndex

from utils.utils import project

import utils.netex_monkeypatching


def infer_operator_from_responsibilityset_and_apply(db_read: Database, db_write: Database, generator_defaults: dict):
    line_ref_to_operator_ref: Dict[str, OperatorRef] = {}
    mapping: Dict[str, OperatorRef] = {}

    def process_line(object: Line|ServiceJourney|TemplateServiceJourney):
        if object.operator_ref is None:
            if object.responsibility_set_ref_attribute is not None and object.responsibility_set_ref_attribute in mapping:
                object.operator_ref = mapping[object.responsibility_set_ref_attribute]
                return object
            elif object.id in line_ref_to_operator_ref:
                object.operator_ref = line_ref_to_operator_ref[object.id]
                return object

    def process_journey(object: ServiceJourney|TemplateServiceJourney):
        changed = False
        if object.operator_ref_or_operator_view is None and object.responsibility_set_ref_attribute is not None and object.responsibility_set_ref_attribute in mapping:
            object.operator_ref_or_operator_view = mapping[object.responsibility_set_ref_attribute]
            changed = True

        if object.operator_ref_or_operator_view is not None and object.flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view is not None and hasattr(object.flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view, 'ref'):
            line_ref_to_operator_ref[object.flexible_line_ref_or_line_ref_or_line_view_or_flexible_line_view.ref] = mapping[object.responsibility_set_ref_attribute]

        if changed:
            return object

    def query1(db_read: Database) -> Generator:
        _load_generator = load_generator(db_read, Line)
        for line in _load_generator:
            new_line = process_line(line)
            if new_line is not None:
                yield new_line

    def query2(db_read: Database) -> Generator:
        _load_generator = load_generator(db_read, ServiceJourney)
        for service_journey in _load_generator:
            new_service_journey = process_journey(service_journey)
            if new_service_journey is not None:
                yield new_service_journey

    def query3(db_read: Database) -> Generator:
        _load_generator = load_generator(db_read, TemplateServiceJourney)
        for template_service_journey in _load_generator:
            new_template_service_journey = process_journey(template_service_journey)
            if new_template_service_journey is not None:
                yield new_template_service_journey


    _mapping: Dict[str, Set] = {}
    # operators = getIndex(load_local(db_read, Operator))
    for responsibility_set in load_local(db_read, ResponsibilitySet):
        _mapping[responsibility_set.id] = set([])
        for role_assignment in responsibility_set.roles.responsibility_role_assignment:
            if StakeholderRoleTypeEnumeration.OPERATION in role_assignment.stakeholder_role_type or StakeholderRoleTypeEnumeration.OPERATION_1 in role_assignment.stakeholder_role_type:
                _mapping[responsibility_set.id].add(project(role_assignment.responsible_organisation_ref, OperatorRef))

    mapping = {x: y.pop() for x, y in _mapping.items() if len(y) == 1}
    # Maybe Authority too?

    update_generator(db_write, ServiceJourney, query2(db_read))
    update_generator(db_write, TemplateServiceJourney, query3(db_read))
    update_generator(db_write, Line, query1(db_read))

