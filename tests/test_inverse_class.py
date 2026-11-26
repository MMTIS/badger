from unittest import TestCase

from domain.netex import ScheduledStopPoint, FareScheduledStopPoint, Authority, GeneralOrganisation, ManagementAgent, OnlineServiceOperator, Operator, OtherOrganisation, ServicedOrganisation, RetailConsortium, TravelAgent, Organisation, TransportOrganisation, TransportOrganisationDummy, Point2
from domain.netex.indexes.inverse_class import classes_and_subclasses

import sys

class Test(TestCase):
    def test_classes_and_subclasses(self):
        classes_and_subclasses((Point2,))

        self.assertCountEqual(classes_and_subclasses((ScheduledStopPoint,)),
                              (FareScheduledStopPoint, ScheduledStopPoint,))

        self.assertCountEqual(classes_and_subclasses((Organisation,)),
                         (Authority, GeneralOrganisation, ManagementAgent, OnlineServiceOperator, Operator,
                          OtherOrganisation, ServicedOrganisation, RetailConsortium, TravelAgent, Organisation,
                          TransportOrganisation, TransportOrganisationDummy,))