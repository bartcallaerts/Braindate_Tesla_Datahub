from pyDataHub import DataVault
from pyDataHubImp.entities.hubs import ODS_Supplier_H
from pyDataHubImp.entities.links import Organization_ODSSupplier_OL


class BrainDateDataVault(DataVault):

    def __init__(self, path, dbutils, name, initWithCreation):
        super().__init__(path, dbutils, name, initWithCreation)

    def registerHubs(self, initWithCreation):
        self.hubs["ODS_Supplier_H"] = ODS_Supplier_H("ODS_Supplier_H", self, initWithCreation)

    def registerLinks(self, initWithCreation):
        self.links["Organization_ODSSupplier_OL"] = Organization_ODSSupplier_OL("Organization_ODSSupplier_OL", self, initWithCreation)