from pyDataHub import Hub
from pyspark.sql.types import StructField, StringType
from pyDataHubImp.entities.satellites import ODS_Supplier_Details_S

#Version: DM-0012 ODS_Supplier_Extract V1.0
class ODS_Supplier_H(Hub):

    def __init__(self, name, dataVault, initWithCreation):
        super().__init__(name, dataVault, initWithCreation)

        self._registerSatellites(initWithCreation)

    def getBusinessFieldsSchema(self):
        return [
            StructField("SupplierId", StringType(), False),
        ]

    def _registerSatellites(self, initWithCreation):
        self.satellites["ODS_Supplier_Details_S"] = ODS_Supplier_Details_S("ODS_Supplier_Details_S", self, self.dataVault, initWithCreation)
