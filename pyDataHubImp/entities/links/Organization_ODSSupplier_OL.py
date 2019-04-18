from pyDataHub import Link
from pyspark.sql.types import StructField, StringType, IntegerType

#Version: DM-0023-v1 DataHub_Organization_OV
class Organization_ODSSupplier_OL(Link):

    def __init__(self, name, dataVault, initWithCreation):
        super().__init__(name, dataVault, initWithCreation)

    def getBusinessFieldsSchema(self):
        return [
            StructField("OrganizationId", IntegerType(), False),
            StructField("SourceId", StringType(), False),
        ]