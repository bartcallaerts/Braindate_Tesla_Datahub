from pyDataHub import Satellite
from pyspark.sql.types import StructField, StringType

#Version: DM-0012 ODS_Supplier_Extract V1.0
class ODS_Supplier_Details_S(Satellite):

    def getBusinessFieldsSchema(self):
        return [
            StructField("SupplierId", StringType(), False),
            StructField("CompanyName", StringType(), False),
            StructField("VATNumber", StringType(), False),
            StructField("Street", StringType(), False),
            StructField("ZipCode", StringType(), False),
            StructField("City", StringType(), False),
            StructField("CountryCode", StringType(), False),
            StructField("IBAN", StringType(), False),
            StructField("BIC", StringType(), False)
        ]