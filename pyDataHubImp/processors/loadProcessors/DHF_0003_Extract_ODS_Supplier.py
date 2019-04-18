import datetime
from pyspark.sql import SparkSession
from pyDataHub import LoadProcessorBase, ProcessItem
from pyspark.sql.functions import col, lit, sha1, concat, udf, array
from pyspark.sql import functions
from pyspark.sql.types import TimestampType, IntegerType
from pyDataHubFunctions.hashing import generateHash


spark = SparkSession \
    .builder \
    .master("local") \
    .appName("pyDataVault") \
    .getOrCreate()


#Business rules:
#BR-0010-v1 Creation of Organization OV

class DHF_0003_Extract_ODS_Supplier(LoadProcessorBase):
    
    def buildProcessItems(self):
        self.processItems.append(ProcessItem(
            "ODS_Supplier_H", self._transformStagingDataForODS_Supplier_H, self.processHub, self.dataHub.dataVault.hubs["ODS_Supplier_H"]))
        self.processItems.append(ProcessItem(
            "ODS_Supplier_Details_S", self._transformStagingDataForODS_Supplier_Detail_S, self.processSatellite, self.dataHub.dataVault.hubs["ODS_Supplier_H"].satellites["ODS_Supplier_Details_S"]))
        self.processItems.append(ProcessItem(
            "Organization_ODSSupplier_OL", self._transformStagingDataForOrganization_ODSSupplier_OL, self.processLink, self.dataHub.dataVault.links["Organization_ODSSupplier_OL"]))
        
   
    def _getStagingData(self, batchName):
        return spark.read.format("csv").option("header", "true").option("delimiter", ";").load(self.dataHub.path + "/staging/ods/popsysuppliers/" + batchName + "/popsysuppliers.csv")

    def _transformStagingDataForOrganization_ODSSupplier_OL(self, stagingDataFrame, entity, loadDate, batchName):
        spark.sql("use " + self.dataHub.dataVault.name)
        maxOrganizationKey = spark.sql("SELECT coalesce(MAX(OrganizationId), 0) AS OrganizationId from Organization_OH").first()[0]
        
        existingLinks = entity.dataVault.links["Organization_ODSSupplier_OL"].dataFrame
        joinExpression = (existingLinks["SourceId"] == stagingDataFrame["SupplierId"])

        rdd = stagingDataFrame.join(existingLinks, joinExpression, "left_anti").select(col("SupplierId")).rdd

        if rdd.count() > 0:
            df = rdd.zipWithIndex().toDF(["id", "organizationId"]).select(col("id.SupplierId").alias("Id"), (col("organizationId") + maxOrganizationKey + 1).alias("OrganizationId"))

            return entity.buildDataFrame().union(df.select(generateHash(array(col("Id"))).alias("HashKey"),
                                                                    lit(loadDate).cast(TimestampType()).alias("LoadDate"), 
                                                                    lit("BR-0010-V1").alias("RecordSource"),
                                                                    col("OrganizationId").cast(IntegerType()),
                                                                    col("Id").alias("SourceId")))
        else:
            return entity.buildDataFrame()                                       

    def _transformStagingDataForODS_Supplier_H(self, stagingDataFrame, entity, loadDate, batchName):
        return entity.buildDataFrame().union(stagingDataFrame.select(generateHash(array(col("SupplierId"))).alias("HashKey"),
                                                                    lit(loadDate).cast(TimestampType()).alias("LoadDate"), 
                                                                    lit(self.dataHub.path + "/staging/ods/popsysuppliers/" + batchName + "/popsysuppliers.csv").alias("RecordSource"),
                                                                    col("SupplierId")))

    def _transformStagingDataForODS_Supplier_Detail_S(self, stagingDataFrame, entity, loadDate, batchName):
        return entity.buildDataFrame().union(stagingDataFrame.select(generateHash(array(col("SupplierId"))).alias("HashKey"),
                                                                     lit(loadDate).cast(TimestampType()).alias("LoadDate"),
                                                                     lit(self.dataHub.path + "/staging/ods/popsysuppliers/" + batchName + "/popsysuppliers.csv").alias("RecordSource"),
                                                                     lit(None).alias("EndDate"),
                                                                     generateHash(array(col("SupplierId"),
                                                                        col("Company"), 
                                                                        col("VATNum"), 
                                                                        col("Street"), 
                                                                        col("ZipCodeID"),
                                                                        col("City"), 
                                                                        col("CountryID"), 
                                                                        col("IBAN1"), 
                                                                        col("BIC1")))
                                                                        .alias("HashDiff"),
                                                                     col("SupplierId").cast("string"), 
                                                                     col("Company").alias("CompanyName"), 
                                                                     col("VATNum").alias("VATNumber"), 
                                                                     col("Street"), 
                                                                     col("ZipCodeID").cast("string").alias("ZipCode"), 
                                                                     col("City"), 
                                                                     col("CountryID").cast("string").alias("CountryCode"), 
                                                                     col("IBAN1").alias("IBAN"), 
                                                                     col("BIC1").alias("BIC")
                                                                     ))