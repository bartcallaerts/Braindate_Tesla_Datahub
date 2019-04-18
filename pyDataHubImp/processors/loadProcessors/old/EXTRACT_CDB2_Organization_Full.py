# import datetime
# import time
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, lit, sha1, concat
# from pyDataHub import LoadProcessorBase, ProcessItem


# spark = SparkSession \
#     .builder \
#     .master("local") \
#     .appName("pyDataVault") \
#     .getOrCreate()


# class EXTRACT_CDB2_Organization_Full(LoadProcessorBase):

#     def buildProcessItems(self):
#         self.processItems.append(ProcessItem(
#             "CDB2Organization_H", self._transformStagingDataForCDB2Organization_H, self.processHub, self.dataHub.dataVault.hubs["CDB2Organization_H"]))
#         self.processItems.append(ProcessItem(
#             "CDB2Organization_S", self._transformStagingDataForCDB2Organization_S, self.processSatellite, self.dataHub.dataVault.hubs["CDB2Organization_H"].satellites["CDB2Organization_S"]))

#     def _getStagingData(self, batchName):
#         return spark.read.option("multiline", "true").json(self.dataHub.path + "/staging/extract/cdb2/organization/full/" + batchName + "/organization.json")

#     def _transformStagingDataForCDB2Organization_H(self, stagingDataFrame, entity, loadDate, batchName):
#         return entity.buildDataFrame().union(stagingDataFrame.select(sha1(col("OrganizationId").cast("string")).alias("HashKey"), 
#                                                                     lit(loadDate).alias("LoadDate"), 
#                                                                     lit(self.dataHub.path + "/staging/extract/cdb2/organization/full/" + batchName + "/organization.json").alias("RecordSource"),
#                                                                     col("OrganizationId")))

#     def _transformStagingDataForCDB2Organization_S(self, stagingDataFrame, entity, loadDate, batchName):
#         endDate = None
#         return entity.buildDataFrame().union(stagingDataFrame.select(sha1(col("OrganizationId").cast("string")).alias("HashKey"),
#                                                                      lit(loadDate).alias("LoadDate"),
#                                                                      lit(self.dataHub.path + "/staging/extract/cdb2/organization/full/" + batchName + "/organization.json").alias("RecordSource"),
#                                                                      lit(endDate).alias("EndDate"),
#                                                                      sha1(concat(col("OrganizationId").cast("string"), col("OrganizationName"))).alias("HashDiff"),
#                                                                      col("OrganizationId"),
#                                                                      col("OrganizationName")
#                                                                      ))