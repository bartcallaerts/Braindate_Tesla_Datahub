# import datetime
# import time
# from pyspark.sql import SparkSession
# from pyDataHub import LoadProcessorBase, ProcessItem
# from pyspark.sql.functions import col, lit, sha1, concat
# from pyspark.sql import functions


# spark = SparkSession \
#     .builder \
#     .master("local") \
#     .appName("pyDataVault") \
#     .getOrCreate()


# class EXTRACT_CDB2_GroupPerson_Full(LoadProcessorBase):

#     def buildProcessItems(self):
#         self.processItems.append(ProcessItem(
#             "NaturalPerson_H", self._transformStagingDataForNaturalPerson_H, self.processHub, self.dataHub.dataVault.hubs["NaturalPerson_H"]))
#         self.processItems.append(ProcessItem(
#             "NaturalPerson_CDB2_GroupPerson_S", self._transformStagingDataForNaturalPerson_CDB2_GroupPerson_S, self.processSatellite, self.dataHub.dataVault.hubs["NaturalPerson_H"].satellites["NaturalPerson_CDB2_GroupPerson_S"]))
#         self.processItems.append(ProcessItem(
#             "Employment_L", self._transformStagingDataForEmployment_L, self.processLink, self.dataHub.dataVault.links["Employment_L"]))
#         self.processItems.append(ProcessItem(
#             "CDB2Organization_H", self._transformStagingDataForCDB2Organization_H, self.processHub, self.dataHub.dataVault.hubs["CDB2Organization_H"]))

#     def _getStagingData(self, batchName):
#         return spark.read.option("multiline", "true").json(self.dataHub.path + "/staging/extract/cdb2/groupperson/full/" + batchName + "/groupPersons.json")

#     def _transformStagingDataForNaturalPerson_H(self, stagingDataFrame, entity, loadDate, batchName):
#         return entity.buildDataFrame().union(stagingDataFrame.select(sha1(col("SSN")).alias("HashKey"), 
#                                                                     lit(loadDate).alias("LoadDate"),   
#                                                                     lit(self.dataHub.path + "/staging/extract/cdb2/groupperson/full/" + batchName + "/groupPersons.json").alias("RecordSource"),
#                                                                     col("SSN").alias("NationalRegisterNumber")))

#     def _transformStagingDataForEmployment_L(self, stagingDataFrame, entity, loadDate, batchName):
#         return entity.buildDataFrame().union(stagingDataFrame.select(sha1(concat(col("SSN"), col("OrganizationId").cast("string"))).alias("HashKey"), 
#                                                                     lit(loadDate).alias("LoadDate"), 
#                                                                     lit(self.dataHub.path + "/staging/extract/cdb2/groupperson/full/" + batchName + "/groupPersons.json").alias("RecordSource"),
#                                                                     col("SSN").alias("NationalRegisterNumber"),
#                                                                     col("OrganizationId").alias("OrganizationId")))

#     def _transformStagingDataForNaturalPerson_CDB2_GroupPerson_S(self, stagingDataFrame, entity, loadDate, batchName):
#         return entity.buildDataFrame().union(stagingDataFrame.select(sha1(col("SSN")).alias("HashKey"),
#                                                                      lit(loadDate).alias("LoadDate"),
#                                                                      lit(self.dataHub.path + "/staging/extract/cdb2/groupperson/full/" + batchName + "/groupPersons.json").alias("RecordSource"),
#                                                                      lit(None).alias("EndDate"),
#                                                                      sha1(concat(col("SSN"), 
#                                                                         functions.when(col("id").isNull(), "").otherwise(col("id").cast("string")), 
#                                                                         functions.when(col("FamilyName").isNull(), "").otherwise(col("FamilyName").cast("string")), 
#                                                                         functions.when(col("FirstName").isNull(), "").otherwise(col("FirstName").cast("string")), 
#                                                                         functions.when(col("MailCorrespondance").isNull(), "").otherwise(col("MailCorrespondance").cast("string")),
#                                                                         functions.when(col("MotherTongue").isNull(), "").otherwise(col("MotherTongue").cast("string")), 
#                                                                         functions.when(col("PersonRemarks").isNull(), "").otherwise(col("PersonRemarks").cast("string")), 
#                                                                         functions.when(col("SexId").isNull(), "").otherwise(col("SexId").cast("string")), 
#                                                                         functions.when(col("AccountingKey").isNull(), "").otherwise(col("AccountingKey").cast("string")),
#                                                                         functions.when(col("Birthday").isNull(), "").otherwise(col("Birthday").cast("string")), 
#                                                                         functions.when(col("EducationLevelId").isNull(), "").otherwise(col("EducationLevelId").cast("string")), 
#                                                                         functions.when(col("Foreigner").isNull(), "").otherwise(col("Foreigner").cast("string")), 
#                                                                         functions.when(col("Function").isNull(), "").otherwise(col("Function").cast("string")), 
#                                                                         functions.when(col("MyCevora").isNull(), "").otherwise(col("MyCevora").cast("string")), 
#                                                                         functions.when(col("IBAN").isNull(), "").otherwise(col("IBAN").cast("string")), 
#                                                                         functions.when(col("PC").isNull(), "").otherwise(col("PC").cast("string")), 
#                                                                         functions.when(col("Risk").isNull(), "").otherwise(col("Risk").cast("string")), 
#                                                                         functions.when(col("WorkStateId").isNull(), "").otherwise(col("WorkStateId").cast("string")), 
#                                                                         functions.when(col("CellPhone").isNull(), "").otherwise(col("CellPhone").cast("string")), 
#                                                                         functions.when(col("Telephone").isNull(), "").otherwise(col("Telephone").cast("string")), 
#                                                                         functions.when(col("Email").isNull(), "").otherwise(col("Email").cast("string")), 
#                                                                         functions.when(col("Fax").isNull(), "").otherwise(col("Fax").cast("string")), 
#                                                                         functions.when(col("Website").isNull(), "").otherwise(col("Website").cast("string"))))
#                                                                         .alias("HashDiff"),
#                                                                      col("Id").cast("string").alias("GroupPersonId"), 
#                                                                      col("FamilyName"), 
#                                                                      col("FirstName"), 
#                                                                      col("MailCorrespondance").cast("boolean"), 
#                                                                      col("MotherTongue").cast("string").alias("MotherTongueCodeValue"), 
#                                                                      col("PersonRemarks"), 
#                                                                      col("SexId").cast("string").alias("SexCodeValue"), 
#                                                                      col("AccountingKey"), 
#                                                                      col("Birthday").alias("DateOfBirth"), 
#                                                                      col("EducationLevelId").cast("string").alias("EducationLevelCodeValue"), 
#                                                                      col("Foreigner").cast("boolean"), 
#                                                                      col("Function"), 
#                                                                      col("MyCevora").cast("boolean"), 
#                                                                      col("Iban"), 
#                                                                      col("PC"), 
#                                                                      col("Risk").cast("boolean"), 
#                                                                      col("WorkStateId").cast("string").alias("WorkStateCodeValue"), 
#                                                                      col("CellPhone"), 
#                                                                      col("Telephone"), 
#                                                                      col("Email"), 
#                                                                      col("Fax"), 
#                                                                      col("Website")
#                                                                      )) 

#     def _transformStagingDataForCDB2Organization_H(self, stagingDataFrame, entity, loadDate, batchName):
#         return entity.buildDataFrame().union(stagingDataFrame.filter("OrganizationId is not null").select(sha1(col("OrganizationId").cast("string")).alias("HashKey"),
#                                                                      lit(loadDate).alias("LoadDate"),
#                                                                      lit(self.dataHub.path + "/staging/extract/cdb2/groupperson/full/" + batchName + "/groupPersons.json").alias("RecordSource"),
#                                                                      col("OrganizationId")
#                                                                      ))