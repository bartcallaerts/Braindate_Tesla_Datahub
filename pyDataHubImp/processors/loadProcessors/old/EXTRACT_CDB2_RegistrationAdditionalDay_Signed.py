# import datetime
# import time
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, lit, sha1, concat
# from pyDataHub import LoadProcessorBase, ProcessItem
# from pyspark.sql import functions

# spark = SparkSession \
#     .builder \
#     .master("local") \
#     .appName("pyDataVault") \
#     .getOrCreate()


# class EXTRACT_CDB2_RegistrationAdditionalDay_Signed(LoadProcessorBase):

#     def buildProcessItems(self):
#         self.processItems.append(ProcessItem(
#             "NaturalPerson_H", self._transformStagingDataForNaturalPerson_H, self.processHub, self.dataHub.dataVault.hubs["NaturalPerson_H"]))
#         self.processItems.append(ProcessItem(
#             "Group_H", self._transformStagingDataForGroup_H, self.processHub, self.dataHub.dataVault.hubs["Group_H"]))
#         self.processItems.append(ProcessItem(
#             "Course_H", self._transformStagingDataForCourse_H, self.processHub, self.dataHub.dataVault.hubs["Course_H"]))
#         self.processItems.append(ProcessItem(
#             "Project_H", self._transformStagingDataForProject_H, self.processHub, self.dataHub.dataVault.hubs["Project_H"]))
#         self.processItems.append(ProcessItem(
#             "AllowanceRegistrationAdditionalDay_L", self._transformStagingDataForAllowanceRegistrationAdditionalDay_L, self.processLink, self.dataHub.dataVault.links["AllowanceRegistrationAdditionalDay_L"]))
#         self.processItems.append(ProcessItem(
#             "GroupCourse_L", self._transformStagingDataForGroupCourse_L, self.processLink, self.dataHub.dataVault.links["GroupCourse_L"]))
#         self.processItems.append(ProcessItem(
#             "GroupProject_L", self._transformStagingDataForGroupProject_L, self.processLink, self.dataHub.dataVault.links["GroupProject_L"]))
#         self.processItems.append(ProcessItem(
#             "AllowanceRegistrationAdditionalDay_CDB2_S", self._transformStagingDataForCAllowanceRegistrationAdditionalDay_CDB2_S, self.processSatellite, self.dataHub.dataVault.links["AllowanceRegistrationAdditionalDay_L"].satellites["AllowanceRegistrationAdditionalDay_CDB2_S"]))


#     def _getStagingData(self, batchName):
#         return spark.read.option("multiline", "true").json(self.dataHub.path + "/staging/extract/cdb2/registrationadditionalday/signed/" + batchName + "/registrationadditionalday.json")

#     def _transformStagingDataForNaturalPerson_H(self, stagingDataFrame, entity, loadDate, batchName):
#         return entity.buildDataFrame().union(stagingDataFrame.select(sha1(col("SSN")).alias("HashKey"), 
#                                                                     lit(loadDate).alias("LoadDate"),   
#                                                                     lit(self.dataHub.path + "/staging/extract/cdb2/registrationadditionalday/signed/" + batchName + "/registrationadditionalday.json").alias("RECORDSOURCE"),
#                                                                     col("SSN").alias("NationalRegisterNumber")))

#     def _transformStagingDataForAllowanceRegistrationAdditionalDay_L(self, stagingDataFrame, entity, loadDate, batchName):
#         return entity.buildDataFrame().union(stagingDataFrame.filter("GroupNumber is not null").select(sha1(concat(col("SSN"), col("groupNumber").cast("string"))).alias("HashKey"),
#                                                                      lit(loadDate).alias("LoadDate"),
#                                                                      lit(self.dataHub.path + "/staging/extract/cdb2/registrationadditionalday/signed/" + batchName + "/registrationadditionalday.json").alias("RecordSource"),
#                                                                      col("SSN").alias("NationalRegisterNumber"),
#                                                                      col("groupNumber")
#                                                                      ))

#     def _transformStagingDataForGroupCourse_L(self, stagingDataFrame, entity, loadDate, batchName):
#         return entity.buildDataFrame().union(stagingDataFrame.filter("GroupNumber is not null").select(sha1(concat(col("groupNumber").cast("string"), col("courseNumber"))).alias("HashKey"),
#                                                                      lit(loadDate).alias("LoadDate"),
#                                                                      lit(self.dataHub.path + "/staging/extract/cdb2/registrationadditionalday/signed/" + batchName + "/registrationadditionalday.json").alias("RecordSource"),
#                                                                      col("groupNumber"),
#                                                                      col("courseNumber")
#                                                                      ))
#     def _transformStagingDataForGroupProject_L(self, stagingDataFrame, entity, loadDate, batchName):
#         return entity.buildDataFrame().union(stagingDataFrame.filter("GroupNumber is not null").select(sha1(concat(col("groupNumber").cast("string"), col("courseNumber").cast("string"))).alias("HashKey"),
#                                                                      lit(loadDate).alias("LoadDate"),
#                                                                      lit(self.dataHub.path + "/staging/extract/cdb2/registrationadditionalday/signed/" + batchName + "/registrationadditionalday.json").alias("RecordSource"),
#                                                                      col("groupNumber"),
#                                                                      col("ProjectNumber")
#                                                                      ))

#     def _transformStagingDataForCAllowanceRegistrationAdditionalDay_CDB2_S(self, stagingDataFrame, entity, loadDate, batchName):
#         endDate = None
#         return entity.buildDataFrame().union(stagingDataFrame.filter("GroupNumber is not null").select(sha1(concat(col("SSN"), col("groupNumber").cast("string"))).alias("HashKey"),
#                                                                      lit(loadDate).alias("LoadDate"),
#                                                                      lit(self.dataHub.path + "/staging/extract/cdb2/organization/full/" + batchName + "/organization.json").alias("RecordSource"),
#                                                                      lit(endDate).alias("EndDate"),
#                                                                      sha1(concat(col("iban"), functions.when(col("bic").isNull(), "").otherwise(col("bic")), col("totalAmount"), col ("state"))).alias("HashDiff"),
#                                                                      col("iban").alias("IBAN"),
#                                                                      col("bic").alias("BIC"),
#                                                                      col("totalAmount"),
#                                                                      col("state")
#                                                                      ))
#     def _transformStagingDataForGroup_H(self, stagingDataFrame, entity, loadDate, batchName):
#         return entity.buildDataFrame().union(stagingDataFrame.select(sha1(col("groupNumber").cast("string")).alias("HashKey"), 
#                                                                     lit(loadDate).alias("LoadDate"),   
#                                                                     lit(self.dataHub.path + "/staging/extract/cdb2/registrationadditionalday/signed/" + batchName + "/registrationadditionalday.json").alias("RecordSource"),
#                                                                     col("groupNumber")))

#     def _transformStagingDataForCourse_H(self, stagingDataFrame, entity, loadDate, batchName):
#         return entity.buildDataFrame().union(stagingDataFrame.select(sha1(col("courseNumber")).alias("HashKey"), 
#                                                                     lit(loadDate).alias("LoadDate"),   
#                                                                     lit(self.dataHub.path + "/staging/extract/cdb2/registrationadditionalday/signed/" + batchName + "/registrationadditionalday.json").alias("RecordSource"),
#                                                                     col("courseNumber")))

#     def _transformStagingDataForProject_H(self, stagingDataFrame, entity, loadDate, batchName):
#         return entity.buildDataFrame().union(stagingDataFrame.select(sha1(col("projectNumber").cast("string")).alias("HashKey"), 
#                                                                     lit(loadDate).alias("LoadDate"),   
#                                                                     lit(self.dataHub.path + "/staging/extract/cdb2/registrationadditionalday/signed/" + batchName + "/registrationadditionalday.json").alias("RecordSource"),
#                                                                     col("projectNumber")))
