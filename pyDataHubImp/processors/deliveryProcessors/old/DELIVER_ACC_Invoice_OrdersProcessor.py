# import datetime
# import time
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, lit, sha1, concat
# from pyDataHub import LoadProcessorBase, ProcessItem, DeliveryProcessorBase


# spark = SparkSession \
#     .builder \
#     .master("local") \
#     .appName("pyDataVault") \
#     .getOrCreate()



# class DELIVER_ACC_Invoice_OrdersProcessor(DeliveryProcessorBase):

#     def process(self, batchName):
        
#         self.dataHub.dataVault.hubs["NATURALPERSON_H"].satellites["NATURALPERSON_CDB2_RO_S"].dataFrame.write.parquet(self.dataHub.path + "/delivery/DELIVER_ACC_Invoice_Orders/" + batchName + "/ACC_Invoice")