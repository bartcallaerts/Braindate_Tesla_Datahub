import datetime
import time
from pyspark.sql import SparkSession
from pyDataHub import BusinessProcessorBase, ProcessItem
from pyspark.sql.functions import col, lit, sha1, concat, udf, array
from pyspark.sql import functions
from pyspark.sql.types import TimestampType, IntegerType
from pyDataHubImp.entities.satellites import Organization_Accounting_Key_OS
from pyDataHubFunctions.hashing import generateHash


spark = SparkSession \
    .builder \
    .master("local") \
    .appName("pyDataVault") \
    .getOrCreate()

#Business rules:
#BR-0006-v1 Organization BV enrichment with AccountKey

class BR_0006_Organization_BV_Enrichment_with_accountKey(BusinessProcessorBase):
    
    def process(self):
        loadDate = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')

        spark.sql("use " + self.dataHub.dataVault.name)
        maxAccountingKey = spark.sql("SELECT coalesce(MAX(AccountingKey), 1000000) as AccountingKey from Organization_Accounting_Key_OS").first()[0]

        dfOrganizationIdsReadyForAccountingKey = spark.sql("""SELECT OrganizationId from Organization_BS where OrganizationId not in (select OrganizationId from organization_accounting_key_os) and IsValidForAccounting = 1""")

        rdd = dfOrganizationIdsReadyForAccountingKey.rdd

        if rdd.count() > 0:
            organizationsWithKeysTuples = rdd.zipWithIndex().toDF(["id", "AccountingKey"]).select(col("id.OrganizationId").alias("OrganizationId"), (col("AccountingKey") + maxAccountingKey + 1).alias("AccountingKey"))

            df = self.dataHub.dataVault.hubs["Organization_OH"].satellites["Organization_Accounting_Key_OS"].buildDataFrame().union(organizationsWithKeysTuples.select(generateHash(array(col("OrganizationId"))).alias("HashKey"), 
                                                                    lit(loadDate).cast(TimestampType()).alias("LoadDate"), 
                                                                    lit("BR-0006-v1").alias("RecordSource"),
                                                                    lit(None).alias("EndDate"),
                                                                    generateHash(array(col("OrganizationId"), col("AccountingKey"))).alias("HashDiff"),
                                                                    col("OrganizationId").cast(IntegerType()),
                                                                    col("AccountingKey").cast(IntegerType())))

            df.write.save(self.dataHub.dataVault.hubs["Organization_OH"].satellites["Organization_Accounting_Key_OS"].storagePath, format="delta", mode="append")    