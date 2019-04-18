import datetime
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sha1, concat, when
from pyDataHub import LoadProcessorBase, ProcessItem, DeliveryProcessorBase
from pyspark.sql.types import IntegerType, BooleanType, StringType, StructField, StructType


spark = SparkSession \
    .builder \
    .master("local") \
    .appName("pyDataVault") \
    .getOrCreate()


#Business rules:
#BR-0002-v1 Delivery mapping to EASI for Accounts

class DHF_0004_Deliver_EASI_Account(DeliveryProcessorBase):

    def process(self, batchName):

        batchEndDateString = datetime.datetime.strptime(batchName, '%Y%m%d%H%M%S%f').strftime('%Y-%m-%d %H:%M:%S.%f')
        pipelineName = 'DP-0003-v1-DELIVER_EASI_Account_Upsert'

        self._fieldList = [
            StructField("COMNUMCP", IntegerType(), False),
            StructField("COMCSLCP", IntegerType(), False),
            StructField("COMALPCP", IntegerType(), False),
            StructField("COMPR1CP", StringType(), False),
            StructField("COMPR2CP", StringType(), False),
            StructField("PRFCODCP", StringType(), False),
            StructField("LANCODCP", StringType(), False),
            StructField("COMNA1CP", StringType(), False),
            StructField("ADDRE1CP", StringType(), False),
            StructField("ADDRE2CP", StringType(), False),
            StructField("VATNBRCP", StringType(), False),
            StructField("VATCOUCP", StringType(), False),
            StructField("VATCODCP", StringType(), False),
            StructField("PHONBRCP", StringType(), False),
            StructField("FAXNBRCP", StringType(), False),
            StructField("EMLADRCP", StringType(), False),
            StructField("BANACCBK", StringType(), False),
            StructField("BICCODBK", StringType(), False),
            StructField("COUCODCI", StringType(), False),
            StructField("CITDSC1CI", StringType(), False),
            StructField("POSCODCI", StringType(), False),
            StructField("CURCODBK", StringType(), False),
            StructField("ITFFINCP", StringType(), False)
        ]
        self._schema = StructType(self._fieldList)
        dfschema = spark.createDataFrame([], self._schema)


        df = dfschema.union(self.dataHub.dataVault.hubs["Organization_OH"].satellites["Organization_BS"].dataFrame.filter("""IsValidForAccounting  = true 
                and
                (
                    (select PipelineStartTimestamp 
                        from pipeline_mh h
                        inner join pipeline_rundetails_ms s on h.hashKey = s.hashkey
                        INNER JOIN
                        (SELECT HashKey, MAX(LoadDate) AS MaxLoadDate
                        FROM pipeline_rundetails_ms
                        GROUP BY HashKey) groupedtt 
                        ON s.HashKey = groupedtt.HashKey 
                        where PipelineName = '""" + pipelineName +
                        """' and s.loaddate = groupedtt.MaxLoadDate) is null
                    or
                    loaddate > (select PipelineStartTimestamp 
                        from pipeline_mh h
                        inner join pipeline_rundetails_ms s on h.hashKey = s.hashkey
                        INNER JOIN
                        (SELECT HashKey, MAX(LoadDate) AS MaxLoadDate
                        FROM pipeline_rundetails_ms
                        GROUP BY HashKey) groupedtt 
                        ON s.HashKey = groupedtt.HashKey 
                        where PipelineName = '""" + pipelineName +
                        """' and s.loaddate = groupedtt.MaxLoadDate)
                    
                )
                and loaddate <= '""" + batchEndDateString + "'").select(
                col("AccountKey").alias("COMNUMCP"),
                col("AccountKey").alias("COMCSLCP"),
                col("AccountKey").alias("COMALPCP"),
                when(col("IsClient"), "A COMPLETER CUS").otherwise(lit(None)).alias("COMPR1CP"),
                when(col("IsSupplier"), "A COMPLETER SUP").otherwise(lit(None)).alias("COMPR2CP"),
                when(col("IsSupplier"), "2").otherwise(lit("1")).alias("PRFCODCP"),
                col("LanguageCode").alias("LANCODCP"),
                col("Name").alias("COMNA1CP"),
                concat(col("BillingStreet"), lit(" "), col("BillingCity"), lit(" "),  col("BillingPostalCode")).alias("ADDRE1CP"),
                lit(None).alias("ADDRE2CP"),
                col("VATNumber").alias("VATNBRCP"),
                col("BillingCountryCode").alias("VATCOUCP"),
                lit("1").alias("VATCODCP"),
                lit(None).alias("PHONBRCP"),
                lit(None).alias("FAXNBRCP"),
                col("Email").alias("EMLADRCP"),
                col("IBAN").alias("BANACCBK"),
                lit(None).alias("BICCODBK"),
                col("BillingCountryCode").alias("COUCODCI"),
                col("BillingCity").alias("CITDSC1CI"),
                col("BillingPostalCode").alias("POSCODCI"),
                lit("EUR").alias("CURCODBK"),
                lit("1").alias("ITFFINCP")
              )).write.parquet(self.dataHub.path + "/delivery/EASI/accounts/" + batchName)