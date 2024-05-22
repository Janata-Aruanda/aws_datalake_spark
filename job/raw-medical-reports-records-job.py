import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, concat_ws, when, lit, split
from pyspark.sql.types import *


date = new


# Inicialização do SparkContext e SparkSession
sc = SparkContext()
spark = SparkSession.builder.config("spark.sql.broadcastTimeout", "36000").config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate()

# Inicialização do GlueContext
glueContext = GlueContext(sc)

# Inicialização do Job
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATABASE_NAME', 'TABLE_NAME', 'BUCKET_PATH', 'BUCKET_PATH_OUTPUT'])
job.init(args['JOB_NAME'], args)

# argumentos do Job
database = args['DATABASE_NAME']
tablename = args['TABLE_NAME']
bucketpath = args['BUCKET_PATH']
bucketpathoutput = args['BUCKET_PATH_OUTPUT']

# Define opções de leitura com configurações detalhadas
options = {
    "basePath": bucketpath,
    "recursiveFileLookup": "true"
}

# Leia os arquivos JSON com o esquema manualmente especificado
df_json = spark.read.options(**options) \
                    .option("multiline", "true") \
                    .option("inferSchema" , "true") \
                    .json(bucketpath)

# Aplicar transformações no DataFrame
df_process = df_json.withColumn("recordAppointmentType_id", col("event.recordAppointmentType.id")) \
    .withColumn("recordAppointmentType_name", col("event.recordAppointmentType.name")) \
    .withColumn("resolution_id", col("event.resolution.id")) \
    .withColumn("resolution_name", col("event.resolution.name")) \
    .withColumn("severity_id", col("event.severity.id")) \
    .withColumn("severity_name", col("event.severity.name")) \
    .withColumn("complaints_id", concat_ws(", ", col("event.complaints.id"))) \
    .withColumn("complaints_name", concat_ws(", ", col("event.complaints.name"))) \
    .withColumn("diagnosis_book", 
                when(col("event.diagnosis").isNull(), lit(None)).otherwise(split(concat_ws(", ", col("event.diagnosis.book")), ", ").getItem(0))) \
    .withColumn("diagnosis_version", 
                when(col("event.diagnosis").isNull(), lit(None)).otherwise(split(concat_ws(", ", col("event.diagnosis.version")), ",").getItem(0).cast(IntegerType()))) \
    .withColumn("diagnosis_chapter_code", 
                when(col("event.diagnosis").isNull(), lit(None)).otherwise(concat_ws(", ", col("event.diagnosis.chapter.code")))) \
    .withColumn("diagnosis_chapter_title", 
                when(col("event.diagnosis").isNull(), lit(None)).otherwise(concat_ws(", ", col("event.diagnosis.chapter.title")))) \
    .withColumn("diagnosis_block_code", 
                when(col("event.diagnosis").isNull(), lit(None)).otherwise(concat_ws(", ", col("event.diagnosis.block.code")))) \
    .withColumn("diagnosis_block_title", 
                when(col("event.diagnosis").isNull(), lit(None)).otherwise(concat_ws(", ", col("event.diagnosis.block.title")))) \
    .withColumn("diagnosis_leaf_code", 
                when(col("event.diagnosis").isNull(), lit(None)).otherwise(concat_ws(", ", col("event.diagnosis.leaf.code")))) \
    .withColumn("diagnosis_leaf_title", 
                when(col("event.diagnosis").isNull(), lit(None)).otherwise(concat_ws(", ", col("event.diagnosis.leaf.title")))) \
    .withColumn("diagnosticHypothesis_book", 
                when(col("event.diagnosticHypothesis").isNull(), lit(None)).otherwise(split(concat_ws(", ", col("event.diagnosticHypothesis.book")), ",").getItem(0), )) \
    .withColumn("diagnosticHypothesis_version", 
                when(col("event.diagnosticHypothesis").isNull(), lit(None)).otherwise(split(concat_ws(", ", col("event.diagnosticHypothesis.version")),  ",").getItem(0).cast(IntegerType()))) \
    .withColumn("diagnosticHypothesis_chapter_code", 
                when(col("event.diagnosticHypothesis").isNull(), lit(None)).otherwise(concat_ws(", ", col("event.diagnosticHypothesis.chapter.code")))) \
    .withColumn("diagnosticHypothesis_chapter_title", 
                when(col("event.diagnosticHypothesis").isNull(), lit(None)).otherwise(concat_ws(", ", col("event.diagnosticHypothesis.chapter.title")))) \
    .withColumn("diagnosticHypothesis_block_code", 
                when(col("event.diagnosticHypothesis").isNull(), lit(None)).otherwise(concat_ws(", ", col("event.diagnosticHypothesis.block.code")))) \
    .withColumn("diagnosticHypothesis_block_title", 
                when(col("event.diagnosticHypothesis").isNull(), lit(None)).otherwise(concat_ws(", ", col("event.diagnosticHypothesis.block.title")))) \
    .withColumn("diagnosticHypothesis_leaf_code", 
                when(col("event.diagnosticHypothesis").isNull(), lit(None)).otherwise(concat_ws(", ", col("event.diagnosticHypothesis.leaf.code")))) \
    .withColumn("diagnosticHypothesis_leaf_title", 
                when(col("event.diagnosticHypothesis").isNull(), lit(None)).otherwise(concat_ws(", ", col("event.diagnosticHypothesis.leaf.title")))) \
    .drop("event")

# Script generated for node Amazon S3
additional_options = {"write.parquet.compression-codec": "gzip"}

tables_collection = spark.catalog.listTables(database)
table_names_in_db = [table.name for table in tables_collection]
table_exists = tablename in table_names_in_db

if table_exists:
    df_process.coalesce(1)\
        .writeTo(f"glue_catalog.{database}.{tablename}") \
        .tableProperty("format-version", "2") \
        .tableProperty("location", bucketpathoutput) \
        .options(**additional_options) \
        .append()
else:
    df_process.coalesce(1)\
        .writeTo(f"glue_catalog.{database}.{tablename}") \
        .tableProperty("format-version", "2") \
        .tableProperty("location", bucketpathoutput) \
        .options(**additional_options) \
        .create()

spark.stop()

# Finalização do Job
job.commit()