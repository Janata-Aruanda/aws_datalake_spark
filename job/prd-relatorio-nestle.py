import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, concat_ws, when, lit, split, arrays_zip, explode_outer
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Inicialização do SparkContext e SparkSession
sc = SparkContext()
spark = SparkSession.builder.config("spark.sql.broadcastTimeout", "36000").config("spark.sql.legacy.timeParserPolicy", "LEGACY").getOrCreate()

# Inicialização do GlueContext
glueContext = GlueContext(sc)

# Inicialização do Job
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)


# Define opções de leitura com configurações detalhadas
options = {
    "basePath": "s3://prd-timeline-bucket/",
    "recursiveFileLookup": "true"
}

# Leia os arquivos JSON com o esquema manualmente especificado
df_json = spark.read.options(**options) \
                    .option("multiline", "true") \
                    .json("s3://prd-timeline-bucket/")

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
    .withColumn("diagnosis_version",  when(col("event.diagnosis.version").isNull(), lit(None)).otherwise(split(concat_ws(", ", col("event.diagnosis.version")),  ",").getItem(0).cast(IntegerType()))) \
    .withColumn("diagnosis_chapter_code", col("event.diagnosis.chapter.code")) \
    .withColumn("diagnosis_chapter_title", col("event.diagnosis.chapter.title")) \
    .withColumn("diagnosis_block_code", col("event.diagnosis.block.code")) \
    .withColumn("diagnosis_block_title", col("event.diagnosis.block.title")) \
    .withColumn("diagnosis_leaf_code", col("event.diagnosis.leaf.code")) \
    .withColumn("diagnosis_leaf_title", col("event.diagnosis.leaf.title")) \
    .withColumn("diagnosticHypothesis_book",
                when(col("event.diagnosticHypothesis").isNull(), lit(None)).otherwise(split(concat_ws(", ", col("event.diagnosticHypothesis.book")), ",").getItem(0), )) \
    .withColumn("diagnosticHypothesis_version",
                when(col("event.diagnosticHypothesis").isNull(), lit(None)).otherwise(split(concat_ws(", ", col("event.diagnosticHypothesis.version")),  ",").getItem(0).cast(IntegerType()))) \
    .withColumn("diagnosticHypothesis_chapter_code",col("event.diagnosticHypothesis.chapter.code")) \
    .withColumn("diagnosticHypothesis_chapter_title",col("event.diagnosticHypothesis.chapter.title")) \
    .withColumn("diagnosticHypothesis_block_code", col("event.diagnosticHypothesis.block.code")) \
    .withColumn("diagnosticHypothesis_block_title", col("event.diagnosticHypothesis.block.title")) \
    .withColumn("diagnosticHypothesis_leaf_code", col("event.diagnosticHypothesis.leaf.code")) \
    .withColumn("diagnosticHypothesis_leaf_title", col("event.diagnosticHypothesis.leaf.title")) \
    .withColumn("diagnosis_chapter", arrays_zip(col("diagnosis_chapter_code"), col("diagnosis_chapter_title"))) \
    .withColumn("diagnosis_block", arrays_zip(col("diagnosis_block_code"), col("diagnosis_block_title"))) \
    .withColumn("diagnosis_leaf", arrays_zip(col("diagnosis_leaf_code"), col("diagnosis_leaf_title"))) \
    .withColumn("diagnosticHypothesis_chapter", arrays_zip(col("diagnosticHypothesis_chapter_code"), col("diagnosticHypothesis_chapter_title"))) \
    .withColumn("diagnosticHypothesis_block", arrays_zip(col("diagnosticHypothesis_block_code"), col("diagnosticHypothesis_block_title"))) \
    .withColumn("diagnosticHypothesis_leaf", arrays_zip(col("diagnosticHypothesis_leaf_code"), col("diagnosticHypothesis_leaf_title"))) \
    .drop("event", "diagnosis_chapter_code", "diagnosis_chapter_title", "diagnosis_block_code", "diagnosis_block_title", "diagnosis_leaf_code", "diagnosis_leaf_title", "diagnosticHypothesis_chapter_code", "diagnosticHypothesis_chapter_title", "diagnosticHypothesis_block_code", "diagnosticHypothesis_block_title", "diagnosticHypothesis_leaf_code", "diagnosticHypothesis_leaf_title")

# Aplicando transformação da Data e Mês
df_process_silver = df_process.withColumn('data', F.to_date(F.col('updatedAt'), 'yyyy-MM-dd')) \
                        .withColumn('ano', F.year('data'))\
                        .withColumn('mes', F.month('data'))\
                        .filter(F.col('mes') == 3) \
                        .drop("data")

# Explodindo as colunas, se não tiver valor irá ficar NULL
df_silver = df_process_silver \
    .withColumn('diagnosis_chapter_code', explode_outer(col("diagnosis_chapter.diagnosis_chapter_code"))) \
    .withColumn('diagnosis_chapter_title', explode_outer(col("diagnosis_chapter.diagnosis_chapter_title"))) \
    .withColumn('diagnosis_block_code', explode_outer(col("diagnosis_block.diagnosis_block_code"))) \
    .withColumn('diagnosis_block_title', explode_outer(col("diagnosis_block.diagnosis_block_title"))) \
    .withColumn('diagnosis_leaf_code', explode_outer(col("diagnosis_leaf.diagnosis_leaf_code"))) \
    .withColumn('diagnosis_leaf_title', explode_outer(col("diagnosis_leaf.diagnosis_leaf_title"))) \
    .withColumn('diagnosticHypothesis_chapter_code', explode_outer(col("diagnosticHypothesis_chapter.diagnosticHypothesis_chapter_code"))) \
    .withColumn('diagnosticHypothesis_chapter_title', explode_outer(col("diagnosticHypothesis_chapter.diagnosticHypothesis_chapter_title"))) \
    .withColumn('diagnosticHypothesis_block_code', explode_outer(col("diagnosticHypothesis_block.diagnosticHypothesis_block_code"))) \
    .withColumn('diagnosticHypothesis_block_title', explode_outer(col("diagnosticHypothesis_block.diagnosticHypothesis_block_title"))) \
    .withColumn('diagnosticHypothesis_leaf_code', explode_outer(col("diagnosticHypothesis_leaf.diagnosticHypothesis_leaf_code"))) \
    .withColumn('diagnosticHypothesis_leaf_title', explode_outer(col("diagnosticHypothesis_leaf.diagnosticHypothesis_leaf_title"))) \
    .dropDuplicates(["eventId"])

# Selecioando as colunas e realizando um filtro
df_gold = df_silver.filter((F.col("tenantId") == 33) & (F.col('mes') == 3)) \
        .select("eventId", "tenantId", "ano", "mes", "eventTypeName", 
                "resolution_name", "severity_name", 
                "diagnosis_chapter_code", "diagnosis_chapter_title", "diagnosis_block_code", "diagnosis_block_title", "diagnosis_leaf_code", "diagnosis_leaf_title",
                "diagnosticHypothesis_chapter_code", "diagnosticHypothesis_chapter_title", "diagnosticHypothesis_block_code", "diagnosticHypothesis_block_title", "diagnosticHypothesis_leaf_code", "diagnosticHypothesis_leaf_title")

# Salvando
df_gold.coalesce(1) \
       .write.format("csv") \
       .mode("overwrite") \
       .option("header", "true") \
       .save("s3://hml-datalake/relatorio/33-nestle/03-2024/")

# Stop no Spark
spark.stop()

# Finalização do Job
job.commit()