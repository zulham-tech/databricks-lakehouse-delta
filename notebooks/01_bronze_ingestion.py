from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

spark = SparkSession.builder.appName('BronzeIngestion').getOrCreate()

raw_df = (
    spark.readStream
    .format('cloudFiles')
    .option('cloudFiles.format', 'json')
    .option('cloudFiles.schemaLocation', '/mnt/bronze/_schema')
    .load('/mnt/raw/events/')
)

bronze_df = raw_df.withColumn('_ingestion_time', current_timestamp()).withColumn('_source_file', input_file_name())

(
    bronze_df.writeStream
    .format('delta')
    .option('checkpointLocation', '/mnt/bronze/_checkpoints/events')
    .option('mergeSchema', 'true')
    .outputMode('append')
    .toTable('catalog.bronze.events')
)
