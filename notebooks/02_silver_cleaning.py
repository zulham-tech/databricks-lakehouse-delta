from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, trim, upper

spark = SparkSession.builder.appName('SilverCleaning').getOrCreate()

bronze_df = spark.readStream.format('delta').table('catalog.bronze.events')

silver_df = (
    bronze_df
    .filter(col('event_id').isNotNull())
    .withColumn('event_time', to_timestamp(col('event_timestamp')))
    .withColumn('user_id',    trim(col('user_id')))
    .withColumn('event_type', upper(col('event_type')))
    .dropDuplicates(['event_id'])
)

(
    silver_df.writeStream
    .format('delta')
    .option('checkpointLocation', '/mnt/silver/_checkpoints/events')
    .outputMode('append')
    .toTable('catalog.silver.events')
)
