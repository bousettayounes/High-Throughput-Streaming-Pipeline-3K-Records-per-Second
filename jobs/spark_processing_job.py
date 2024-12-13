from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

KAFKA_BROKERS = "localhost:29093,localhost:39093,localhost:49093"
TOPIC_NAME = "Financial_TRANSACTIONS"
AGGREGATES_TOPIC = "AGG_TOPIC"
ANNOMALIES_TOPIC = "ANN_TOPIC"
CHECKPOINT_LOCATION = r'D:\Personal_Projects\Big Data processing\checkpoint'
STATE_LOCATION = r'D:\Personal_Projects\Big Data processing\state'
spark = (SparkSession.builder
         .appName("FinancialTransactionsProcessing")
         .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1')
         .config('spark.sql.streaming.checkpointLocation', CHECKPOINT_LOCATION)
         .config('spark.sql.streaming.stateStoreDir', STATE_LOCATION)
         .config('spark.sql.shuffle.partitions', 20)
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
transaction_schema = StructType([
    StructField('transactionId', StringType(), nullable=True),
    StructField('userId', StringType(), nullable=True),
    StructField('merchantId', StringType(), nullable=True),
    StructField('amount', DoubleType(), nullable=True),
    StructField('transactionTime', LongType(), nullable=True),
    StructField('transactionType', StringType(), nullable=True),
    StructField('location', StringType(), nullable=True),
    StructField('paymentMethod', StringType(), nullable=True),
    StructField('isInternational', StringType(), nullable=True),
    StructField('currency', StringType(), nullable=True),
])


kafka_stream = (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', KAFKA_BROKERS)
                .option('subscribe', TOPIC_NAME)
                .option('startingOffsets', 'earliest')
                .load())

transactions_dataframe = (kafka_stream
                          .selectExpr("CAST(value AS STRING)")
                          .select(from_json(col('value'), transaction_schema).alias("Data"))
                          .select('Data.*'))

transactions_dataframe = transactions_dataframe.withColumn(
    'transactionTimestamp', (col('transactionTime') / 1000).cast('timestamp'))

aggregated_df = transactions_dataframe.groupBy("merchantId") \
    .agg(
        sum("amount").alias("totalAmount"),
        count("*").alias("transactionCount")
    )

aggregated_df = transactions_dataframe.groupby("merchantId") \
    .agg(
        sum("amount").alias("totalAmount"),
        count("*").alias("transactionCount")
    )

aggregation_query = aggregated_df.withColumn("key", col("merchantId").cast("string")) \
    .withColumn("value", to_json(struct(
        col("merchantId"),
        col("totalAmount"),
        col("transactionCount")
    ))).selectExpr("key", "value") \
    .writeStream \
    .format("kafka") \
    .outputMode("update") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("topic", AGGREGATES_TOPIC) \
    .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/aggregates") \
    .start().awaitTermination()

