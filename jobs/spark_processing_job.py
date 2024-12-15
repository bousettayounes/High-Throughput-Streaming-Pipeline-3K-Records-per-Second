from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

KAFKA_BROKERS = "kafka-broker-1:19091,kafka-broker-2:19092,kafka-broker-3:19093"
TOPIC_NAME = "Financial_TRANSACTIONS"
AGGREGATES_TOPIC = "AGGREGATED_TOPIC"
ANNOMALIES_TOPIC = "ANNOMALIES_TOPIC"
CHECKPOINT_LOCATION = "/opt/bitnami/spark/checkpoint"
STATE_LOCATION = "/opt/bitnami/spark/state"



spark = (SparkSession.builder
         .appName("FinancialTransactionsProcessing")
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

aggregated_df.withColumn("key", col("merchantId").cast("string")) \
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