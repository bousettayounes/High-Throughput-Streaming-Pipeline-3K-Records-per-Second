from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from confluent_kafka.schema_registry import SchemaRegistryClient
from datetime import datetime
import json,time


CURRENT_TIME = datetime.now().strftime("%Y%m%d_%H%M%S")
KAFKA_BROKERS = "kafka-broker-1:19091,kafka-broker-2:19092,kafka-broker-3:19093"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
TOPIC_NAME = "Financial_TRANSACTIONS"
AGGREGATES_TOPIC = "AGGREGATED_TOPIC"
ANNOMALIES_TOPIC = "ANNOMALIES_TOPIC"
CHECKPOINT_LOCATION = f"/opt/bitnami/spark/checkpoint/{CURRENT_TIME}"
STATE_LOCATION = "/opt/bitnami/spark/state"
SUBJECT_NAME = "Financial_TRANSACTIONS_schema"

def get_schema_from_registry():

    try:
        schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
        
        latest_schema = schema_registry_client.get_latest_version(SUBJECT_NAME)
        
        schema_str = latest_schema.schema.schema_str
        
        return parse_avro_schema_to_spark_schema(json.loads(schema_str))
    
    except Exception as e:
        print(f"ERROR RETRIEVING SCHEMA : {e}")
        raise

def parse_avro_schema_to_spark_schema(avro_schema):

    fields = []
    for field in avro_schema['fields']:
        field_name = field['name']
        field_type = field['type']
        
        if isinstance(field_type, list):
            actual_type = field_type[0] if field_type[0] != 'null' else field_type[1]
        else:
            actual_type = field_type
        
        spark_type = {
            'string': StringType(),
            'long': LongType(),
            'double': DoubleType(),
            'int': IntegerType(),
            'boolean': BooleanType()
        }.get(actual_type, StringType())
        
        fields.append(StructField(field_name, spark_type, nullable=True))
    
    return StructType(fields)

spark = (SparkSession.builder
         .appName("FinancialTransactionsProcessing")
         .config('spark.sql.streaming.checkpointLocation', CHECKPOINT_LOCATION)
         .config('spark.sql.streaming.stateStoreDir', STATE_LOCATION)
         .config('spark.sql.shuffle.partitions', 20)
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

transaction_schema = get_schema_from_registry()

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
    .option("partitionBy","key")\
    .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/aggregates") \
    .start().awaitTermination()