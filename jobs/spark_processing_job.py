from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from confluent_kafka.schema_registry import SchemaRegistryClient
from datetime import datetime
import json
import threading

CURRENT_TIME = datetime.now().strftime("%Y%m%d_%H%M%S")
KAFKA_BROKERS = "kafka-broker-1:19091,kafka-broker-2:19092,kafka-broker-3:19093"
SCHEMA_REGISTRY_URL = "http://schema-registry:9081"
TOPIC_NAME = "Financial_TRANSACTIONS"
AGGREGATES_TOPIC = "AGGREGATED_TOPIC"
ANNOMALIES_TOPIC = "ANNOMALIES_TOPIC"
CHECKPOINT_LOCATION = f"/opt/bitnami/spark/checkpoint/{CURRENT_TIME}"
STATE_LOCATION = "/opt/bitnami/spark/state"
SUBJECT_NAME = "Financial_TRANSACTIONS_schema"
DB_URL = "jdbc:postgresql://postgres_db_transactions:5432/Transactions_DB"
DB_PROPERTIES = {'user': 'user', 'password': 'password', 'driver': 'org.postgresql.Driver'}
TABLE_NAME = "Transactions"

def get_schema_from_registry():
    try:
        schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
        latest_schema = schema_registry_client.get_latest_version(SUBJECT_NAME)
        schema_str = latest_schema.schema.schema_str
        return parse_avro_schema_to_spark_schema(json.loads(schema_str))
    except Exception as e:
        print(f"ERROR WHILE RETRIEVING SCHEMA : {e}")
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

agg_transactions_dataframe = transactions_dataframe.withColumn(
    'transactionTimestamp', (col('transactiontime') / 1000).cast('timestamp'))

def write_to_db(batch_df, batch_id):
    batch_df.write.jdbc(url=DB_URL, table=TABLE_NAME, mode="append", properties=DB_PROPERTIES)

def write_to_database():
    transactions_dataframe.writeStream \
        .foreachBatch(write_to_db) \
        .outputMode("append") \
        .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/db") \
        .start()

def write_aggregated_data():
    aggregated_df = agg_transactions_dataframe.groupBy("merchantId") \
        .agg(
            sum("amount").alias("totalAmount"),
            count("*").alias("transactionCount")
        )

    aggregated_df.withColumn("key", col("merchantId").cast("string")) \
        .withColumn("value", to_json(struct(
            col("merchantId"),
            col("totalAmount"),
            col("transactionCount")
        )))\
        .selectExpr("key", "value") \
        .writeStream \
        .format("kafka") \
        .outputMode("update") \
        .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
        .option("topic", AGGREGATES_TOPIC) \
        .option("partitionBy", "key") \
        .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/aggregates") \
        .start().awaitTermination()

def run_parallel():
    db_thread = threading.Thread(target=write_to_database)
    kafka_thread = threading.Thread(target=write_aggregated_data)
    
    db_thread.start()
    kafka_thread.start()
    
    db_thread.join()
    kafka_thread.join()

if __name__ == "__main__":
    run_parallel()
