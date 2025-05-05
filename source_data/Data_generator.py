import json
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from datetime import datetime
import logging, uuid, random, threading

KAFKA_BROKERS = "localhost:29093,localhost:39093,localhost:49093"
SCHEMA_REGISTRY_URL = "http://localhost:9081"
NUM_PARTITIONS = 5
REPLICATION_FACTOR = 3
TOPIC_NAME = "Financial_TRANSACTIONS"
SUBJECT_NAME = "Financial_TRANSACTIONS_schema"

logging.basicConfig(level=logging.INFO)

def register_or_get_schema():

    schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_schema = {
        "type": "record",
        "name": "Transactions_schema",
        "fields": [
            {"name": "transactionId", "type": ["string", "null"]},
            {"name": "userId", "type": ["string", "null"]},
            {"name": "merchantId", "type": ["string", "null"]},
            {"name": "amount", "type": ["double", "null"]},
            {"name": "transactionTime", "type": ["string", "null"]},
            {"name": "transactionType", "type": ["string", "null"]},
            {"name": "location", "type": ["string", "null"]},
            {"name": "paymentMethod", "type": ["string", "null"]},
            {"name": "isInternational", "type": ["string", "null"]},
            {"name": "currency", "type": ["string", "null"]}
        ]
    }

    schema_str = json.dumps(avro_schema)
    schema = Schema(schema_str, schema_type="AVRO")

    try:
        registered_schema = schema_registry_client.get_latest_version(SUBJECT_NAME)
        logging.info(f"Retrieved existing schema. Schema ID: {registered_schema.schema_id}")
        return registered_schema.schema.schema_str, schema_registry_client
    
    except Exception as e:
        try:
            schema_id = schema_registry_client.register_schema(subject_name=SUBJECT_NAME, schema=schema)
            logging.info(f"Schema registered successfully. Schema ID: {schema_id}")
            return schema_str, schema_registry_client
        
        except Exception as reg_error:
            logging.error(f"Error registering schema: {reg_error}")
            raise


def generate_transaction():
    transaction_time = datetime.now() 
    
    return {
        "transactionId": str(uuid.uuid4()),
        "userId": f"user_{random.randint(1, 100)}",
        "merchantId": random.choice(['merchant_1', 'merchant_2', 'merchant_3']),
        "amount": round(random.uniform(50000, 150000), 2),
        "transactionTime": transaction_time.strftime('%Y-%m-%d %H:%M:%S'),
        "transactionType": random.choice(['purchase', 'refund']),
        "location": f"location_{random.randint(1, 50)}",
        "paymentMethod": random.choice(['credit_card', 'paypal', 'bank_transfer']),
        "isInternational": random.choice(['True', 'False']),
        "currency": random.choice(['USD', 'EUR', 'GBP'])
    }

def create_topic(topic_name):
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKERS})

    try:
        meta_data = admin_client.list_topics(timeout=10)
        if topic_name not in meta_data.topics:
            topic = NewTopic(
                topic=topic_name,
                num_partitions=NUM_PARTITIONS,
                replication_factor=REPLICATION_FACTOR
            )
            fs = admin_client.create_topics([topic])
            for topic, future in fs.items():
                try:
                    future.result()
                    logging.info(f"TOPIC '{topic_name}' CREATED SUCCESSFULLY!")
                except Exception as e:
                    logging.error(f"FAILED TO CREATE TOPIC '{topic_name}': {e}")
        else:
            logging.info(f"TOPIC '{topic_name}' ALREADY EXISTS")

    except Exception as e:
        logging.error(f"ERROR CREATING WHILE CREATING THE TOPIC : {topic_name} | ERROR :  {e}")

def delivery_report(err, message):
    if err is not None:
        logging.error(f"DELIVERY FAILED FOR RECORD {message.key()}: {err}")
    else:
        logging.info(f"RECORD {message.key()} SUCCESSFULLY PRODUCED")

def produce_transactions(thread_id, producer, schema_registry_client, schema_str):
    while True:
        transaction = generate_transaction()
        try:
            producer.produce(
                topic=TOPIC_NAME,
                key=transaction["userId"],
                value=json.dumps(transaction).encode("utf-8"),
                on_delivery=delivery_report
            )
            logging.info(f" THREAD {thread_id} - PRODUCED TRANSACTION: {transaction}")
            producer.flush()
        except Exception as e:
            logging.error(f"ERROR SENDING TRANSACTION: {e}")

def producer_data_in_para(num_threads, schema_registry_client, schema_str):
    threads = []
    producer_conf = {
        'bootstrap.servers': KAFKA_BROKERS,
        'queue.buffering.max.messages': 10000,
        'queue.buffering.max.kbytes': 512000,
        'batch.num.messages': 1000,
        'linger.ms': 10,
        'acks': 1,
        'compression.type': 'gzip'
    }
    producer = Producer(producer_conf)
    
    try:
        for i in range(num_threads):
            thread = threading.Thread(
                target=produce_transactions, 
                args=(i, producer, schema_registry_client, schema_str)
            )
            thread.daemon = True
            thread.start()
            threads.append(thread)
        
        for thread in threads:
            thread.join()
    except Exception as e:
        logging.error(f"Error is : {e}")

def main():
    create_topic(TOPIC_NAME)
    
    schema_str, schema_registry_client = register_or_get_schema()
    
    producer_data_in_para(3, schema_registry_client, schema_str)

if __name__ == "__main__":
    main()