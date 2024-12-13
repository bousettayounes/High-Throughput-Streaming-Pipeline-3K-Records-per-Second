from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import logging, uuid, random, time, json

KAFKA_BROKERS = "localhost:29093,localhost:39093,localhost:49093"
NUM_PARTITIONS = 5
REPLICATION_FACTOR = 3
TOPIC_NAME = "Financial_TRANSACTIONS"

logging.basicConfig(level=logging.INFO)

def generate_transaction():
    return dict(
        transactionId=str(uuid.uuid4()),
        userId=f"user_{random.randint(1, 100)}",
        amount=round(random.uniform(50000, 150000), 2),
        transactionTime=int(time.time()),
        merchantId=random.choice(['merchant_1', 'merchant_2', 'merchant_3']),
        transactionType=random.choice(['purchase', 'refund']),
        location=f"location_{random.randint(1, 50)}",
        paymentMethod=random.choice(['credit_card', 'paypal', 'bank_transfer']),
        isInternational=random.choice(['True', 'False']),
        currency=random.choice(['USD', 'EUR', 'GBP'])
    )

producer_conf = {
    'bootstrap.servers': KAFKA_BROKERS,
    'queue.buffering.max.messages': 10000,
    'queue.buffering.max.kbytes': 512000,
    'batch.num.messages': 1000,
    'linger.ms': 10,
    'acks': 1,
    'compression.type': 'gzip'
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
            print(f"TOPIC '{topic_name}' ALREADY EXISTS")
    except Exception as e:
        logging.error(f"ERROR CREATING TOPIC: {e}")

def delivery_report(err, message):
    if err is not None:
        logging.info(f"DELIVERY FAILED FOR RECORD {message.key()}: {err}")
    else:
        logging.info(f"RECORD {message.key()} SUCCESSFULLY PRODUCED")

if __name__ == "__main__":
    create_topic(TOPIC_NAME)
    producer = Producer(producer_conf)

    while True:
        transaction = generate_transaction()
        try:
            producer.produce(
                topic=TOPIC_NAME,
                key=transaction["userId"],
                value=json.dumps(transaction).encode("utf-8"),
                on_delivery=delivery_report
            )
            print(f"PRODUCED TRANSACTION: {transaction}")
            producer.flush()
        except Exception as e:
            print(f"ERROR SENDING TRANSACTION: {e}")
