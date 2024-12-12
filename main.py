from importlib.metadata import metadata
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import logging

KAFKA_BROKERS = "localhost:29093,localhost:39093,localhost:49093"
NUM_PARTITIONS = 5
REPLICATION_FACTOR = 3
TOPIC_NAME = "Financial_TRANSACTIONS"

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
        logging.error(f"ERROR CREATING TOPIC: {e}")


if __name__=="__main__":
    create_topic(TOPIC_NAME)