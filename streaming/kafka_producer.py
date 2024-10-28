from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json
import time

def producer_func():
    admin_client = KafkaAdminClient(
    bootstrap_servers="172.25.0.12:9092"
    )
    producer = KafkaProducer(bootstrap_servers='172.25.0.13:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    with open('web_traffic_logs.json', 'r') as file:
        logs = json.load(file)

    TOPIC_NAME = 'web-traffic-logs'

    existing_topics = admin_client.list_topics()

    if TOPIC_NAME not in existing_topics:
        topic_list = [NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)

    print("kafka is producing messages...")
    for log in logs:
        producer.send(TOPIC_NAME, value=log)
        time.sleep(0.1)

    producer.flush()
    producer.close()
    print("Finished!")

if __name__ == "__main__":
    producer_func()