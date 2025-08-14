from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable
import time

print("Attempting to connect to Kafka broker for topic creation...")
admin_client = None

# Retry loop to ensure the Kafka broker is available
while admin_client is None:
    try:
        admin_client = KafkaAdminClient(bootstrap_servers="kafka:29092")
        print("Successfully connected to Kafka broker.")
    except NoBrokersAvailable:
        print("Kafka broker not yet available. Retrying in 5 seconds...")
        time.sleep(5)

topic_name = "test_topic"
existing_topics = admin_client.list_topics()

if topic_name not in existing_topics:
    print(f"Creating topic: {topic_name}")
    admin_client.create_topics([NewTopic(name=topic_name, num_partitions=1, replication_factor=1)])
    time.sleep(2)  # Give Kafka time to process the creation
    print(f"Topic '{topic_name}' created.")
else:
    print(f"Topic '{topic_name}' already exists.")

admin_client.close()
