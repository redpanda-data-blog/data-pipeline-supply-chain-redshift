import csv
import json
from kafka import KafkaProducer

# Kafka producer configuration
config = {
    'bootstrap_servers': ['localhost:19092'],  # Update with your Redpanda server address
    'client_id': 'supply-chain-producer'
}

# Initialize KafkaProducer using kafka-python-ng
producer = KafkaProducer(
    bootstrap_servers=config['bootstrap_servers'],
    client_id=config['client_id'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize the messages as JSON
)

topic_name = 'supply_chain_data'  # Update with your topic name

# Define your schema for the supply chain data
schema = {
    "type": "struct",
    "fields": [
        {"field": "supplier_id", "type": "string"},
        {"field": "delivery_timestamp", "type": "string"},
        {"field": "production_timestamp", "type": "string"},
        {"field": "vehicle_timestamp", "type": "string"},
        {"field": "material_delivered", "type": "string"},
        {"field": "quantity", "type": "string"},
        {"field": "delivery_status", "type": "string"},
        {"field": "production_line_id", "type": "string"},
        {"field": "operation_efficiency", "type": "string"},
        {"field": "downtime_minutes", "type": "string"},
        {"field": "vehicle_id", "type": "string"},
        {"field": "latitude", "type": "string"},
        {"field": "longitude", "type": "string"},
        {"field": "environmental_conditions", "type": "string"}
    ]
}

def produce_message(record):
    message = {
        "schema": schema,
        "payload": record
    }
    # Send the message to Kafka
    producer.send(topic_name, value=message)
    producer.flush()

def produce_from_csv(file_path):
    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            produce_message(row)

csv_file_path = 'supply_chain_data.csv'  # Update this with the path to your CSV file
produce_from_csv(csv_file_path)
