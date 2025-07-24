#!/usr/bin/env python3
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json

topics = [
    "order_service_logs",        # Order Service logs
    "notification_service_logs", # Notification Service logs
    "inventory_service_logs"     # Inventory Service logs
]

bootstrap_servers = 'localhost:9092'


consumer = KafkaConsumer(
    *topics,  
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='latest',  
    enable_auto_commit=True,    
    group_id='log_consumer_group', 
    value_deserializer=lambda x: x.decode('utf-8') 
)

# Initialize Elasticsearch client
es = Elasticsearch("http://localhost:9200")

# Index name for storing logs
log_index = "system_logs"

def create_index(index_name):
    if not es.indices.exists(index=index_name):
        es.indices.create(
            index=index_name,
            body={
                "mappings": {
                    "properties": {
                        "node_id": {"type": "keyword"},
                        "log_level": {"type": "keyword"},
                        "message_type": {"type": "keyword"},
                        "message": {"type": "text"},
                        "service_name": {"type": "keyword"},
                        "timestamp": {"type": "date"},
                        "response_time_ms": {"type": "float"},
                        "threshold_limit_ms": {"type": "float"},
                        "error_details": {"type": "text"}
                    }
                }
            }
        )
        print(f"Elasticsearch index '{index_name}' created.")
    else:
        print(f"Elasticsearch index '{index_name}' already exists.")


create_index(log_index)

def send_to_elasticsearch(index_name, log_data):
    try:
        es.index(index=index_name, document=log_data)
        print(f"Log indexed: {log_data}")
    except Exception as e:
        print(f"Error indexing log: {e}")


print("Listening to Kafka topics:", topics)

try:
    for message in consumer:
        log_data = message.value

     
        try:
            parsed_log = json.loads(log_data)
        except json.JSONDecodeError:
            print(f"Non-JSON message received: {log_data}")
            continue

        
        log_type = parsed_log.get("message_type", "UNKNOWN").upper()

        if log_type == "REGISTRATION":
            formatted_log = {
                "node_id": parsed_log.get("node_id"),
                "message_type": "REGISTRATION",
                "service_name": parsed_log.get("service_name"),
                "timestamp": parsed_log.get("timestamp")
            }
        elif log_type == "LOG":
            log_level = parsed_log.get("log_level", "UNKNOWN").upper()
            formatted_log = {
                "log_id": parsed_log.get("log_id"),
                "node_id": parsed_log.get("node_id"),
                "log_level": log_level,
                "message_type": "LOG",
                "message": parsed_log.get("message"),
                "service_name": parsed_log.get("service_name"),
                "timestamp": parsed_log.get("timestamp"),
                "response_time_ms": parsed_log.get("response_time_ms"),
                "threshold_limit_ms": parsed_log.get("threshold_limit_ms"),
                "error_details": parsed_log.get("error_details")
            }
        elif log_type == "HEARTBEAT":
            formatted_log = {
                "node_id": parsed_log.get("node_id"),
                "message_type": "HEARTBEAT",
                "status": parsed_log.get("status"),
                "timestamp": parsed_log.get("timestamp")
            }
        else:
            print(f"Unknown message type: {log_type}")
            continue

        # Send the formatted log to Elasticsearch
        send_to_elasticsearch(log_index, formatted_log)

except KeyboardInterrupt:
    print("\nShutting down consumer...")
finally:
    consumer.close()
