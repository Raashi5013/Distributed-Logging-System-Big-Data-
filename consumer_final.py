#!/usr/bin/env python3
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import json

# Initialize Elasticsearch client
client = Elasticsearch("http://localhost:9200")
last_heartbeat = {}
heartbeat_timeout = timedelta(seconds=10)

# Define the Kafka topics for each service (from Fluentd)
topics = [
    "order_service_logs",       
    "notification_service_logs", 
    "inventory_service_logs" 
]

bootstrap_servers = '192.168.128.128:9092'

if not client.indices.exists(index="registration_logs"):
    client.indices.create(index="registration_logs")
    print("created database")

if not client.indices.exists(index="registry_logs"):
    client.indices.create(index="registry_logs")

if not client.indices.exists(index="log_entries"):
    client.indices.create(index="log_entries")

consumer = KafkaConsumer(
    *topics,
    value_deserializer=lambda m: json.loads(m.decode('ascii')),  
    bootstrap_servers=bootstrap_servers
)

output_file = 'consumer_output.json'

with open(output_file, 'w') as file:
    print("Listening to Kafka topics:", topics)

    file.write("[\n")

    first_entry = True  

    try:
        for message in consumer:
            parsed_log = message.value 

            log_type = parsed_log.get("message_type", "UNKNOWN").upper()

            if log_type == "REGISTRATION":
                formatted_log = {
                    "node_id": parsed_log.get("node_id"),
                    "message_type": "REGISTRATION",
                    "service_name": parsed_log.get("service_name"),
                    "timestamp": parsed_log.get("timestamp")
                }

                client.index(index="registration_logs", document=formatted_log)

            elif log_type == "REGISTRY":
                formatted_log = {
                    "message_type": "REGISTRY",
                    "node_id": parsed_log.get("node_id"),
                    "service_name": parsed_log.get("service_name"),
                    "status": parsed_log.get("status"),
                    "timestamp": parsed_log.get("timestamp")
                }

                client.index(index="registry_logs", document=formatted_log)

            elif log_type == "LOG":
                log_level = parsed_log.get("log_level", "UNKNOWN").upper()
                if log_level in ["INFO", "WARN", "ERROR"]:
                    formatted_log = {
                        "log_id": parsed_log.get("log_id"),
                        "node_id": parsed_log.get("node_id"),
                        "log_level": log_level,
                        "message_type": "LOG",
                        "message": parsed_log.get("message"),
                        "service_name": parsed_log.get("service_name"),
                        "timestamp": parsed_log.get("timestamp")
                    }
                    if log_level in ["WARN", "ERROR"]:
                        print(f"ALERT: [{log_level}] {formatted_log['service_name']} (Node {formatted_log['node_id']}) - {formatted_log['message']}")
                    #print(f"Logged {formatted_log} into log_entries")
                    client.index(index="log_entries", document=formatted_log)

            elif log_type == "HEARTBEAT":
                node_id = parsed_log.get("node_id")
                last_heartbeat[node_id] = datetime.now()
                formatted_log = {
                    "node_id": node_id,
                    "message_type": "HEARTBEAT",
                    "status": parsed_log.get("status"),
                    "timestamp": parsed_log.get("timestamp")
                }
            else:
                print(f"Unknown message type: {log_type}")
                continue

            # Check for node heartbeat timeouts
            current_time = datetime.now()
            for node_id, last_time in last_heartbeat.items():
                if current_time - last_time > heartbeat_timeout:
                    print(f"NODE FAILURE: Node {node_id} last heartbeat at {last_time}")

            if not first_entry:
                file.write(",\n")
            else:
                first_entry = False
            
            if formatted_log:
                json.dump(formatted_log, file, indent=2)

            #print(f"Logged: {formatted_log}")
            #print("-" * 40)

    except KeyboardInterrupt:
        print("\nShutting down consumer...")
    finally:
        file.write("\n]\n")
        consumer.close()