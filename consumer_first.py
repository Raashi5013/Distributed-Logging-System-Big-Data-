from kafka import KafkaConsumer
import json

# Define the Kafka topics for each service (from Fluentd)
topics = [
    "order_service_logs",        # Order Service logs
    "notification_service_logs", # Notification Service logs
    "inventory_service_logs"     # Inventory Service logs
]

bootstrap_servers = 'localhost:9092'

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    *topics,  # Subscribe to multiple topics
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='latest',  # Start from the latest message
    enable_auto_commit=True,     # Commit offsets automatically
    group_id='log_consumer_group',  # Consumer group ID
    value_deserializer=lambda x: x.decode('utf-8')  # Decode message as UTF-8 text first
)

# Open the output file for appending logs
output_file = 'sample_initial_consumer_output.json'

with open(output_file, 'w') as file:
    print("Listening to Kafka topics:", topics)
    
    # Write the opening bracket to indicate the start of a JSON array
    file.write("[\n")

    first_entry = True  # Flag to check if it's the first log entry

    try:
        for message in consumer:
            # Get the log message content
            log_data = message.value

            # Attempt to parse the log message as JSON
            try:
                parsed_log = json.loads(log_data)
            except json.JSONDecodeError:
                print(f"Non-JSON message received: {log_data}")
                continue

            # Ensure the parsed log matches the expected format
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
                if log_level == "INFO":
                    formatted_log = {
                        "log_id": parsed_log.get("log_id"),
                        "node_id": parsed_log.get("node_id"),
                        "log_level": "INFO",
                        "message_type": "LOG",
                        "message": parsed_log.get("message"),
                        "service_name": parsed_log.get("service_name"),
                        "timestamp": parsed_log.get("timestamp")
                    }
                elif log_level == "WARN":
                    formatted_log = {
                        "log_id": parsed_log.get("log_id"),
                        "node_id": parsed_log.get("node_id"),
                        "log_level": "WARN",
                        "message_type": "LOG",
                        "message": parsed_log.get("message"),
                        "service_name": parsed_log.get("service_name"),
                        "response_time_ms": parsed_log.get("response_time_ms"),
                        "threshold_limit_ms": parsed_log.get("threshold_limit_ms"),
                        "timestamp": parsed_log.get("timestamp")
                    }
                elif log_level == "ERROR":
                    formatted_log = {
                        "log_id": parsed_log.get("log_id"),
                        "node_id": parsed_log.get("node_id"),
                        "log_level": "ERROR",
                        "message_type": "LOG",
                        "message": parsed_log.get("message"),
                        "service_name": parsed_log.get("service_name"),
                        "error_details": parsed_log.get("error_details"),
                        "timestamp": parsed_log.get("timestamp")
                    }
                else:
                    print(f"Unknown log level: {log_level}")
                    continue
            elif log_type == "HEARTBEAT":
                formatted_log = {
                    "node_id": parsed_log.get("node_id"),
                    "message_type": "HEARTBEAT",
                    "status": parsed_log.get("status"),
                    "timestamp": parsed_log.get("timestamp")
                }
            elif log_type == "REGISTRY":
                formatted_log = {
                    "message_type": "REGISTRY",
                    "node_id": parsed_log.get("node_id"),
                    "service_name": parsed_log.get("service_name"),
                    "status": parsed_log.get("status"),
                    "timestamp": parsed_log.get("timestamp")
                }
            else:
                print(f"Unknown message type: {log_type}")
                continue

            # Write the formatted log to the output file
            # If it's not the first entry, write a comma before the log entry
            if not first_entry:
                file.write(",\n")
            else:
                first_entry = False  # Set the flag to False after the first entry
            
            # Dump the log in JSON format
            json.dump(formatted_log, file, indent=2)

            print(f"Logged: {formatted_log}")
            print("-" * 40)

    except KeyboardInterrupt:
        print("\nShutting down consumer...")
    finally:
        # Write the closing bracket to indicate the end of the JSON array
        file.write("\n]\n")
        consumer.close()