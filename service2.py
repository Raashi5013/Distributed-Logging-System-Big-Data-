#!/usr/bin/env python3
import json
from time import sleep
import threading
from datetime import datetime
import random
import uuid
from fluent import sender
import socket

NODE_ID = 2
SERVICE_NAME = "Notification Service"

hostname = socket.gethostname()
IPAddr = socket.gethostbyname(hostname)
port = 24225  # Fluentd port 

fluentd_logger = sender.FluentSender('app', host='localhost', port=port)

def get_timestamp():
    return datetime.now().isoformat()

def registration():
    registration_log = {
        "node_id": NODE_ID,
        "message_type": "REGISTRATION",
        "service_name": SERVICE_NAME,
        "timestamp": get_timestamp(),
    }
    return registration_log

def generate_registry_log(status="UP"):
    return {
        "message_type": "REGISTRY",
        "node_id": NODE_ID,
        "service_name": SERVICE_NAME,
        "status": status,
        "timestamp": get_timestamp(),
    }

def generate_info_log():
    return {
        "log_id": str(uuid.uuid4()),
        "node_id": NODE_ID,
        "log_level": "INFO",
        "message_type": "LOG",
        "message": f"INFO: General operation log for {SERVICE_NAME}.",
        "service_name": SERVICE_NAME,
        "timestamp": get_timestamp(),
    }

def generate_warn_log():
    return {
        "log_id": str(uuid.uuid4()),
        "node_id": NODE_ID,
        "log_level": "WARN",
        "message_type": "LOG",
        "message": f"WARN: Response time high for {SERVICE_NAME}.",
        "service_name": SERVICE_NAME,
        "response_time_ms": random.randint(400, 500),
        "threshold_limit_ms": 300,
        "timestamp": get_timestamp(),
    }

def generate_error_log():
    return {
        "log_id": str(uuid.uuid4()),
        "node_id": NODE_ID,
        "log_level": "ERROR",
        "message_type": "LOG",
        "message": f"ERROR: Issue detected in {SERVICE_NAME}.",
        "service_name": SERVICE_NAME,
        "error_details": {
            "error_code": "500",
            "error_message": "Internal server error",
        },
        "timestamp": get_timestamp(),
    }

def generate_heartbeat_log():
    return {
        "node_id": NODE_ID,
        "message_type": "HEARTBEAT",
        "status": "UP",
        "timestamp": get_timestamp(),
    }

def heartbeat():
    while True:
        heartbeat_log = generate_heartbeat_log()
        fluentd_logger.emit("notification_service", heartbeat_log)
        sleep(5) 

heartbeat_thread = threading.Thread(target=heartbeat)
heartbeat_thread.daemon = True
heartbeat_thread.start()

try:
    registration_msg = registration()
    fluentd_logger.emit("notification_service", registration_msg)

    registry_log = generate_registry_log(status="UP")
    fluentd_logger.emit("notification_service", registry_log)

    while True:
        random_choice = random.randint(1, 100)
        if random_choice <= 60:
            log = generate_info_log()
        elif random_choice <= 90:
            log = generate_warn_log()
        else:
            log = generate_error_log()

        fluentd_logger.emit("notification_service", log)
        sleep(1)

except KeyboardInterrupt:
    print(f"Stopping {SERVICE_NAME}.")
    registry_log = generate_registry_log(status="DOWN")
    fluentd_logger.emit("notification_service", registry_log)
