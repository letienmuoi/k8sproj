#!/usr/bin/env python3
import json
import os
import sys
import time
import urllib.error
import urllib.request


BASE_URL = os.getenv("NIFI_URL", "http://nifi:8080/nifi-api")
INPUT_DIRECTORY = os.getenv("INPUT_DIRECTORY", "/opt/nifi/input")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw-data")


def request(method, path, payload=None):
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        f"{BASE_URL}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        body = response.read()
        return json.loads(body) if body else {}


def wait_for_nifi():
    for attempt in range(90):
        try:
            root = request("GET", "/flow/process-groups/root")
            return root["processGroupFlow"]["id"]
        except (urllib.error.URLError, urllib.error.HTTPError, KeyError, json.JSONDecodeError):
            if attempt == 89:
                raise
            time.sleep(5)
    raise RuntimeError("NiFi did not become ready")


def processor_by_name(group_id, name):
    response = request("GET", f"/process-groups/{group_id}/processors")
    for processor in response.get("processors", []):
        if processor.get("component", {}).get("name") == name:
            return processor
    return None


def create_processor(group_id, name, processor_type, position, properties, auto_terminate):
    existing = processor_by_name(group_id, name)
    if existing:
        return existing

    return request(
        "POST",
        f"/process-groups/{group_id}/processors",
        {
            "revision": {"version": 0},
            "component": {
                "name": name,
                "type": processor_type,
                "position": position,
                "config": {
                    "properties": properties,
                    "schedulingPeriod": "1 sec",
                    "schedulingStrategy": "TIMER_DRIVEN",
                    "executionNode": "ALL",
                    "concurrentlySchedulableTaskCount": 1,
                    "autoTerminatedRelationships": auto_terminate,
                },
            },
        },
    )


def connection_exists(group_id, source_id, destination_id):
    response = request("GET", f"/process-groups/{group_id}/connections")
    for connection in response.get("connections", []):
        component = connection.get("component", {})
        if component.get("source", {}).get("id") == source_id and component.get("destination", {}).get("id") == destination_id:
            return True
    return False


def create_connection(group_id, source_id, destination_id, relationship):
    if connection_exists(group_id, source_id, destination_id):
        return

    request(
        "POST",
        f"/process-groups/{group_id}/connections",
        {
            "revision": {"version": 0},
            "component": {
                "name": relationship,
                "source": {"id": source_id, "groupId": group_id, "type": "PROCESSOR"},
                "destination": {"id": destination_id, "groupId": group_id, "type": "PROCESSOR"},
                "selectedRelationships": [relationship],
                "flowFileExpiration": "0 sec",
                "backPressureObjectThreshold": 10000,
                "backPressureDataSizeThreshold": "1 GB",
            },
        },
    )


def start_processor(processor_id):
    processor = request("GET", f"/processors/{processor_id}")
    if processor.get("component", {}).get("state") == "RUNNING":
        return

    request(
        "PUT",
        f"/processors/{processor_id}/run-status",
        {
            "revision": processor["revision"],
            "state": "RUNNING",
            "disconnectedNodeAcknowledged": False,
        },
    )


def main():
    group_id = wait_for_nifi()
    get_file = create_processor(
        group_id,
        "Read text files",
        "org.apache.nifi.processors.standard.GetFile",
        {"x": 100.0, "y": 200.0},
        {
            "Input Directory": INPUT_DIRECTORY,
            "File Filter": ".*\\.txt",
            "Keep Source File": "false",
            "Recurse Subdirectories": "true",
            "Ignore Hidden Files": "true",
        },
        [],
    )
    split_text = create_processor(
        group_id,
        "Split into lines",
        "org.apache.nifi.processors.standard.SplitText",
        {"x": 450.0, "y": 200.0},
        {"Line Split Count": "1", "Header Line Count": "0", "Remove Trailing Newlines": "true"},
        ["failure", "original"],
    )
    publish_kafka = create_processor(
        group_id,
        "Publish raw-data",
        "org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6",
        {"x": 800.0, "y": 200.0},
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "topic": KAFKA_TOPIC,
            "acks": "all",
            "security.protocol": "PLAINTEXT",
        },
        ["success", "failure"],
    )

    create_connection(group_id, get_file["id"], split_text["id"], "success")
    create_connection(group_id, split_text["id"], publish_kafka["id"], "splits")

    for processor in (publish_kafka, split_text, get_file):
        start_processor(processor["id"])

    print("NiFi flow is configured and running", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"NiFi bootstrap failed: {error}", file=sys.stderr, flush=True)
        raise
