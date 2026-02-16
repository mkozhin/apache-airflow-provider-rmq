"""Example DAG 1: Basic publish and consume with message processing.

Demonstrates the simplest use case — declare a queue,
publish a message, wait for it with a sensor, consume it,
log the full message content, and clean up.

Uses the TaskFlow API (@dag / @task decorators).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime

from airflow.decorators import dag, task

from apache_airflow_provider_rmq.operators.rmq_consume import RMQConsumeOperator
from apache_airflow_provider_rmq.operators.rmq_management import RMQQueueManagementOperator
from apache_airflow_provider_rmq.operators.rmq_publish import RMQPublishOperator
from apache_airflow_provider_rmq.sensors.rmq import RMQSensor

log = logging.getLogger("airflow.task")

RMQ_CONN_ID = "rmq_default"
QUEUE_NAME = "example_basic"


@dag(
    dag_id="rmq_example_basic",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "rmq"],
    doc_md="""
    ### Basic RabbitMQ Example
    1. Creates a durable queue
    2. Publishes a JSON message
    3. Waits for the message with RMQSensor
    4. Consumes and logs each message body
    5. Processes messages in a downstream task
    6. Deletes the queue
    """,
)
def rmq_example_basic():
    create_queue = RMQQueueManagementOperator(
        task_id="create_queue",
        action="declare_queue",
        queue_name=QUEUE_NAME,
        durable=True,
        rmq_conn_id=RMQ_CONN_ID,
    )

    publish = RMQPublishOperator(
        task_id="publish_message",
        rmq_conn_id=RMQ_CONN_ID,
        queue_name=QUEUE_NAME,
        message={"event": "order_created", "order_id": 42},
    )

    wait = RMQSensor(
        task_id="wait_for_message",
        queue_name=QUEUE_NAME,
        rmq_conn_id=RMQ_CONN_ID,
        poke_interval=5,
        timeout=60,
    )

    consume = RMQConsumeOperator(
        task_id="consume_messages",
        queue_name=QUEUE_NAME,
        rmq_conn_id=RMQ_CONN_ID,
        max_messages=10,
    )

    @task
    def process_messages(messages: list[dict]) -> list[dict]:
        """Log and process each consumed message."""
        results = []
        for i, msg in enumerate(messages):
            body = msg["body"]
            headers = msg.get("headers", {})
            routing_key = msg.get("routing_key", "")

            log.info(
                "Message %d: body=%s, headers=%s, routing_key=%s",
                i, body, headers, routing_key,
            )

            # Parse JSON body and enrich with metadata
            data = json.loads(body) if isinstance(body, str) else body
            data["_processed"] = True
            data["_routing_key"] = routing_key
            results.append(data)

        log.info("Processed %d messages total.", len(results))
        return results

    cleanup = RMQQueueManagementOperator(
        task_id="delete_queue",
        action="delete_queue",
        queue_name=QUEUE_NAME,
        rmq_conn_id=RMQ_CONN_ID,
    )

    create_queue >> publish >> wait >> consume
    processed = process_messages(consume.output)
    processed >> cleanup


rmq_example_basic()