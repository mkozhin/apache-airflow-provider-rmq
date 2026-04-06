"""Example DAG 4: Deferrable sensor with header filtering.

Demonstrates RMQSensor in deferrable mode — the sensor defers
execution to the triggerer process, freeing the worker slot while
waiting for a matching RabbitMQ message.

Uses the TaskFlow API (@dag / @task decorators).
"""

from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import dag, task

from airflow_provider_rmq.operators.rmq_management import RMQQueueManagementOperator
from airflow_provider_rmq.operators.rmq_publish import RMQPublishOperator
from airflow_provider_rmq.sensors.rmq import RMQSensor

log = logging.getLogger("airflow.task")

RMQ_CONN_ID = "rmq_default"
QUEUE_NAME = "example_deferred"


@dag(
    dag_id="rmq_sensor_deferrable",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "rmq", "deferrable"],
    doc_md="""
    ### Deferrable Sensor Example
    Uses `deferrable=True` to free the worker slot while waiting.
    The sensor defers to RMQTrigger which runs in the triggerer process.

    **Note:** `filter_callable` is NOT supported in deferrable mode.
    Use `filter_headers` for dict-based filtering instead.
    """,
)
def rmq_sensor_deferrable():
    setup = RMQQueueManagementOperator(
        task_id="setup_queue",
        action="declare_queue",
        queue_name=QUEUE_NAME,
        durable=True,
        rmq_conn_id=RMQ_CONN_ID,
    )

    # Publish a non-matching message first, then a matching one
    publish_noise = RMQPublishOperator(
        task_id="publish_noise",
        rmq_conn_id=RMQ_CONN_ID,
        queue_name=QUEUE_NAME,
        message={"event": "page_view"},
        headers={"x-type": "analytics"},
    )

    publish_order = RMQPublishOperator(
        task_id="publish_order",
        rmq_conn_id=RMQ_CONN_ID,
        queue_name=QUEUE_NAME,
        message={"event": "order_created", "order_id": 999},
        headers={"x-type": "order"},
        delivery_mode=2,
    )

    # Deferrable sensor — waits for x-type=order header
    wait_for_order = RMQSensor(
        task_id="wait_for_order",
        queue_name=QUEUE_NAME,
        rmq_conn_id=RMQ_CONN_ID,
        filter_headers={"x-type": "order"},
        deferrable=True,
        poke_interval=5,
        timeout=120,
    )

    @task
    def process_message(message: dict) -> dict:
        """Log and process the message received by the sensor."""
        log.info("Sensor matched message: %s", message)
        return message

    cleanup = RMQQueueManagementOperator(
        task_id="cleanup_queue",
        action="delete_queue",
        queue_name=QUEUE_NAME,
        rmq_conn_id=RMQ_CONN_ID,
    )

    setup >> publish_noise >> publish_order >> wait_for_order
    processed = process_message(wait_for_order.output)
    processed >> cleanup


rmq_sensor_deferrable()
