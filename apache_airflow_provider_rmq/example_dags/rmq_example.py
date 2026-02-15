"""Example DAG demonstrating RabbitMQ provider operators and sensor."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from apache_airflow_provider_rmq.operators.rmq_management import RMQQueueManagementOperator
from apache_airflow_provider_rmq.operators.rmq_publish import RMQPublishOperator
from apache_airflow_provider_rmq.operators.rmq_consume import RMQConsumeOperator
from apache_airflow_provider_rmq.sensors.rmq import RMQSensor

RMQ_CONN_ID = "rmq_default"
QUEUE_NAME = "example_queue"

with DAG(
    dag_id="rmq_example",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "rmq"],
) as dag:
    create_queue = RMQQueueManagementOperator(
        task_id="create_queue",
        action="declare_queue",
        queue_name=QUEUE_NAME,
        durable=True,
        rmq_conn_id=RMQ_CONN_ID,
    )

    publish_message = RMQPublishOperator(
        task_id="publish_message",
        rmq_conn_id=RMQ_CONN_ID,
        routing_key=QUEUE_NAME,
        message={"hello": "world"},
    )

    wait_for_message = RMQSensor(
        task_id="wait_for_message",
        queue_name=QUEUE_NAME,
        rmq_conn_id=RMQ_CONN_ID,
        poke_interval=5,
        timeout=60,
    )

    consume_messages = RMQConsumeOperator(
        task_id="consume_messages",
        queue_name=QUEUE_NAME,
        rmq_conn_id=RMQ_CONN_ID,
        max_messages=10,
    )

    cleanup_queue = RMQQueueManagementOperator(
        task_id="cleanup_queue",
        action="delete_queue",
        queue_name=QUEUE_NAME,
        rmq_conn_id=RMQ_CONN_ID,
    )

    create_queue >> publish_message >> wait_for_message >> consume_messages >> cleanup_queue
