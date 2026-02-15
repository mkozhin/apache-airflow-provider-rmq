"""Example DAG 3: Consuming with header and body filters.

Demonstrates RMQConsumeOperator with:
- Header-based filtering (filter_headers)
- Custom callable filtering (filter_callable)
- QoS configuration (prefetch_count)
- Body-path filtering via dict syntax (body.field.nested)
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG

from apache_airflow_provider_rmq.operators.rmq_consume import RMQConsumeOperator
from apache_airflow_provider_rmq.operators.rmq_management import RMQQueueManagementOperator
from apache_airflow_provider_rmq.operators.rmq_publish import RMQPublishOperator

RMQ_CONN_ID = "rmq_default"
QUEUE_NAME = "example_filtered"


def amount_above_threshold(properties, body: str) -> bool:
    """Custom filter: only accept messages with amount > 100."""
    import json

    try:
        data = json.loads(body)
        return data.get("amount", 0) > 100
    except (json.JSONDecodeError, TypeError):
        return False


with DAG(
    dag_id="rmq_consume_with_filters",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "rmq", "filters"],
    doc_md="""
    ### Filtering Example
    Publishes several messages with different headers and body content,
    then consumes only those that match specific criteria.
    """,
) as dag:
    setup_queue = RMQQueueManagementOperator(
        task_id="setup_queue",
        action="declare_queue",
        queue_name=QUEUE_NAME,
        durable=True,
        rmq_conn_id=RMQ_CONN_ID,
    )

    # Publish messages with different headers
    publish_order = RMQPublishOperator(
        task_id="publish_order",
        rmq_conn_id=RMQ_CONN_ID,
        queue_name=QUEUE_NAME,
        message={"type": "order", "amount": 250.0},
        headers={"x-type": "order", "x-region": "eu"},
    )

    publish_invoice = RMQPublishOperator(
        task_id="publish_invoice",
        rmq_conn_id=RMQ_CONN_ID,
        queue_name=QUEUE_NAME,
        message={"type": "invoice", "amount": 50.0},
        headers={"x-type": "invoice", "x-region": "us"},
    )

    publish_small_order = RMQPublishOperator(
        task_id="publish_small_order",
        rmq_conn_id=RMQ_CONN_ID,
        queue_name=QUEUE_NAME,
        message={"type": "order", "amount": 10.0},
        headers={"x-type": "order", "x-region": "us"},
    )

    # Consume only "order" type messages (header filter)
    consume_orders_only = RMQConsumeOperator(
        task_id="consume_orders_by_header",
        queue_name=QUEUE_NAME,
        rmq_conn_id=RMQ_CONN_ID,
        max_messages=10,
        filter_headers={"x-type": "order"},
    )

    # Consume with body-path filter (body.type == "invoice")
    consume_invoices_by_body = RMQConsumeOperator(
        task_id="consume_invoices_by_body",
        queue_name=QUEUE_NAME,
        rmq_conn_id=RMQ_CONN_ID,
        max_messages=10,
        filter_headers={"body.type": "invoice"},
    )

    # Consume with callable filter and QoS
    consume_high_value = RMQConsumeOperator(
        task_id="consume_high_value_orders",
        queue_name=QUEUE_NAME,
        rmq_conn_id=RMQ_CONN_ID,
        max_messages=50,
        filter_callable=amount_above_threshold,
        qos={"prefetch_count": 10},
    )

    purge = RMQQueueManagementOperator(
        task_id="purge_remaining",
        action="purge_queue",
        queue_name=QUEUE_NAME,
        rmq_conn_id=RMQ_CONN_ID,
    )

    cleanup = RMQQueueManagementOperator(
        task_id="cleanup_queue",
        action="delete_queue",
        queue_name=QUEUE_NAME,
        rmq_conn_id=RMQ_CONN_ID,
    )

    setup_queue >> [publish_order, publish_invoice, publish_small_order]
    [publish_order, publish_invoice, publish_small_order] >> consume_orders_only
    consume_orders_only >> consume_invoices_by_body >> consume_high_value >> purge >> cleanup
