"""Example DAG 3: Consuming with header and body filters.

Demonstrates RMQConsumeOperator with:
- Header-based filtering (filter_headers)
- Custom callable filtering (filter_callable)
- QoS configuration (prefetch_count)
- Body-path filtering via dict syntax (body.field.nested)
- Processing consumed messages in downstream @task functions

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

log = logging.getLogger("airflow.task")

RMQ_CONN_ID = "rmq_default"
QUEUE_NAME = "example_filtered"


def amount_above_threshold(properties, body: str) -> bool:
    """Custom filter: only accept messages with amount > 100."""
    try:
        data = json.loads(body)
        return data.get("amount", 0) > 100
    except (json.JSONDecodeError, TypeError):
        return False


@dag(
    dag_id="rmq_consume_with_filters",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "rmq", "filters"],
    doc_md="""
    ### Filtering Example
    Publishes several messages with different headers and body content,
    then consumes only those that match specific criteria.
    Each consume step logs the matched messages and passes them
    to a downstream processing task.
    """,
)
def rmq_consume_with_filters():
    setup_queue = RMQQueueManagementOperator(
        task_id="setup_queue",
        action="declare_queue",
        queue_name=QUEUE_NAME,
        durable=True,
        rmq_conn_id=RMQ_CONN_ID,
    )

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

    @task
    def process_orders(messages: list[dict]) -> list[dict]:
        """Log each order message and extract order data."""
        orders = []
        for msg in messages:
            body = json.loads(msg["body"]) if isinstance(msg["body"], str) else msg["body"]
            log.info(
                "Order received: amount=%.2f, region=%s, body=%s",
                body.get("amount", 0),
                msg.get("headers", {}).get("x-region", "unknown"),
                msg["body"],
            )
            orders.append(body)
        log.info("Total orders consumed: %d", len(orders))
        return orders

    # Consume with body-path filter (body.type == "invoice")
    consume_invoices_by_body = RMQConsumeOperator(
        task_id="consume_invoices_by_body",
        queue_name=QUEUE_NAME,
        rmq_conn_id=RMQ_CONN_ID,
        max_messages=10,
        filter_headers={"body.type": "invoice"},
    )

    @task
    def process_invoices(messages: list[dict]) -> list[dict]:
        """Log each invoice message and compute totals."""
        invoices = []
        total = 0.0
        for msg in messages:
            body = json.loads(msg["body"]) if isinstance(msg["body"], str) else msg["body"]
            amount = body.get("amount", 0)
            total += amount
            log.info("Invoice: amount=%.2f, body=%s", amount, msg["body"])
            invoices.append(body)
        log.info("Total invoices: %d, combined amount: %.2f", len(invoices), total)
        return invoices

    # Consume with callable filter and QoS
    consume_high_value = RMQConsumeOperator(
        task_id="consume_high_value_orders",
        queue_name=QUEUE_NAME,
        rmq_conn_id=RMQ_CONN_ID,
        max_messages=50,
        filter_callable=amount_above_threshold,
        qos={"prefetch_count": 10},
    )

    @task
    def process_high_value(messages: list[dict]) -> list[dict]:
        """Log high-value messages (amount > 100)."""
        items = []
        for msg in messages:
            body = json.loads(msg["body"]) if isinstance(msg["body"], str) else msg["body"]
            log.info("High-value message: amount=%.2f, body=%s", body.get("amount", 0), msg["body"])
            items.append(body)
        log.info("High-value messages total: %d", len(items))
        return items

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

    # Wire up the DAG
    setup_queue >> [publish_order, publish_invoice, publish_small_order]
    [publish_order, publish_invoice, publish_small_order] >> consume_orders_only

    orders = process_orders(consume_orders_only.output)
    orders >> consume_invoices_by_body

    invoices = process_invoices(consume_invoices_by_body.output)
    invoices >> consume_high_value

    high_value = process_high_value(consume_high_value.output)
    high_value >> purge >> cleanup


rmq_consume_with_filters()