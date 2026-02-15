"""Example DAG 2: Advanced publishing with all message properties.

Demonstrates RMQPublishOperator with every available parameter:
headers, priority, delivery_mode, expiration, correlation_id, etc.
Also shows publishing multiple messages and using exchange routing.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG

from apache_airflow_provider_rmq.operators.rmq_management import RMQQueueManagementOperator
from apache_airflow_provider_rmq.operators.rmq_publish import RMQPublishOperator

RMQ_CONN_ID = "rmq_default"
EXCHANGE_NAME = "example_events"
QUEUE_ORDERS = "example_orders"
QUEUE_INVOICES = "example_invoices"

with DAG(
    dag_id="rmq_publish_advanced",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "rmq", "advanced"],
    doc_md="""
    ### Advanced Publishing Example
    - Creates a topic exchange and two queues bound with different routing keys
    - Publishes messages with all available AMQP properties
    - Publishes a batch of messages at once
    - Cleans up resources
    """,
) as dag:

    # --- Infrastructure setup ---

    create_exchange = RMQQueueManagementOperator(
        task_id="create_exchange",
        action="declare_exchange",
        exchange_name=EXCHANGE_NAME,
        exchange_type="topic",
        durable=True,
        rmq_conn_id=RMQ_CONN_ID,
    )

    create_orders_queue = RMQQueueManagementOperator(
        task_id="create_orders_queue",
        action="declare_queue",
        queue_name=QUEUE_ORDERS,
        durable=True,
        rmq_conn_id=RMQ_CONN_ID,
    )

    create_invoices_queue = RMQQueueManagementOperator(
        task_id="create_invoices_queue",
        action="declare_queue",
        queue_name=QUEUE_INVOICES,
        durable=True,
        rmq_conn_id=RMQ_CONN_ID,
    )

    bind_orders = RMQQueueManagementOperator(
        task_id="bind_orders",
        action="bind_queue",
        queue_name=QUEUE_ORDERS,
        exchange_name=EXCHANGE_NAME,
        routing_key="events.orders.*",
        rmq_conn_id=RMQ_CONN_ID,
    )

    bind_invoices = RMQQueueManagementOperator(
        task_id="bind_invoices",
        action="bind_queue",
        queue_name=QUEUE_INVOICES,
        exchange_name=EXCHANGE_NAME,
        routing_key="events.invoices.*",
        rmq_conn_id=RMQ_CONN_ID,
    )

    # --- Publish with all properties ---

    publish_order = RMQPublishOperator(
        task_id="publish_order_with_all_props",
        rmq_conn_id=RMQ_CONN_ID,
        exchange=EXCHANGE_NAME,
        routing_key="events.orders.created",
        message={"order_id": 123, "amount": 99.99, "currency": "USD"},
        content_type="application/json",
        delivery_mode=2,  # persistent
        headers={"x-source": "airflow", "x-type": "order", "x-priority": "high"},
        priority=5,
        expiration="60000",  # TTL 60 seconds
        correlation_id="corr-abc-123",
        reply_to="reply_queue",
        message_id="msg-order-123",
    )

    # --- Publish a batch of messages ---

    publish_batch = RMQPublishOperator(
        task_id="publish_invoice_batch",
        rmq_conn_id=RMQ_CONN_ID,
        exchange=EXCHANGE_NAME,
        routing_key="events.invoices.created",
        message=[
            {"invoice_id": 1, "total": 100.0},
            {"invoice_id": 2, "total": 250.0},
            {"invoice_id": 3, "total": 75.50},
        ],
        delivery_mode=2,
        headers={"x-source": "airflow", "x-type": "invoice"},
    )

    # --- Publish directly to queue (shortcut) ---

    publish_direct = RMQPublishOperator(
        task_id="publish_direct_to_queue",
        rmq_conn_id=RMQ_CONN_ID,
        queue_name=QUEUE_ORDERS,  # shortcut: sets exchange="" and routing_key=queue_name
        message="plain text message body",
    )

    # --- Cleanup ---

    unbind_orders = RMQQueueManagementOperator(
        task_id="unbind_orders",
        action="unbind_queue",
        queue_name=QUEUE_ORDERS,
        exchange_name=EXCHANGE_NAME,
        routing_key="events.orders.*",
        rmq_conn_id=RMQ_CONN_ID,
    )

    unbind_invoices = RMQQueueManagementOperator(
        task_id="unbind_invoices",
        action="unbind_queue",
        queue_name=QUEUE_INVOICES,
        exchange_name=EXCHANGE_NAME,
        routing_key="events.invoices.*",
        rmq_conn_id=RMQ_CONN_ID,
    )

    delete_orders = RMQQueueManagementOperator(
        task_id="delete_orders_queue",
        action="delete_queue",
        queue_name=QUEUE_ORDERS,
        rmq_conn_id=RMQ_CONN_ID,
    )

    delete_invoices = RMQQueueManagementOperator(
        task_id="delete_invoices_queue",
        action="delete_queue",
        queue_name=QUEUE_INVOICES,
        rmq_conn_id=RMQ_CONN_ID,
    )

    delete_exchange = RMQQueueManagementOperator(
        task_id="delete_exchange",
        action="delete_exchange",
        exchange_name=EXCHANGE_NAME,
        rmq_conn_id=RMQ_CONN_ID,
    )

    # --- Dependencies ---

    create_exchange >> [create_orders_queue, create_invoices_queue]
    create_orders_queue >> bind_orders
    create_invoices_queue >> bind_invoices

    [bind_orders, bind_invoices] >> publish_order >> publish_batch >> publish_direct

    publish_direct >> [unbind_orders, unbind_invoices]
    unbind_orders >> delete_orders
    unbind_invoices >> delete_invoices
    [delete_orders, delete_invoices] >> delete_exchange
