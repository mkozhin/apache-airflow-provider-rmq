"""Example DAG 6: Dead Letter Queue (DLQ) setup and exchange management.

Demonstrates:
- Creating a main queue with DLQ arguments using RMQHook.build_dlq_arguments()
- Setting up exchange-to-exchange bindings
- Queue purging
- All management actions in one DAG

Uses the TaskFlow API (@dag / @task decorators).
"""

from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import dag, task

from airflow_provider_rmq.hooks.rmq import RMQHook
from airflow_provider_rmq.operators.rmq_management import RMQQueueManagementOperator
from airflow_provider_rmq.operators.rmq_publish import RMQPublishOperator

log = logging.getLogger("airflow.task")

RMQ_CONN_ID = "rmq_default"
MAIN_EXCHANGE = "example_main_exchange"
DLX_EXCHANGE = "example_dlx"
MAIN_QUEUE = "example_main_with_dlq"
DLQ_QUEUE = "example_dead_letters"


@dag(
    dag_id="rmq_dlq_setup",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "rmq", "dlq"],
    doc_md="""
    ### Dead Letter Queue Example
    Sets up a complete DLQ infrastructure:
    1. Main exchange + DLX exchange
    2. Main queue with TTL and DLQ routing
    3. Dead letter queue bound to DLX
    4. Publishes a test message
    5. Purges and cleans up
    """,
)
def rmq_dlq_setup():
    # Create exchanges
    create_main_exchange = RMQQueueManagementOperator(
        task_id="create_main_exchange",
        action="declare_exchange",
        exchange_name=MAIN_EXCHANGE,
        exchange_type="direct",
        durable=True,
        rmq_conn_id=RMQ_CONN_ID,
    )

    create_dlx = RMQQueueManagementOperator(
        task_id="create_dlx_exchange",
        action="declare_exchange",
        exchange_name=DLX_EXCHANGE,
        exchange_type="direct",
        durable=True,
        rmq_conn_id=RMQ_CONN_ID,
    )

    # Create DLQ queue and bind to DLX
    create_dlq = RMQQueueManagementOperator(
        task_id="create_dlq_queue",
        action="declare_queue",
        queue_name=DLQ_QUEUE,
        durable=True,
        rmq_conn_id=RMQ_CONN_ID,
    )

    bind_dlq = RMQQueueManagementOperator(
        task_id="bind_dlq_to_dlx",
        action="bind_queue",
        queue_name=DLQ_QUEUE,
        exchange_name=DLX_EXCHANGE,
        routing_key="dead",
        rmq_conn_id=RMQ_CONN_ID,
    )

    @task
    def create_main_queue_with_dlq():
        """Create a queue with Dead Letter Queue arguments using the hook directly."""
        hook = RMQHook(rmq_conn_id=RMQ_CONN_ID)
        dlq_args = RMQHook.build_dlq_arguments(
            dlx_exchange=DLX_EXCHANGE,
            dlx_routing_key="dead",
            message_ttl=30000,
        )
        hook.queue_declare(
            queue_name=MAIN_QUEUE,
            durable=True,
            arguments=dlq_args,
        )
        log.info("Created queue '%s' with DLQ args: %s", MAIN_QUEUE, dlq_args)

    bind_main = RMQQueueManagementOperator(
        task_id="bind_main_queue",
        action="bind_queue",
        queue_name=MAIN_QUEUE,
        exchange_name=MAIN_EXCHANGE,
        routing_key="events",
        rmq_conn_id=RMQ_CONN_ID,
    )

    # Publish a test message
    publish = RMQPublishOperator(
        task_id="publish_test_message",
        rmq_conn_id=RMQ_CONN_ID,
        exchange=MAIN_EXCHANGE,
        routing_key="events",
        message={"test": "dlq_message"},
        delivery_mode=2,
        expiration="5000",
    )

    # Cleanup
    purge_main = RMQQueueManagementOperator(
        task_id="purge_main",
        action="purge_queue",
        queue_name=MAIN_QUEUE,
        rmq_conn_id=RMQ_CONN_ID,
    )

    purge_dlq = RMQQueueManagementOperator(
        task_id="purge_dlq",
        action="purge_queue",
        queue_name=DLQ_QUEUE,
        rmq_conn_id=RMQ_CONN_ID,
    )

    delete_main_queue = RMQQueueManagementOperator(
        task_id="delete_main_queue",
        action="delete_queue",
        queue_name=MAIN_QUEUE,
        rmq_conn_id=RMQ_CONN_ID,
    )

    delete_dlq_queue = RMQQueueManagementOperator(
        task_id="delete_dlq_queue",
        action="delete_queue",
        queue_name=DLQ_QUEUE,
        rmq_conn_id=RMQ_CONN_ID,
    )

    delete_main_ex = RMQQueueManagementOperator(
        task_id="delete_main_exchange",
        action="delete_exchange",
        exchange_name=MAIN_EXCHANGE,
        rmq_conn_id=RMQ_CONN_ID,
    )

    delete_dlx_ex = RMQQueueManagementOperator(
        task_id="delete_dlx_exchange",
        action="delete_exchange",
        exchange_name=DLX_EXCHANGE,
        rmq_conn_id=RMQ_CONN_ID,
    )

    # Dependencies
    setup_main = create_main_queue_with_dlq()

    [create_main_exchange, create_dlx] >> create_dlq >> bind_dlq >> setup_main
    setup_main >> bind_main >> publish
    publish >> [purge_main, purge_dlq]
    purge_main >> delete_main_queue
    purge_dlq >> delete_dlq_queue
    [delete_main_queue, delete_dlq_queue] >> [delete_main_ex, delete_dlx_ex]


rmq_dlq_setup()
