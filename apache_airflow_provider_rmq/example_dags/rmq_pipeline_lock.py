"""Example DAG 5: Pipeline lock pattern using RabbitMQ.

Demonstrates using a RabbitMQ queue as a distributed lock to prevent
concurrent pipeline executions. The queue holds a "running" message
while the pipeline is active. If a new run is triggered while the
previous one is still running, it detects the lock and exits early.

This pattern involves two DAGs:
- rmq_pipeline_start: checks lock, publishes lock message, runs pipeline
- rmq_pipeline_finish: consumes lock message to release the lock

Uses the TaskFlow API (@dag / @task decorators).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime

from airflow.decorators import dag, task

from apache_airflow_provider_rmq.hooks.rmq import RMQHook
from apache_airflow_provider_rmq.operators.rmq_consume import RMQConsumeOperator
from apache_airflow_provider_rmq.operators.rmq_management import RMQQueueManagementOperator
from apache_airflow_provider_rmq.operators.rmq_publish import RMQPublishOperator

log = logging.getLogger("airflow.task")

RMQ_CONN_ID = "rmq_default"
LOCK_QUEUE = "pipeline_lock"


# --- Start DAG ---

@dag(
    dag_id="rmq_pipeline_start",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "rmq", "pipeline-lock"],
    doc_md="""
    ### Pipeline Lock Pattern
    Uses a RabbitMQ queue as a distributed lock:
    1. Check if lock queue has a message (pipeline is running)
    2. If empty: publish lock message and proceed
    3. If occupied: notify and skip
    """,
)
def rmq_pipeline_start():
    ensure_queue = RMQQueueManagementOperator(
        task_id="ensure_lock_queue",
        action="declare_queue",
        queue_name=LOCK_QUEUE,
        durable=True,
        rmq_conn_id=RMQ_CONN_ID,
    )

    @task.branch
    def check_lock() -> str:
        """Check if pipeline is already running by inspecting the lock queue."""
        hook = RMQHook(rmq_conn_id=RMQ_CONN_ID)
        info = hook.queue_info(LOCK_QUEUE)
        if info.get("exists") and info.get("message_count", 0) > 0:
            return "pipeline_already_running"
        return "acquire_lock"

    acquire_lock = RMQPublishOperator(
        task_id="acquire_lock",
        rmq_conn_id=RMQ_CONN_ID,
        queue_name=LOCK_QUEUE,
        message=json.dumps({
            "status": "running",
            "started_at": "{{ ts }}",
            "dag_run_id": "{{ run_id }}",
        }),
        delivery_mode=2,
    )

    @task
    def pipeline_already_running():
        """Notify that pipeline is already running."""
        log.info("Pipeline is already running. Skipping this execution.")

    @task
    def do_pipeline_work():
        """Simulate pipeline work."""
        log.info("Running pipeline tasks...")

    branch = check_lock()
    ensure_queue >> branch
    branch >> acquire_lock >> do_pipeline_work()
    branch >> pipeline_already_running()


rmq_pipeline_start()


# --- Finish DAG ---

@dag(
    dag_id="rmq_pipeline_finish",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "rmq", "pipeline-lock"],
    doc_md="""
    ### Pipeline Lock Release
    Consumes the lock message from the queue, signaling pipeline completion.
    Should be triggered as the last step of the pipeline.
    """,
)
def rmq_pipeline_finish():
    release_lock = RMQConsumeOperator(
        task_id="release_lock",
        queue_name=LOCK_QUEUE,
        rmq_conn_id=RMQ_CONN_ID,
        max_messages=1,
    )

    @task
    def pipeline_complete(lock_messages: list[dict]):
        """Log lock info and confirm pipeline completion."""
        for msg in lock_messages:
            log.info("Released lock: %s", msg["body"])
        log.info("Pipeline finished. Lock released.")

    pipeline_complete(release_lock.output)


rmq_pipeline_finish()
