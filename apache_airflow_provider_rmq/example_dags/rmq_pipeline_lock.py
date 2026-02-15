"""Example DAG 5: Pipeline lock pattern using RabbitMQ.

Demonstrates using a RabbitMQ queue as a distributed lock to prevent
concurrent pipeline executions. The queue holds a "running" message
while the pipeline is active. If a new run is triggered while the
previous one is still running, it detects the lock and exits early.

This pattern involves two DAGs:
- rmq_pipeline_start: checks lock, publishes lock message, runs pipeline
- rmq_pipeline_finish: consumes lock message to release the lock

This file shows the "start" DAG. The "finish" DAG would be a separate
DAG that runs at the end of the pipeline.
"""

from __future__ import annotations

import json
from datetime import datetime

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator

from apache_airflow_provider_rmq.hooks.rmq import RMQHook
from apache_airflow_provider_rmq.operators.rmq_consume import RMQConsumeOperator
from apache_airflow_provider_rmq.operators.rmq_management import RMQQueueManagementOperator
from apache_airflow_provider_rmq.operators.rmq_publish import RMQPublishOperator

RMQ_CONN_ID = "rmq_default"
LOCK_QUEUE = "pipeline_lock"


def check_pipeline_lock(**context) -> str:
    """Check if pipeline is already running by inspecting the lock queue.

    Uses RMQHook.queue_info() to check message count without consuming.
    Returns the task_id of the next task to execute (branching).
    """
    hook = RMQHook(rmq_conn_id=RMQ_CONN_ID)
    info = hook.queue_info(LOCK_QUEUE)
    if info.get("exists") and info.get("message_count", 0) > 0:
        return "pipeline_already_running"
    return "acquire_lock"


def notify_already_running(**context):
    """Send a notification that pipeline is already running."""
    print("Pipeline is already running. Skipping this execution.")
    # In production: send webhook, Slack notification, etc.


# --- Start DAG ---

with DAG(
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
) as start_dag:

    ensure_queue = RMQQueueManagementOperator(
        task_id="ensure_lock_queue",
        action="declare_queue",
        queue_name=LOCK_QUEUE,
        durable=True,
        rmq_conn_id=RMQ_CONN_ID,
    )

    check_lock = BranchPythonOperator(
        task_id="check_lock",
        python_callable=check_pipeline_lock,
    )

    acquire_lock = RMQPublishOperator(
        task_id="acquire_lock",
        rmq_conn_id=RMQ_CONN_ID,
        queue_name=LOCK_QUEUE,
        message=json.dumps({
            "status": "running",
            "started_at": "{{ ts }}",
            "dag_run_id": "{{ run_id }}",
        }),
        delivery_mode=2,  # persistent
    )

    already_running = PythonOperator(
        task_id="pipeline_already_running",
        python_callable=notify_already_running,
    )

    do_work = PythonOperator(
        task_id="do_pipeline_work",
        python_callable=lambda: print("Running pipeline tasks..."),
    )

    ensure_queue >> check_lock
    check_lock >> acquire_lock >> do_work
    check_lock >> already_running


# --- Finish DAG (separate DAG that runs at the end of the pipeline) ---

with DAG(
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
) as finish_dag:

    release_lock = RMQConsumeOperator(
        task_id="release_lock",
        queue_name=LOCK_QUEUE,
        rmq_conn_id=RMQ_CONN_ID,
        max_messages=1,
    )

    done = PythonOperator(
        task_id="pipeline_complete",
        python_callable=lambda: print("Pipeline finished. Lock released."),
    )

    release_lock >> done
