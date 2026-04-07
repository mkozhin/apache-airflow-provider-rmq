# Changelog

## v1.2.1

- **Fixed:** `arguments` added to `template_fields` in `RMQQueueManagementOperator` — `x-*` arguments (e.g., `alternate-exchange`, DLQ settings) now support Jinja templates and XCom

## v1.2.0

- **Added:** `message_wait_timeout` added to `template_fields` in `RMQSensor` — the parameter now supports Jinja templates and XCom, enabling dynamic timeouts computed at runtime (e.g., remaining seconds until end of business hours)
- **Changed:** `RMQSensor` timeout behaviour — when `message_wait_timeout` expires, `AirflowSkipException` is now raised instead of `RuntimeError`. The task is marked **SKIPPED** rather than FAILED, and `on_failure_callback` is not triggered. This is a **breaking change** for code that caught `RuntimeError` on timeout

## v1.1.0

- **Added:** `mode="push"` in `RMQSensor` and `RMQTrigger` — deferrable sensor now supports push delivery via AMQP `basic_consume`. The broker delivers messages instantly as they arrive, eliminating polling delay. Default remains `mode="pull"` (backwards compatible)
- **Added:** `message_wait_timeout` parameter in `RMQSensor` and `RMQTrigger` — optional client-side timeout (seconds) for push mode. Sensor raises `RuntimeError` on expiry. Only valid with `mode="push"`
- **Added:** Example DAG `docs/example_dags/rmq_sensor_push.py` demonstrating push mode with filtering and timeout
- **Changed:** Example DAGs moved from `airflow_provider_rmq/example_dags/` to `docs/example_dags/` (no longer shipped inside the pip package)
- **Changed:** Version is now derived from git tags via `setuptools-scm` — no hardcoded version in source
- **Changed:** CI workflow now runs tests on Python 3.10/3.11/3.12 before publishing; triggers on `v*` tag push

## v1.0.1

- **Fixed:** SSL context and URL encoding improvements in `RMQTrigger`
- **Fixed:** Connection stability improvements in `RMQHook` (auto-reconnect, GC cleanup)

## v1.0.0

- **Added:** `RMQHook` — synchronous AMQP hook with retry logic, SSL/TLS support, queue/exchange management
- **Added:** `RMQPublishOperator` — publish messages to RabbitMQ exchanges or queues
- **Added:** `RMQConsumeOperator` — consume messages with dict and callable filter support
- **Added:** `RMQQueueManagementOperator` — declare, delete, purge, bind queues and exchanges
- **Added:** `RMQSensor` — wait for a matching message in poke and deferrable modes
- **Added:** `RMQTrigger` — async trigger using aio-pika for deferrable sensor support
- **Added:** `MessageFilter` — dict-based and callable-based message filtering
- **Added:** SSL/TLS configuration via Airflow connection extras
