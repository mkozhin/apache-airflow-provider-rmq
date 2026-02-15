<h1 align="center">
  Apache Airflow Provider for RabbitMQ
</h1>
<h3 align="center">
  Hooks, operators, sensors, and deferrable triggers for RabbitMQ integration in Apache Airflow.
</h3>

<p align="center">
  <a href="#installation">Installation</a> &bull;
  <a href="#connection-setup">Connection</a> &bull;
  <a href="#components">Components</a> &bull;
  <a href="#example-dags">Examples</a> &bull;
  <a href="#contributing">Contributing</a>
</p>

---

> **AI Disclosure:** This provider was developed with the assistance of **Claude Code** (Anthropic, model **Claude Opus 4.6**). The code, tests, and documentation were co-authored by a human developer and an LLM. Please evaluate the code quality on its own merits and make informed decisions about whether to use it in your projects.

---

## Overview

`apache-airflow-provider-rmq` is a community provider package that enables Apache Airflow to interact with RabbitMQ. It supports:

- Publishing messages to exchanges and queues
- Consuming messages with header-based and callable-based filtering
- Waiting for specific messages with sensors (classic poke and deferrable mode)
- Full queue and exchange management (declare, delete, purge, bind, unbind)
- SSL/TLS connections
- Dead Letter Queue (DLQ) setup helpers
- QoS configuration (prefetch)

### Requirements

| Dependency | Version |
|---|---|
| Apache Airflow | `>=2.7.0, <3.0.0` |
| pika | `>=1.3.0, <2.0.0` |
| aio-pika | `>=9.0.0, <10.0.0` |
| tenacity | `>=8.0.0` |
| Python | `>=3.10` |

---

## Installation

### Install from PyPI

```bash
pip install apache-airflow-provider-rmq
```

### Building from source

```bash
git clone https://github.com/your-org/apache-airflow-provider-rmq.git
cd apache-airflow-provider-rmq
pip install build
python -m build
pip install dist/apache_airflow_provider_rmq-*.whl
```

---

## Connection Setup

Create a new connection in the Airflow UI (**Admin > Connections**) with:

| Field | Value | Description |
|---|---|---|
| Connection Id | `rmq_default` | Any unique ID |
| Connection Type | `AMQP` | Registered by the provider |
| Host | `localhost` | RabbitMQ server hostname |
| Port | `5672` | `5671` for SSL |
| Login | `guest` | RabbitMQ username |
| Password | `guest` | RabbitMQ password |
| Schema | `/` | Virtual host |

### SSL/TLS Configuration

Add SSL settings in the **Extra** field as JSON:

```json
{
  "ssl_enabled": true,
  "ca_certs": "/path/to/ca.pem",
  "certfile": "/path/to/client-cert.pem",
  "keyfile": "/path/to/client-key.pem",
  "cert_reqs": "CERT_REQUIRED"
}
```

The hook also provides custom form widgets for SSL fields (`ssl_enabled`, `ca_certs`, `certfile`, `keyfile`) visible in the Airflow connection form.

Set `"cert_reqs": "CERT_NONE"` to disable certificate verification (not recommended for production).

---

## Components

### RMQHook

**Import:** `from apache_airflow_provider_rmq.hooks.rmq import RMQHook`

Core hook for all RabbitMQ interactions. Uses pika `BlockingConnection` with automatic retry logic (tenacity). The connection is closed automatically when the hook object is garbage-collected, so you do not need to call `close()` manually. Context manager (`with`) is also supported.

#### Constructor Parameters

| Parameter | Type | Default | Required | Description |
|---|---|---|---|---|
| `rmq_conn_id` | `str` | `"rmq_default"` | No | Airflow connection ID |
| `vhost` | `str \| None` | `None` | No | Override virtual host from connection |
| `qos` | `dict \| None` | `None` | No | QoS settings: `prefetch_size`, `prefetch_count`, `global_qos` |
| `retry_count` | `int` | `3` | No | Number of connection retry attempts |
| `retry_delay` | `float` | `1.0` | No | Base delay (seconds) between retries (exponential backoff) |

#### Key Methods

| Method | Description |
|---|---|
| `get_channel()` | Returns a pika `BlockingChannel` (creates connection lazily) |
| `queue_declare(queue_name, passive, durable, exclusive, auto_delete, arguments)` | Declare a queue |
| `queue_delete(queue_name, if_unused, if_empty)` | Delete a queue |
| `queue_bind(queue, exchange, routing_key, arguments)` | Bind a queue to an exchange |
| `queue_unbind(queue, exchange, routing_key, arguments)` | Unbind a queue from an exchange |
| `queue_purge(queue_name)` | Remove all messages from a queue |
| `queue_info(queue_name)` | Get queue info (message_count, consumer_count, exists) via passive declare |
| `exchange_declare(exchange, exchange_type, passive, durable, auto_delete, internal, arguments)` | Declare an exchange |
| `exchange_delete(exchange, if_unused)` | Delete an exchange |
| `exchange_bind(destination, source, routing_key, arguments)` | Bind exchange to exchange |
| `exchange_unbind(destination, source, routing_key, arguments)` | Unbind exchange from exchange |
| `basic_publish(exchange, routing_key, body, properties)` | Publish a message |
| `consume_messages(queue_name, max_messages, auto_ack, inactivity_timeout)` | Consume messages from a queue |
| `ack(delivery_tag)` | Acknowledge a message |
| `nack(delivery_tag, requeue)` | Negatively acknowledge a message |
| `build_dlq_arguments(dlx_exchange, dlx_routing_key, message_ttl)` | Static method: build `x-*` args for DLQ support |
| `test_connection()` | Test the connection (used by Airflow UI) |
| `close()` | Close channel and connection |

#### Usage Example

```python
from apache_airflow_provider_rmq.hooks.rmq import RMQHook

hook = RMQHook(rmq_conn_id="rmq_default")
info = hook.queue_info("my_queue")
print(f"Messages in queue: {info['message_count']}")

hook.basic_publish(
    exchange="",
    routing_key="my_queue",
    body='{"key": "value"}',
)
# Connection is closed automatically when hook goes out of scope
```

---

### RMQPublishOperator

**Import:** `from apache_airflow_provider_rmq.operators.rmq_publish import RMQPublishOperator`

Publishes one or more messages to RabbitMQ. Supports strings, dicts (auto-serialized to JSON), and lists.

#### Parameters

| Parameter | Type | Default | Required | Description |
|---|---|---|---|---|
| `rmq_conn_id` | `str` | `"rmq_default"` | No | Airflow connection ID |
| `exchange` | `str` | `""` | No | Exchange to publish to (empty = default exchange) |
| `routing_key` | `str` | `""` | No | Routing key for the message |
| `message` | `str \| list[str] \| dict \| list[dict] \| None` | `None` | No | Message payload. Dicts are JSON-serialized |
| `queue_name` | `str \| None` | `None` | No | Shortcut: sets `exchange=""` and `routing_key=queue_name` |
| `content_type` | `str \| None` | `None` | No | AMQP content type header (e.g., `"application/json"`) |
| `delivery_mode` | `int \| None` | `None` | No | `1` = non-persistent, `2` = persistent |
| `headers` | `dict \| None` | `None` | No | Custom AMQP headers |
| `priority` | `int \| None` | `None` | No | Message priority (0-9) |
| `expiration` | `str \| None` | `None` | No | Per-message TTL in milliseconds (as string, e.g., `"60000"`) |
| `correlation_id` | `str \| None` | `None` | No | Application correlation identifier |
| `reply_to` | `str \| None` | `None` | No | Reply-to queue name |
| `message_id` | `str \| None` | `None` | No | Application message identifier |

**Template fields:** `exchange`, `routing_key`, `message`

#### Usage Example

```python
# Publish a single dict to a queue
RMQPublishOperator(
    task_id="publish",
    queue_name="my_queue",
    message={"event": "order_created", "id": 42},
    delivery_mode=2,
    headers={"x-source": "airflow"},
)

# Publish a batch of messages to an exchange
RMQPublishOperator(
    task_id="publish_batch",
    exchange="events",
    routing_key="orders.new",
    message=[
        {"id": 1, "item": "widget"},
        {"id": 2, "item": "gadget"},
    ],
)
```

---

### RMQConsumeOperator

**Import:** `from apache_airflow_provider_rmq.operators.rmq_consume import RMQConsumeOperator`

Consumes messages from a RabbitMQ queue. Matching messages are ACKed and returned via XCom. Non-matching messages are NACKed with `requeue=True`.

#### Parameters

| Parameter | Type | Default | Required | Description |
|---|---|---|---|---|
| `queue_name` | `str` | — | **Yes** | Name of the queue to consume from |
| `rmq_conn_id` | `str` | `"rmq_default"` | No | Airflow connection ID |
| `max_messages` | `int` | `100` | No | Maximum number of messages to consume per execution |
| `filter_headers` | `dict[str, Any] \| None` | `None` | No | Dict of AMQP headers that a message must match. Supports `body.*` keys for JSON body filtering (e.g., `{"body.data.status": "active"}`) |
| `filter_callable` | `Callable[[Any, str], bool] \| None` | `None` | No | Custom filter function `(properties, body_str) -> bool` |
| `qos` | `dict \| None` | `None` | No | QoS settings: `{"prefetch_count": 10}` |

**Template fields:** `queue_name`

**Returns:** `list[dict]` — list of matched messages, each with keys: `body`, `headers`, `routing_key`, `exchange`

#### Usage Example

```python
# Consume with header filter
RMQConsumeOperator(
    task_id="consume_orders",
    queue_name="orders",
    filter_headers={"x-type": "order"},
    max_messages=50,
    qos={"prefetch_count": 10},
)

# Consume with body-path filter
RMQConsumeOperator(
    task_id="consume_active",
    queue_name="events",
    filter_headers={"body.status": "active"},
)

# Consume with custom callable filter
def large_orders(properties, body: str) -> bool:
    import json
    data = json.loads(body)
    return data.get("amount", 0) > 1000

RMQConsumeOperator(
    task_id="consume_large",
    queue_name="orders",
    filter_callable=large_orders,
)
```

---

### RMQQueueManagementOperator

**Import:** `from apache_airflow_provider_rmq.operators.rmq_management import RMQQueueManagementOperator`

Performs queue and exchange management operations on RabbitMQ.

#### Parameters

| Parameter | Type | Default | Required | Description |
|---|---|---|---|---|
| `action` | `str` | — | **Yes** | Action to perform (see table below) |
| `rmq_conn_id` | `str` | `"rmq_default"` | No | Airflow connection ID |
| `queue_name` | `str \| None` | `None` | Conditional | Queue name (required for queue actions) |
| `durable` | `bool` | `False` | No | Resource survives broker restart |
| `exclusive` | `bool` | `False` | No | Queue is exclusive to this connection |
| `auto_delete` | `bool` | `False` | No | Resource is deleted when no longer in use |
| `exchange_name` | `str \| None` | `None` | Conditional | Exchange name (required for exchange actions) |
| `exchange_type` | `str` | `"direct"` | No | Exchange type: `direct`, `fanout`, `topic`, `headers` |
| `internal` | `bool` | `False` | No | Exchange cannot be published to directly |
| `if_unused` | `bool` | `False` | No | Only delete if resource has no consumers/bindings |
| `if_empty` | `bool` | `False` | No | Only delete queue if it is empty |
| `routing_key` | `str` | `""` | No | Routing key for bind/unbind actions |
| `arguments` | `dict \| None` | `None` | No | Optional `x-*` arguments (e.g., DLQ settings) |
| `source_exchange` | `str \| None` | `None` | Conditional | Source exchange for exchange bind/unbind |

**Template fields:** `queue_name`, `exchange_name`, `routing_key`

#### Supported Actions

| Action | Required Parameters | Description |
|---|---|---|
| `declare_queue` | `queue_name` | Create a queue |
| `delete_queue` | `queue_name` | Delete a queue |
| `purge_queue` | `queue_name` | Remove all messages from a queue |
| `bind_queue` | `queue_name`, `exchange_name` | Bind a queue to an exchange |
| `unbind_queue` | `queue_name`, `exchange_name` | Unbind a queue from an exchange |
| `declare_exchange` | `exchange_name` | Create an exchange |
| `delete_exchange` | `exchange_name` | Delete an exchange |
| `bind_exchange` | `exchange_name`, `source_exchange` | Bind exchange to exchange |
| `unbind_exchange` | `exchange_name`, `source_exchange` | Unbind exchange from exchange |

#### Usage Example

```python
# Create a durable queue
RMQQueueManagementOperator(
    task_id="create_queue",
    action="declare_queue",
    queue_name="my_queue",
    durable=True,
)

# Create a topic exchange and bind a queue
RMQQueueManagementOperator(
    task_id="create_exchange",
    action="declare_exchange",
    exchange_name="events",
    exchange_type="topic",
    durable=True,
)

RMQQueueManagementOperator(
    task_id="bind",
    action="bind_queue",
    queue_name="my_queue",
    exchange_name="events",
    routing_key="orders.*",
)
```

---

### RMQSensor

**Import:** `from apache_airflow_provider_rmq.sensors.rmq import RMQSensor`

Waits for a message in a RabbitMQ queue that matches optional filter conditions. Supports classic poke mode and deferrable mode.

#### Parameters

| Parameter | Type | Default | Required | Description |
|---|---|---|---|---|
| `queue_name` | `str` | — | **Yes** | Name of the queue to monitor |
| `rmq_conn_id` | `str` | `"rmq_default"` | No | Airflow connection ID |
| `filter_headers` | `dict[str, Any] \| None` | `None` | No | Dict-based header/body filter |
| `filter_callable` | `Callable \| None` | `None` | No | Custom filter function. **Not supported with `deferrable=True`** |
| `deferrable` | `bool` | `False` | No | Use deferrable mode (frees worker slot while waiting) |
| `poke_batch_size` | `int` | `100` | No | Max messages to fetch per poke cycle |
| `poke_interval` | `float` | `60` | No | Seconds between poke attempts (inherited from BaseSensorOperator) |
| `timeout` | `float` | `604800` | No | Max seconds to wait before failing (inherited from BaseSensorOperator) |
| `mode` | `str` | `"poke"` | No | `"poke"` or `"reschedule"` (inherited from BaseSensorOperator) |

**Template fields:** `queue_name`

**Returns:** `dict | None` — matched message with keys: `body`, `headers`, `routing_key`, `exchange`

#### Deferrable Mode

When `deferrable=True`, the sensor defers execution to the Airflow triggerer process using `RMQTrigger`. This frees the worker slot while waiting for a message, which is more resource-efficient for long waits.

**Limitation:** `filter_callable` cannot be used with `deferrable=True` because Python callables cannot be serialized to the triggerer process. Use `filter_headers` instead.

#### Usage Example

```python
# Classic poke mode with callable filter
RMQSensor(
    task_id="wait_for_order",
    queue_name="orders",
    filter_callable=lambda props, body: "urgent" in body,
    poke_interval=10,
    timeout=300,
    mode="reschedule",
)

# Deferrable mode with header filter
RMQSensor(
    task_id="wait_for_event",
    queue_name="events",
    filter_headers={"x-type": "payment"},
    deferrable=True,
    timeout=600,
)
```

---

### RMQTrigger

**Import:** `from apache_airflow_provider_rmq.triggers.rmq import RMQTrigger`

Async trigger for deferrable sensor mode. Uses `aio_pika` for non-blocking AMQP access. Typically not used directly — `RMQSensor` with `deferrable=True` creates it automatically.

#### Parameters

| Parameter | Type | Default | Required | Description |
|---|---|---|---|---|
| `rmq_conn_id` | `str` | — | **Yes** | Airflow connection ID |
| `queue_name` | `str` | — | **Yes** | Queue to poll |
| `filter_data` | `dict \| None` | `None` | No | Serialized filter from `MessageFilter.serialize()` |
| `poll_interval` | `float` | `5.0` | No | Seconds between polls when queue is empty |

---

### MessageFilter (Utility)

**Import:** `from apache_airflow_provider_rmq.utils.filters import MessageFilter`

Evaluates whether a RabbitMQ message matches given filter conditions. Used internally by operators and sensors.

#### Filter Modes

1. **Header filtering** (`filter_headers`): dict of key-value pairs that message headers must match.
   - Regular keys check `properties.headers` dict
   - Keys starting with `body.` traverse the JSON-parsed message body (e.g., `{"body.data.status": "active"}`)

2. **Callable filtering** (`filter_callable`): `fn(properties, body_str) -> bool`

Both can be combined (AND logic: both must pass).

---

## Example DAGs

The package includes several example DAGs in `apache_airflow_provider_rmq/example_dags/`:

| DAG | Description |
|---|---|
| `rmq_example_basic` | Simple publish-wait-consume-cleanup flow |
| `rmq_publish_advanced` | Advanced publishing with all AMQP properties, batch messages, topic exchange |
| `rmq_consume_with_filters` | Header filters, body-path filters, callable filters, QoS |
| `rmq_sensor_deferrable` | Deferrable sensor with header filtering |
| `rmq_pipeline_start` / `rmq_pipeline_finish` | Pipeline lock pattern — prevent concurrent executions |
| `rmq_dlq_setup` | Dead Letter Queue infrastructure setup with DLX, TTL, exchange-to-exchange bindings |

---

## Repository Structure

```
apache-airflow-provider-rmq/
├── apache_airflow_provider_rmq/
│   ├── __init__.py                  # Provider metadata & get_provider_info()
│   ├── hooks/
│   │   └── rmq.py                   # RMQHook
│   ├── operators/
│   │   ├── rmq_publish.py           # RMQPublishOperator
│   │   ├── rmq_consume.py           # RMQConsumeOperator
│   │   └── rmq_management.py        # RMQQueueManagementOperator
│   ├── sensors/
│   │   └── rmq.py                   # RMQSensor
│   ├── triggers/
│   │   └── rmq.py                   # RMQTrigger
│   ├── utils/
│   │   ├── filters.py               # MessageFilter
│   │   └── ssl.py                   # build_ssl_context()
│   └── example_dags/                # Example DAGs
├── tests/                           # Unit tests
├── pyproject.toml
└── readme.md
```

---

## Running Tests

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run all tests
pytest tests/

# Run specific test module
pytest tests/test_trigger.py -v
```

---

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
