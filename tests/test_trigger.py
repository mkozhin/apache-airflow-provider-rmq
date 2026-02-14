from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from apache_airflow_provider_rmq.triggers.rmq import RMQTrigger
from tests.conftest import FakeAirflowConnection


# ---------------------------------------------------------------------------
# Serialize / Deserialize
# ---------------------------------------------------------------------------
class TestSerialize:
    def test_serialize_full(self):
        trigger = RMQTrigger(
            rmq_conn_id="my_conn",
            queue_name="my_queue",
            filter_data={"filter_headers": {"x-type": "order"}},
            poll_interval=10.0,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "apache_airflow_provider_rmq.triggers.rmq.RMQTrigger"
        assert kwargs == {
            "rmq_conn_id": "my_conn",
            "queue_name": "my_queue",
            "filter_data": {"filter_headers": {"x-type": "order"}},
            "poll_interval": 10.0,
        }

    def test_serialize_defaults(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        _, kwargs = trigger.serialize()
        assert kwargs["filter_data"] == {}
        assert kwargs["poll_interval"] == 5.0

    def test_roundtrip(self):
        original = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"key": "val"}},
            poll_interval=3.0,
        )
        _, kwargs = original.serialize()
        restored = RMQTrigger(**kwargs)
        assert restored.rmq_conn_id == original.rmq_conn_id
        assert restored.queue_name == original.queue_name
        assert restored.filter_data == original.filter_data
        assert restored.poll_interval == original.poll_interval


# ---------------------------------------------------------------------------
# Helper to run async trigger and collect events
# ---------------------------------------------------------------------------
async def _collect_events(trigger: RMQTrigger) -> list[dict]:
    events = []
    async for event in trigger.run():
        events.append(event.payload)
    return events


# ---------------------------------------------------------------------------
# Run — matching message (no filter)
# ---------------------------------------------------------------------------
class TestRunNoFilter:
    @pytest.mark.asyncio
    async def test_any_message_matches(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")

        fake_message = MagicMock()
        fake_message.body = b"hello"
        fake_message.headers = {"x-source": "test"}
        fake_message.routing_key = "rk"
        fake_message.ack = AsyncMock()

        fake_queue = MagicMock()
        fake_queue.get = AsyncMock(return_value=fake_message)

        fake_channel = MagicMock()
        fake_channel.declare_queue = AsyncMock(return_value=fake_queue)

        fake_connection = AsyncMock()
        fake_connection.__aenter__ = AsyncMock(return_value=fake_connection)
        fake_connection.__aexit__ = AsyncMock(return_value=False)
        fake_connection.channel = AsyncMock(return_value=fake_channel)

        fake_conn_info = FakeAirflowConnection()

        with patch("apache_airflow_provider_rmq.triggers.rmq.aio_pika") as mock_aio_pika:
            mock_aio_pika.connect_robust = AsyncMock(return_value=fake_connection)
            with patch("apache_airflow_provider_rmq.triggers.rmq.BaseHook") as mock_base:
                mock_base.get_connection = MagicMock(return_value=fake_conn_info)

                events = await _collect_events(trigger)

        assert len(events) == 1
        assert events[0]["status"] == "success"
        assert events[0]["message"]["body"] == "hello"
        assert events[0]["message"]["headers"] == {"x-source": "test"}
        assert events[0]["message"]["routing_key"] == "rk"
        fake_message.ack.assert_awaited_once()


# ---------------------------------------------------------------------------
# Run — with dict filter, matching
# ---------------------------------------------------------------------------
class TestRunWithFilter:
    @pytest.mark.asyncio
    async def test_matching_message_acked(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"x-type": "order"}},
        )

        fake_message = MagicMock()
        fake_message.body = b"order data"
        fake_message.headers = {"x-type": "order"}
        fake_message.routing_key = "orders"
        fake_message.ack = AsyncMock()

        fake_queue = MagicMock()
        fake_queue.get = AsyncMock(return_value=fake_message)

        fake_channel = MagicMock()
        fake_channel.declare_queue = AsyncMock(return_value=fake_queue)

        fake_connection = AsyncMock()
        fake_connection.__aenter__ = AsyncMock(return_value=fake_connection)
        fake_connection.__aexit__ = AsyncMock(return_value=False)
        fake_connection.channel = AsyncMock(return_value=fake_channel)

        fake_conn_info = FakeAirflowConnection()

        with patch("apache_airflow_provider_rmq.triggers.rmq.aio_pika") as mock_aio_pika:
            mock_aio_pika.connect_robust = AsyncMock(return_value=fake_connection)
            with patch("apache_airflow_provider_rmq.triggers.rmq.BaseHook") as mock_base:
                mock_base.get_connection = MagicMock(return_value=fake_conn_info)

                events = await _collect_events(trigger)

        assert len(events) == 1
        assert events[0]["status"] == "success"
        assert events[0]["message"]["body"] == "order data"
        fake_message.ack.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_non_matching_then_matching(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"x-type": "order"}},
            poll_interval=0.01,
        )

        non_match_msg = MagicMock()
        non_match_msg.body = b"invoice"
        non_match_msg.headers = {"x-type": "invoice"}
        non_match_msg.routing_key = "invoices"
        non_match_msg.ack = AsyncMock()
        non_match_msg.nack = AsyncMock()

        match_msg = MagicMock()
        match_msg.body = b"order"
        match_msg.headers = {"x-type": "order"}
        match_msg.routing_key = "orders"
        match_msg.ack = AsyncMock()

        fake_queue = MagicMock()
        # First call returns non-matching, second returns matching
        fake_queue.get = AsyncMock(side_effect=[non_match_msg, match_msg])

        fake_channel = MagicMock()
        fake_channel.declare_queue = AsyncMock(return_value=fake_queue)

        fake_connection = AsyncMock()
        fake_connection.__aenter__ = AsyncMock(return_value=fake_connection)
        fake_connection.__aexit__ = AsyncMock(return_value=False)
        fake_connection.channel = AsyncMock(return_value=fake_channel)

        fake_conn_info = FakeAirflowConnection()

        with patch("apache_airflow_provider_rmq.triggers.rmq.aio_pika") as mock_aio_pika:
            mock_aio_pika.connect_robust = AsyncMock(return_value=fake_connection)
            with patch("apache_airflow_provider_rmq.triggers.rmq.BaseHook") as mock_base:
                mock_base.get_connection = MagicMock(return_value=fake_conn_info)

                events = await _collect_events(trigger)

        assert len(events) == 1
        assert events[0]["status"] == "success"
        assert events[0]["message"]["body"] == "order"
        non_match_msg.nack.assert_awaited_once_with(requeue=True)
        match_msg.ack.assert_awaited_once()


# ---------------------------------------------------------------------------
# Run — empty queue polls
# ---------------------------------------------------------------------------
class TestRunEmptyQueue:
    @pytest.mark.asyncio
    async def test_polls_then_gets_message(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            poll_interval=0.01,
        )

        fake_message = MagicMock()
        fake_message.body = b"delayed"
        fake_message.headers = {}
        fake_message.routing_key = "rk"
        fake_message.ack = AsyncMock()

        fake_queue = MagicMock()
        # First two calls return None (empty), third returns message
        fake_queue.get = AsyncMock(side_effect=[None, None, fake_message])

        fake_channel = MagicMock()
        fake_channel.declare_queue = AsyncMock(return_value=fake_queue)

        fake_connection = AsyncMock()
        fake_connection.__aenter__ = AsyncMock(return_value=fake_connection)
        fake_connection.__aexit__ = AsyncMock(return_value=False)
        fake_connection.channel = AsyncMock(return_value=fake_channel)

        fake_conn_info = FakeAirflowConnection()

        with patch("apache_airflow_provider_rmq.triggers.rmq.aio_pika") as mock_aio_pika:
            mock_aio_pika.connect_robust = AsyncMock(return_value=fake_connection)
            with patch("apache_airflow_provider_rmq.triggers.rmq.BaseHook") as mock_base:
                mock_base.get_connection = MagicMock(return_value=fake_conn_info)

                events = await _collect_events(trigger)

        assert len(events) == 1
        assert events[0]["status"] == "success"
        assert events[0]["message"]["body"] == "delayed"
        assert fake_queue.get.await_count == 3


# ---------------------------------------------------------------------------
# Run — connection error
# ---------------------------------------------------------------------------
class TestRunError:
    @pytest.mark.asyncio
    async def test_connection_error_yields_error_event(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")

        fake_conn_info = FakeAirflowConnection()

        with patch("apache_airflow_provider_rmq.triggers.rmq.aio_pika") as mock_aio_pika:
            mock_aio_pika.connect_robust = AsyncMock(side_effect=ConnectionError("refused"))
            with patch("apache_airflow_provider_rmq.triggers.rmq.BaseHook") as mock_base:
                mock_base.get_connection = MagicMock(return_value=fake_conn_info)

                events = await _collect_events(trigger)

        assert len(events) == 1
        assert events[0]["status"] == "error"
        assert "refused" in events[0]["error"]


# ---------------------------------------------------------------------------
# SSL URL construction
# ---------------------------------------------------------------------------
class TestSSLUrl:
    @pytest.mark.asyncio
    async def test_ssl_uses_amqps_scheme(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")

        fake_conn_info = FakeAirflowConnection(
            host="rmq.example.com", port=0,
            extra='{"ssl_enabled": true}',
        )

        with patch("apache_airflow_provider_rmq.triggers.rmq.aio_pika") as mock_aio_pika:
            mock_aio_pika.connect_robust = AsyncMock(side_effect=ConnectionError("test"))
            with patch("apache_airflow_provider_rmq.triggers.rmq.BaseHook") as mock_base:
                mock_base.get_connection = MagicMock(return_value=fake_conn_info)

                events = await _collect_events(trigger)

        call_args = mock_aio_pika.connect_robust.call_args
        url = call_args[0][0]
        assert url.startswith("amqps://")
        assert "rmq.example.com:5671" in url