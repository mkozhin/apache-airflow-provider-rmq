from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pika
import pytest

from airflow_provider_rmq.operators.rmq_publish import RMQPublishOperator


HOOK_PATH = "airflow_provider_rmq.operators.rmq_publish.RMQHook"


@pytest.fixture
def mock_hook():
    """Return a mocked RMQHook used as context manager."""
    with patch(HOOK_PATH) as MockHook:
        hook_instance = MagicMock()
        MockHook.return_value.__enter__ = MagicMock(return_value=hook_instance)
        MockHook.return_value.__exit__ = MagicMock(return_value=False)
        yield MockHook, hook_instance


class TestPublishViaExchange:
    def test_publish_single_string(self, mock_hook):
        MockHook, hook = mock_hook
        op = RMQPublishOperator(
            task_id="test", exchange="my_ex", routing_key="rk", message="hello",
        )
        op.execute(context={})
        hook.basic_publish.assert_called_once()
        call_kwargs = hook.basic_publish.call_args.kwargs
        assert call_kwargs["exchange"] == "my_ex"
        assert call_kwargs["routing_key"] == "rk"
        assert call_kwargs["body"] == "hello"

    def test_publish_uses_exchange_and_routing_key(self, mock_hook):
        MockHook, hook = mock_hook
        op = RMQPublishOperator(
            task_id="test", exchange="orders", routing_key="new.order", message="data",
        )
        op.execute(context={})
        call_kwargs = hook.basic_publish.call_args.kwargs
        assert call_kwargs["exchange"] == "orders"
        assert call_kwargs["routing_key"] == "new.order"


class TestPublishToQueue:
    def test_queue_name_sets_default_exchange(self, mock_hook):
        MockHook, hook = mock_hook
        op = RMQPublishOperator(
            task_id="test", queue_name="my_queue", message="hello",
        )
        assert op.exchange == ""
        assert op.routing_key == "my_queue"
        op.execute(context={})
        call_kwargs = hook.basic_publish.call_args.kwargs
        assert call_kwargs["exchange"] == ""
        assert call_kwargs["routing_key"] == "my_queue"

    def test_queue_name_overrides_exchange(self, mock_hook):
        _, hook = mock_hook
        op = RMQPublishOperator(
            task_id="test", exchange="ignored", routing_key="ignored",
            queue_name="my_queue", message="msg",
        )
        assert op.exchange == ""
        assert op.routing_key == "my_queue"


class TestMessageTypes:
    def test_single_string(self, mock_hook):
        _, hook = mock_hook
        op = RMQPublishOperator(task_id="t", exchange="", routing_key="q", message="text")
        op.execute(context={})
        assert hook.basic_publish.call_count == 1
        assert hook.basic_publish.call_args.kwargs["body"] == "text"

    def test_list_of_strings(self, mock_hook):
        _, hook = mock_hook
        op = RMQPublishOperator(
            task_id="t", exchange="", routing_key="q", message=["a", "b", "c"],
        )
        op.execute(context={})
        assert hook.basic_publish.call_count == 3
        bodies = [c.kwargs["body"] for c in hook.basic_publish.call_args_list]
        assert bodies == ["a", "b", "c"]

    def test_single_dict(self, mock_hook):
        _, hook = mock_hook
        data = {"key": "value", "num": 42}
        op = RMQPublishOperator(task_id="t", exchange="", routing_key="q", message=data)
        op.execute(context={})
        assert hook.basic_publish.call_count == 1
        body = hook.basic_publish.call_args.kwargs["body"]
        assert json.loads(body) == data

    def test_list_of_dicts(self, mock_hook):
        _, hook = mock_hook
        msgs = [{"id": 1}, {"id": 2}]
        op = RMQPublishOperator(task_id="t", exchange="", routing_key="q", message=msgs)
        op.execute(context={})
        assert hook.basic_publish.call_count == 2
        bodies = [json.loads(c.kwargs["body"]) for c in hook.basic_publish.call_args_list]
        assert bodies == msgs

    def test_mixed_list(self, mock_hook):
        _, hook = mock_hook
        msgs = ["plain", {"key": "val"}]
        op = RMQPublishOperator(task_id="t", exchange="", routing_key="q", message=msgs)
        op.execute(context={})
        assert hook.basic_publish.call_count == 2
        b0 = hook.basic_publish.call_args_list[0].kwargs["body"]
        b1 = hook.basic_publish.call_args_list[1].kwargs["body"]
        assert b0 == "plain"
        assert json.loads(b1) == {"key": "val"}

    def test_none_message(self, mock_hook):
        _, hook = mock_hook
        op = RMQPublishOperator(task_id="t", exchange="", routing_key="q", message=None)
        op.execute(context={})
        hook.basic_publish.assert_not_called()

    def test_integer_message(self, mock_hook):
        _, hook = mock_hook
        op = RMQPublishOperator(task_id="t", exchange="", routing_key="q", message=123)
        op.execute(context={})
        assert hook.basic_publish.call_args.kwargs["body"] == "123"


class TestMessageProperties:
    def test_properties_passed(self, mock_hook):
        _, hook = mock_hook
        op = RMQPublishOperator(
            task_id="t", exchange="", routing_key="q", message="msg",
            content_type="application/json",
            delivery_mode=2,
            headers={"x-custom": "val"},
            priority=5,
            expiration="60000",
            correlation_id="corr-123",
            reply_to="reply_queue",
            message_id="msg-456",
        )
        op.execute(context={})
        props = hook.basic_publish.call_args.kwargs["properties"]
        assert isinstance(props, pika.BasicProperties)
        assert props.content_type == "application/json"
        assert props.delivery_mode == 2
        assert props.headers == {"x-custom": "val"}
        assert props.priority == 5
        assert props.expiration == "60000"
        assert props.correlation_id == "corr-123"
        assert props.reply_to == "reply_queue"
        assert props.message_id == "msg-456"

    def test_default_properties_are_none(self, mock_hook):
        _, hook = mock_hook
        op = RMQPublishOperator(task_id="t", exchange="", routing_key="q", message="msg")
        op.execute(context={})
        props = hook.basic_publish.call_args.kwargs["properties"]
        assert props.content_type is None
        assert props.delivery_mode is None
        assert props.headers is None

    def test_same_properties_for_batch(self, mock_hook):
        _, hook = mock_hook
        op = RMQPublishOperator(
            task_id="t", exchange="", routing_key="q",
            message=["a", "b"], delivery_mode=2,
        )
        op.execute(context={})
        props_list = [c.kwargs["properties"] for c in hook.basic_publish.call_args_list]
        assert all(p.delivery_mode == 2 for p in props_list)


class TestConnectionId:
    def test_default_conn_id(self, mock_hook):
        MockHook, _ = mock_hook
        op = RMQPublishOperator(task_id="t", exchange="", routing_key="q", message="m")
        op.execute(context={})
        MockHook.assert_called_once_with(rmq_conn_id="rmq_default")

    def test_custom_conn_id(self, mock_hook):
        MockHook, _ = mock_hook
        op = RMQPublishOperator(
            task_id="t", exchange="", routing_key="q", message="m",
            rmq_conn_id="my_rmq",
        )
        op.execute(context={})
        MockHook.assert_called_once_with(rmq_conn_id="my_rmq")