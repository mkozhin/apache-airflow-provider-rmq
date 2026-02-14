from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from apache_airflow_provider_rmq.operators.rmq_management import (
    RMQQueueManagementOperator,
    VALID_ACTIONS,
)


HOOK_PATH = "apache_airflow_provider_rmq.operators.rmq_management.RMQHook"


@pytest.fixture
def mock_hook():
    with patch(HOOK_PATH) as MockHook:
        hook_instance = MagicMock()
        MockHook.return_value.__enter__ = MagicMock(return_value=hook_instance)
        MockHook.return_value.__exit__ = MagicMock(return_value=False)
        yield MockHook, hook_instance


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
class TestValidation:
    def test_invalid_action_raises(self):
        with pytest.raises(ValueError, match="Invalid action"):
            RMQQueueManagementOperator(task_id="t", action="bad_action")

    def test_all_valid_actions_accepted(self):
        for action in VALID_ACTIONS:
            op = RMQQueueManagementOperator(task_id="t", action=action)
            assert op.action == action


# ---------------------------------------------------------------------------
# Queue actions
# ---------------------------------------------------------------------------
class TestQueueActions:
    def test_declare_queue(self, mock_hook):
        _, hook = mock_hook
        method_result = MagicMock()
        method_result.method.queue = "my_queue"
        method_result.method.message_count = 0
        hook.queue_declare.return_value = method_result

        op = RMQQueueManagementOperator(
            task_id="t", action="declare_queue", queue_name="my_queue",
            durable=True, exclusive=False, auto_delete=False,
            arguments={"x-max-length": 1000},
        )
        result = op.execute(context={})
        hook.queue_declare.assert_called_once_with(
            queue_name="my_queue", durable=True, exclusive=False,
            auto_delete=False, arguments={"x-max-length": 1000},
        )
        assert result == {"queue": "my_queue", "message_count": 0}

    def test_delete_queue(self, mock_hook):
        _, hook = mock_hook
        op = RMQQueueManagementOperator(
            task_id="t", action="delete_queue", queue_name="my_queue",
            if_unused=True, if_empty=True,
        )
        op.execute(context={})
        hook.queue_delete.assert_called_once_with(
            queue_name="my_queue", if_unused=True, if_empty=True,
        )

    def test_purge_queue(self, mock_hook):
        _, hook = mock_hook
        op = RMQQueueManagementOperator(
            task_id="t", action="purge_queue", queue_name="my_queue",
        )
        op.execute(context={})
        hook.queue_purge.assert_called_once_with(queue_name="my_queue")

    def test_bind_queue(self, mock_hook):
        _, hook = mock_hook
        op = RMQQueueManagementOperator(
            task_id="t", action="bind_queue", queue_name="my_queue",
            exchange_name="my_ex", routing_key="rk",
            arguments={"x-match": "all"},
        )
        op.execute(context={})
        hook.queue_bind.assert_called_once_with(
            queue="my_queue", exchange="my_ex", routing_key="rk",
            arguments={"x-match": "all"},
        )

    def test_unbind_queue(self, mock_hook):
        _, hook = mock_hook
        op = RMQQueueManagementOperator(
            task_id="t", action="unbind_queue", queue_name="my_queue",
            exchange_name="my_ex", routing_key="rk",
        )
        op.execute(context={})
        hook.queue_unbind.assert_called_once_with(
            queue="my_queue", exchange="my_ex", routing_key="rk",
            arguments=None,
        )


# ---------------------------------------------------------------------------
# Exchange actions
# ---------------------------------------------------------------------------
class TestExchangeActions:
    def test_declare_exchange(self, mock_hook):
        _, hook = mock_hook
        op = RMQQueueManagementOperator(
            task_id="t", action="declare_exchange", exchange_name="my_ex",
            exchange_type="topic", durable=True, auto_delete=False,
            internal=True, arguments={"alternate-exchange": "alt"},
        )
        op.execute(context={})
        hook.exchange_declare.assert_called_once_with(
            exchange="my_ex", exchange_type="topic", durable=True,
            auto_delete=False, internal=True,
            arguments={"alternate-exchange": "alt"},
        )

    def test_delete_exchange(self, mock_hook):
        _, hook = mock_hook
        op = RMQQueueManagementOperator(
            task_id="t", action="delete_exchange", exchange_name="my_ex",
            if_unused=True,
        )
        op.execute(context={})
        hook.exchange_delete.assert_called_once_with(
            exchange="my_ex", if_unused=True,
        )

    def test_bind_exchange(self, mock_hook):
        _, hook = mock_hook
        op = RMQQueueManagementOperator(
            task_id="t", action="bind_exchange", exchange_name="dst_ex",
            source_exchange="src_ex", routing_key="rk",
        )
        op.execute(context={})
        hook.exchange_bind.assert_called_once_with(
            destination="dst_ex", source="src_ex", routing_key="rk",
            arguments=None,
        )

    def test_unbind_exchange(self, mock_hook):
        _, hook = mock_hook
        op = RMQQueueManagementOperator(
            task_id="t", action="unbind_exchange", exchange_name="dst_ex",
            source_exchange="src_ex", routing_key="rk",
            arguments={"key": "val"},
        )
        op.execute(context={})
        hook.exchange_unbind.assert_called_once_with(
            destination="dst_ex", source="src_ex", routing_key="rk",
            arguments={"key": "val"},
        )


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------
class TestConnection:
    def test_default_conn_id(self, mock_hook):
        MockHook, hook = mock_hook
        hook.queue_purge.return_value = None
        op = RMQQueueManagementOperator(
            task_id="t", action="purge_queue", queue_name="q",
        )
        op.execute(context={})
        MockHook.assert_called_once_with(rmq_conn_id="rmq_default")

    def test_custom_conn_id(self, mock_hook):
        MockHook, hook = mock_hook
        hook.queue_purge.return_value = None
        op = RMQQueueManagementOperator(
            task_id="t", action="purge_queue", queue_name="q",
            rmq_conn_id="custom_rmq",
        )
        op.execute(context={})
        MockHook.assert_called_once_with(rmq_conn_id="custom_rmq")