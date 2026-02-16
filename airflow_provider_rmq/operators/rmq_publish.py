from __future__ import annotations

import json
import logging
from typing import Any, Sequence

import pika
from airflow.models import BaseOperator

from airflow_provider_rmq.hooks.rmq import RMQHook

log = logging.getLogger("airflow.task")


class RMQPublishOperator(BaseOperator):
    """Publish one or more messages to RabbitMQ.

    Supports publishing to a named exchange or directly to a queue
    (via default exchange with routing_key=queue_name).

    Messages can be strings, dicts (auto-serialized to JSON), or lists thereof.
    """

    template_fields: Sequence[str] = ("exchange", "routing_key", "message")
    ui_color = "#ff6600"

    def __init__(
        self,
        *,
        rmq_conn_id: str = "rmq_default",
        exchange: str = "",
        routing_key: str = "",
        message: str | list[str] | dict | list[dict] | None = None,
        queue_name: str | None = None,
        content_type: str | None = None,
        delivery_mode: int | None = None,
        headers: dict | None = None,
        priority: int | None = None,
        expiration: str | None = None,
        correlation_id: str | None = None,
        reply_to: str | None = None,
        message_id: str | None = None,
        **kwargs,
    ):
        """Create a new RMQPublishOperator.

        :param rmq_conn_id: Airflow connection ID for RabbitMQ.
        :type rmq_conn_id: str
        :param exchange: Exchange to publish to (empty string for default exchange).
        :type exchange: str
        :param routing_key: Routing key for the message.
        :type routing_key: str
        :param message: Message payload — string, dict, or list thereof. Dicts are JSON-serialized.
        :type message: str | list[str] | dict | list[dict] | None
        :param queue_name: Shortcut — sets ``exchange=""`` and ``routing_key=queue_name``.
        :type queue_name: str | None
        :param content_type: AMQP content type header.
        :type content_type: str | None
        :param delivery_mode: ``1`` for non-persistent, ``2`` for persistent.
        :type delivery_mode: int | None
        :param headers: Custom AMQP headers.
        :type headers: dict | None
        :param priority: Message priority (0–9).
        :type priority: int | None
        :param expiration: Per-message TTL in milliseconds (as string).
        :type expiration: str | None
        :param correlation_id: Application correlation identifier.
        :type correlation_id: str | None
        :param reply_to: Reply-to queue name.
        :type reply_to: str | None
        :param message_id: Application message identifier.
        :type message_id: str | None
        """
        super().__init__(**kwargs)
        self.rmq_conn_id = rmq_conn_id
        if queue_name:
            self.exchange = ""
            self.routing_key = queue_name
        else:
            self.exchange = exchange
            self.routing_key = routing_key
        self.message = message
        self.content_type = content_type
        self.delivery_mode = delivery_mode
        self.headers = headers
        self.priority = priority
        self.expiration = expiration
        self.correlation_id = correlation_id
        self.reply_to = reply_to
        self.message_id = message_id

    def execute(self, context: Any) -> None:
        properties = pika.BasicProperties(
            content_type=self.content_type,
            delivery_mode=self.delivery_mode,
            headers=self.headers,
            priority=self.priority,
            expiration=self.expiration,
            correlation_id=self.correlation_id,
            reply_to=self.reply_to,
            message_id=self.message_id,
        )

        messages = self._normalize_messages()

        with RMQHook(rmq_conn_id=self.rmq_conn_id) as hook:
            for msg in messages:
                hook.basic_publish(
                    exchange=self.exchange,
                    routing_key=self.routing_key,
                    body=msg,
                    properties=properties,
                )
                log.info(
                    "Published message to exchange='%s' routing_key='%s'",
                    self.exchange,
                    self.routing_key,
                )

    def _normalize_messages(self) -> list[str]:
        """Convert message input to a list of string payloads.

        :return: List of string-encoded messages ready for publishing.
        :rtype: list[str]
        """
        if self.message is None:
            return []
        if isinstance(self.message, list):
            return [
                json.dumps(m) if isinstance(m, dict) else str(m)
                for m in self.message
            ]
        if isinstance(self.message, dict):
            return [json.dumps(self.message)]
        return [str(self.message)]