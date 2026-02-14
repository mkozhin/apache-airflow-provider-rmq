from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncIterator
from urllib.parse import quote

import aio_pika
from airflow.hooks.base import BaseHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

from apache_airflow_provider_rmq.utils.filters import MessageFilter

log = logging.getLogger("airflow.task")


class RMQTrigger(BaseTrigger):
    """Async trigger that waits for a matching message in a RabbitMQ queue.

    Uses aio_pika for non-blocking AMQP access. Polls the queue periodically
    and evaluates dict-based filters on each message.

    Note: callable filters are NOT supported here (cannot be serialized).
    """

    def __init__(
        self,
        rmq_conn_id: str,
        queue_name: str,
        filter_data: dict[str, Any] | None = None,
        poll_interval: float = 5.0,
    ):
        super().__init__()
        self.rmq_conn_id = rmq_conn_id
        self.queue_name = queue_name
        self.filter_data = filter_data or {}
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "apache_airflow_provider_rmq.triggers.rmq.RMQTrigger",
            {
                "rmq_conn_id": self.rmq_conn_id,
                "queue_name": self.queue_name,
                "filter_data": self.filter_data,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Main trigger loop. Polls RabbitMQ asynchronously."""
        msg_filter = MessageFilter.deserialize(self.filter_data)

        try:
            conn_info = await asyncio.get_event_loop().run_in_executor(
                None, BaseHook.get_connection, self.rmq_conn_id
            )
            extras = conn_info.extra_dejson
            ssl_enabled = extras.get("ssl_enabled", False)
            vhost = conn_info.schema or "/"
            port = conn_info.port or (5671 if ssl_enabled else 5672)

            # Encode vhost for URL (e.g. "/" -> "%2F")
            vhost_encoded = quote(vhost, safe="")

            url = (
                f"{'amqps' if ssl_enabled else 'amqp'}://"
                f"{conn_info.login or 'guest'}:{conn_info.password or 'guest'}"
                f"@{conn_info.host or 'localhost'}:{port}/{vhost_encoded}"
            )

            connection = await aio_pika.connect_robust(url)

            async with connection:
                channel = await connection.channel()
                queue = await channel.declare_queue(self.queue_name, passive=True)

                while True:
                    message = await queue.get(fail=False)

                    if message is None:
                        await asyncio.sleep(self.poll_interval)
                        continue

                    body_str = message.body.decode("utf-8")

                    if msg_filter.has_filters:

                        class _PropsShim:
                            def __init__(self, hdrs):
                                self.headers = hdrs

                        props_shim = _PropsShim(dict(message.headers or {}))

                        if msg_filter.matches(props_shim, body_str):
                            await message.ack()
                            yield TriggerEvent({
                                "status": "success",
                                "message": {
                                    "body": body_str,
                                    "headers": dict(message.headers or {}),
                                    "routing_key": message.routing_key or "",
                                },
                            })
                            return
                        else:
                            await message.nack(requeue=True)
                            await asyncio.sleep(self.poll_interval)
                    else:
                        await message.ack()
                        yield TriggerEvent({
                            "status": "success",
                            "message": {
                                "body": body_str,
                                "headers": dict(message.headers or {}),
                                "routing_key": message.routing_key or "",
                            },
                        })
                        return

        except Exception as e:
            log.exception("RMQTrigger encountered an error")
            yield TriggerEvent({"status": "error", "error": str(e)})