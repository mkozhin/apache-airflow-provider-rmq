from __future__ import annotations

import json
import logging
from typing import Any, Callable, Protocol

log = logging.getLogger("airflow.task")


class HasHeaders(Protocol):
    """Protocol for objects that have a headers attribute (pika BasicProperties, aio_pika shim)."""

    headers: dict[str, Any] | None


class MessageFilter:
    """Evaluate whether a RabbitMQ message matches given filter conditions.

    Supports two modes:
    1. Dict-based filtering: {"header_key": "value", "body.nested.key": "value"}
       - Keys starting with "body." traverse the JSON-parsed message body.
       - All other keys check the message properties.headers dict.
    2. Callable-based filtering: fn(properties, body_str) -> bool

    Both can be combined (AND logic: both must pass).
    """

    def __init__(
        self,
        filter_headers: dict[str, Any] | None = None,
        filter_callable: Callable[[Any, str], bool] | None = None,
    ):
        self._filter_headers = filter_headers
        self._filter_callable = filter_callable

    @property
    def has_filters(self) -> bool:
        return self._filter_headers is not None or self._filter_callable is not None

    def matches(self, properties: HasHeaders, body: str) -> bool:
        """Return True if the message passes all configured filters."""
        if not self.has_filters:
            return True

        if self._filter_headers is not None:
            if not self._match_dict(properties, body):
                return False

        if self._filter_callable is not None:
            try:
                if not self._filter_callable(properties, body):
                    return False
            except Exception:
                log.warning("Filter callable raised an exception; treating as non-match", exc_info=True)
                return False

        return True

    def _match_dict(self, properties: HasHeaders, body: str) -> bool:
        """Evaluate dict-based filter."""
        headers = properties.headers or {}
        parsed_body: dict | None = None

        for key, expected_value in self._filter_headers.items():
            if key.startswith("body."):
                if parsed_body is None:
                    try:
                        parsed_body = json.loads(body)
                    except (json.JSONDecodeError, TypeError):
                        return False
                # Navigate nested keys: "body.data.status" -> parsed_body["data"]["status"]
                path = key[5:].split(".")
                current = parsed_body
                for part in path:
                    if isinstance(current, dict) and part in current:
                        current = current[part]
                    else:
                        return False
                if current != expected_value:
                    return False
            else:
                if headers.get(key) != expected_value:
                    return False
        return True

    def serialize(self) -> dict[str, Any]:
        """Serialize for use in Trigger (must be JSON-serializable).

        Note: callable filters cannot be serialized; only dict filters are passed to triggers.
        """
        return {"filter_headers": self._filter_headers}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> MessageFilter:
        """Reconstruct MessageFilter from serialized data (dict-based only)."""
        return cls(filter_headers=data.get("filter_headers"))