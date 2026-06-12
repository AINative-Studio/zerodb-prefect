"""
ZeroDBSensor -- Polls ZeroDB events and triggers Prefect flows.

    from zerodb_prefect import ZeroDBSensor

    sensor = ZeroDBSensor(event_type='zerodb.vector.stored')

    @sensor.on_event
    def process_vector(event):
        return {'processed': event.data['vector_id']}

    sensor.start()
"""

import asyncio
import inspect
import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional

import requests

from zerodb_prefect.provision import resolve_credentials

logger = logging.getLogger(__name__)


class ZeroDBEvent:
    """Wrapper for a ZeroDB event payload.

    Attributes:
        event_type: Type of the event (e.g. 'zerodb.file.uploaded').
        event_id: Unique event ID.
        data: Event payload dict.
        timestamp: ISO 8601 timestamp.
        project_id: ZeroDB project ID.
        metadata: Additional metadata.
    """

    __slots__ = ("event_type", "event_id", "data", "timestamp", "project_id", "metadata")

    def __init__(self, event_type="", event_id="", data=None, timestamp="",
                 project_id="", metadata=None):
        self.event_type = event_type
        self.event_id = event_id
        self.data = data if data is not None else {}
        self.timestamp = timestamp
        self.project_id = project_id
        self.metadata = metadata if metadata is not None else {}

    @classmethod
    def from_dict(cls, d):
        """Create a ZeroDBEvent from a dict (e.g. webhook or poll payload)."""
        return cls(
            event_type=d.get("event_type", d.get("type", "")),
            event_id=d.get("event_id", d.get("id", "")),
            data=d.get("data", d.get("payload", {})),
            timestamp=d.get("timestamp", d.get("created_at", "")),
            project_id=d.get("project_id", ""),
            metadata=d.get("metadata", {}),
        )

    def to_dict(self):
        """Serialize to dict."""
        return {
            "event_type": self.event_type,
            "event_id": self.event_id,
            "data": self.data,
            "timestamp": self.timestamp,
            "project_id": self.project_id,
            "metadata": self.metadata,
        }


class ZeroDBSensor:
    """Polls ZeroDB events and triggers handlers (Prefect flow entry points).

    Args:
        event_type: ZeroDB event type to listen for.
        api_key: ZeroDB API key (auto-resolved if not provided).
        project_id: ZeroDB project ID.
        base_url: ZeroDB API base URL.
        poll_interval: Seconds between polls (default 5).
        batch_size: Max events per poll (default 100).
    """

    def __init__(
        self,
        event_type,
        api_key=None,
        project_id=None,
        base_url=None,
        poll_interval=5,
        batch_size=100,
    ):
        self._event_type = event_type
        self._api_key, self._project_id, self._base_url = resolve_credentials(
            api_key=api_key,
            project_id=project_id,
        )
        if base_url:
            self._base_url = base_url

        self._poll_interval = poll_interval
        self._batch_size = batch_size
        self._handlers: List[Callable] = []
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_event_id: Optional[str] = None

        self._session = requests.Session()
        self._session.headers.update({
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._api_key}",
            "X-Project-ID": self._project_id,
        })

        self._events_url = f"{self._base_url}/api/v1/public/events"

    @property
    def event_type(self):
        """The event type this sensor listens for."""
        return self._event_type

    @property
    def is_running(self):
        """Whether the polling loop is active."""
        return self._running

    @property
    def handler_count(self):
        """Number of registered handlers."""
        return len(self._handlers)

    @property
    def last_event_id(self):
        """The ID of the last processed event (cursor)."""
        return self._last_event_id

    def on_event(self, func):
        """Decorator to register an event handler.

        Args:
            func: Sync or async function receiving a ZeroDBEvent.

        Returns:
            The original function (unmodified).
        """
        self._handlers.append(func)
        return func

    def add_handler(self, func):
        """Register an event handler (non-decorator version).

        Args:
            func: Sync or async function receiving a ZeroDBEvent.
        """
        self._handlers.append(func)

    def poll(self):
        """Fetch new events from ZeroDB event stream.

        Returns:
            list of ZeroDBEvent objects.
        """
        params = {
            "event_type": self._event_type,
            "limit": self._batch_size,
        }
        if self._last_event_id:
            params["after"] = self._last_event_id

        try:
            resp = self._session.get(f"{self._events_url}/stream", params=params)
            resp.raise_for_status()
            result = resp.json()

            if isinstance(result, list):
                events_raw = result
            else:
                events_raw = result.get("events", [])
            parsed = []
            for raw in events_raw:
                event = ZeroDBEvent.from_dict(raw)
                parsed.append(event)
                if event.event_id:
                    self._last_event_id = event.event_id

            return parsed

        except requests.RequestException as e:
            logger.warning("Failed to poll ZeroDB events: %s", e)
            return []

    def _dispatch_event(self, event):
        """Dispatch an event to all registered handlers."""
        results = []
        for handler in self._handlers:
            try:
                if inspect.iscoroutinefunction(handler):
                    loop = asyncio.new_event_loop()
                    try:
                        result = loop.run_until_complete(handler(event))
                    finally:
                        loop.close()
                else:
                    result = handler(event)
                results.append(result)
            except Exception as e:
                logger.error(
                    "Handler %s failed for event %s: %s",
                    handler.__name__,
                    event.event_id,
                    e,
                )
                results.append(None)
        return results

    def _poll_loop(self):
        """Background polling loop."""
        while self._running:
            events = self.poll()
            for event in events:
                self._dispatch_event(event)
            time.sleep(self._poll_interval)

    def start(self):
        """Start polling for events in a background thread.

        Returns:
            self for chaining.
        """
        if self._running:
            return self

        self._running = True
        self._thread = threading.Thread(
            target=self._poll_loop,
            daemon=True,
            name=f"zerodb-sensor-{self._event_type}",
        )
        self._thread.start()
        logger.info("ZeroDBSensor started: listening for %s events", self._event_type)
        return self

    def stop(self):
        """Stop polling for events."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=self._poll_interval + 1)
            self._thread = None
        logger.info("ZeroDBSensor stopped")

    def process_webhook(self, payload):
        """Process a webhook payload directly (alternative to polling).

        Args:
            payload: Dict from the webhook request body.

        Returns:
            list of handler results.
        """
        event = ZeroDBEvent.from_dict(payload)

        # Filter by event type
        if event.event_type != self._event_type:
            return []

        return self._dispatch_event(event)
