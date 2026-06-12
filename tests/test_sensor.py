"""
Tests for zerodb-prefect ZeroDBSensor.

Uses mocked HTTP responses -- no real API calls.
"""

import os
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("ZERODB_API_KEY", "test-key-000")
os.environ.setdefault("ZERODB_PROJECT_ID", "test-project-000")

from zerodb_prefect.sensor import ZeroDBSensor, ZeroDBEvent


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_session():
    with patch("zerodb_prefect.sensor.requests.Session") as MockSession:
        session = MagicMock()
        MockSession.return_value = session
        yield session


@pytest.fixture
def sensor(mock_session):
    return ZeroDBSensor(
        event_type="zerodb.file.uploaded",
        api_key="test-key",
        project_id="test-project",
    )


# ---------------------------------------------------------------------------
# ZeroDBEvent
# ---------------------------------------------------------------------------

class TestZeroDBEvent:
    def test_from_dict_standard_keys(self):
        d = {
            "event_type": "zerodb.file.uploaded",
            "event_id": "evt-1",
            "data": {"file": "a.txt"},
            "timestamp": "2026-06-10T00:00:00Z",
            "project_id": "proj-1",
            "metadata": {"source": "test"},
        }
        event = ZeroDBEvent.from_dict(d)
        assert event.event_type == "zerodb.file.uploaded"
        assert event.event_id == "evt-1"
        assert event.data == {"file": "a.txt"}
        assert event.timestamp == "2026-06-10T00:00:00Z"
        assert event.project_id == "proj-1"
        assert event.metadata == {"source": "test"}

    def test_from_dict_alternate_keys(self):
        d = {"type": "zerodb.table.insert", "id": "evt-2", "payload": {"row": 1}}
        event = ZeroDBEvent.from_dict(d)
        assert event.event_type == "zerodb.table.insert"
        assert event.event_id == "evt-2"
        assert event.data == {"row": 1}

    def test_from_dict_empty(self):
        event = ZeroDBEvent.from_dict({})
        assert event.event_type == ""
        assert event.event_id == ""
        assert event.data == {}

    def test_to_dict(self):
        event = ZeroDBEvent(event_type="zerodb.custom", event_id="e1", data={"k": "v"})
        d = event.to_dict()
        assert d["event_type"] == "zerodb.custom"
        assert d["event_id"] == "e1"
        assert d["data"] == {"k": "v"}

    def test_defaults(self):
        event = ZeroDBEvent()
        assert event.event_type == ""
        assert event.data == {}
        assert event.metadata == {}


# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------

class TestConstructor:
    def test_event_type(self, sensor):
        assert sensor.event_type == "zerodb.file.uploaded"

    def test_not_running_initially(self, sensor):
        assert sensor.is_running is False

    def test_no_handlers_initially(self, sensor):
        assert sensor.handler_count == 0

    def test_no_last_event_id(self, sensor):
        assert sensor.last_event_id is None

    def test_custom_poll_interval(self, mock_session):
        s = ZeroDBSensor(
            event_type="zerodb.table.insert",
            api_key="k",
            project_id="p",
            poll_interval=10,
        )
        assert s._poll_interval == 10

    def test_custom_batch_size(self, mock_session):
        s = ZeroDBSensor(
            event_type="zerodb.table.insert",
            api_key="k",
            project_id="p",
            batch_size=50,
        )
        assert s._batch_size == 50

    def test_custom_base_url(self, mock_session):
        s = ZeroDBSensor(
            event_type="zerodb.table.insert",
            api_key="k",
            project_id="p",
            base_url="https://custom.example.com",
        )
        assert s._base_url == "https://custom.example.com"


# ---------------------------------------------------------------------------
# Handler registration
# ---------------------------------------------------------------------------

class TestHandlerRegistration:
    def test_on_event_decorator(self, sensor):
        @sensor.on_event
        def handler(event):
            pass
        assert sensor.handler_count == 1

    def test_add_handler(self, sensor):
        def handler(event):
            pass
        sensor.add_handler(handler)
        assert sensor.handler_count == 1

    def test_multiple_handlers(self, sensor):
        @sensor.on_event
        def h1(event):
            pass

        @sensor.on_event
        def h2(event):
            pass

        assert sensor.handler_count == 2

    def test_decorator_returns_original_func(self, sensor):
        def handler(event):
            return "test"

        decorated = sensor.on_event(handler)
        assert decorated is handler


# ---------------------------------------------------------------------------
# Polling
# ---------------------------------------------------------------------------

class TestPolling:
    def test_poll_returns_events(self, sensor, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "events": [
                {
                    "event_type": "zerodb.file.uploaded",
                    "event_id": "evt-1",
                    "data": {"file": "test.txt"},
                    "timestamp": "2026-06-10T00:00:00Z",
                }
            ]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_resp

        events = sensor.poll()
        assert len(events) == 1
        assert events[0].event_type == "zerodb.file.uploaded"
        assert events[0].event_id == "evt-1"

    def test_poll_updates_cursor(self, sensor, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "events": [
                {"event_type": "zerodb.file.uploaded", "event_id": "evt-99", "data": {}},
            ]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_resp

        sensor.poll()
        assert sensor.last_event_id == "evt-99"

    def test_poll_empty_events(self, sensor, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"events": []}
        mock_resp.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_resp

        events = sensor.poll()
        assert events == []

    def test_poll_error_returns_empty(self, sensor, mock_session):
        import requests
        mock_session.get.side_effect = requests.RequestException("timeout")

        events = sensor.poll()
        assert events == []

    def test_poll_list_format(self, sensor, mock_session):
        """Some APIs return a raw list instead of {events: [...]}."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = [
            {"event_type": "zerodb.file.uploaded", "event_id": "evt-list", "data": {}}
        ]
        mock_resp.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_resp

        events = sensor.poll()
        assert len(events) == 1
        assert events[0].event_id == "evt-list"

    def test_poll_sends_after_cursor(self, sensor, mock_session):
        sensor._last_event_id = "cursor-123"
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"events": []}
        mock_resp.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_resp

        sensor.poll()
        call_args = mock_session.get.call_args
        assert call_args[1]["params"]["after"] == "cursor-123"

    def test_poll_multiple_events_updates_cursor_to_last(self, sensor, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "events": [
                {"event_type": "zerodb.file.uploaded", "event_id": "evt-1", "data": {}},
                {"event_type": "zerodb.file.uploaded", "event_id": "evt-2", "data": {}},
                {"event_type": "zerodb.file.uploaded", "event_id": "evt-3", "data": {}},
            ]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_session.get.return_value = mock_resp

        events = sensor.poll()
        assert len(events) == 3
        assert sensor.last_event_id == "evt-3"


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

class TestDispatch:
    def test_dispatch_sync_handler(self, sensor):
        results = []

        @sensor.on_event
        def handler(event):
            results.append(event.event_id)
            return "ok"

        event = ZeroDBEvent(event_type="zerodb.file.uploaded", event_id="evt-1")
        dispatch_results = sensor._dispatch_event(event)
        assert results == ["evt-1"]
        assert dispatch_results == ["ok"]

    def test_dispatch_async_handler(self, sensor):
        results = []

        @sensor.on_event
        async def handler(event):
            results.append(event.event_id)
            return "async-ok"

        event = ZeroDBEvent(event_type="zerodb.file.uploaded", event_id="evt-2")
        dispatch_results = sensor._dispatch_event(event)
        assert results == ["evt-2"]
        assert dispatch_results == ["async-ok"]

    def test_dispatch_handler_error_returns_none(self, sensor):
        @sensor.on_event
        def bad_handler(event):
            raise ValueError("boom")

        event = ZeroDBEvent(event_type="zerodb.file.uploaded", event_id="evt-3")
        results = sensor._dispatch_event(event)
        assert results == [None]

    def test_dispatch_multiple_handlers(self, sensor):
        @sensor.on_event
        def h1(event):
            return "r1"

        @sensor.on_event
        def h2(event):
            return "r2"

        event = ZeroDBEvent(event_type="zerodb.file.uploaded", event_id="evt-4")
        results = sensor._dispatch_event(event)
        assert results == ["r1", "r2"]


# ---------------------------------------------------------------------------
# Webhook
# ---------------------------------------------------------------------------

class TestWebhook:
    def test_process_webhook_matching(self, sensor):
        results_capture = []

        @sensor.on_event
        def handler(event):
            results_capture.append(event.data)
            return "processed"

        payload = {
            "event_type": "zerodb.file.uploaded",
            "event_id": "evt-wh-1",
            "data": {"file": "upload.pdf"},
        }
        results = sensor.process_webhook(payload)
        assert results_capture == [{"file": "upload.pdf"}]
        assert results == ["processed"]

    def test_process_webhook_non_matching(self, sensor):
        @sensor.on_event
        def handler(event):
            return "should not run"

        payload = {
            "event_type": "zerodb.table.insert",
            "event_id": "evt-wh-2",
            "data": {},
        }
        results = sensor.process_webhook(payload)
        assert results == []

    def test_process_webhook_no_handlers(self, sensor):
        payload = {
            "event_type": "zerodb.file.uploaded",
            "event_id": "evt-wh-3",
            "data": {},
        }
        results = sensor.process_webhook(payload)
        assert results == []


# ---------------------------------------------------------------------------
# Start / Stop
# ---------------------------------------------------------------------------

class TestStartStop:
    def test_start_sets_running(self, sensor):
        sensor.start()
        assert sensor.is_running is True
        sensor.stop()

    def test_stop_clears_running(self, sensor):
        sensor.start()
        sensor.stop()
        assert sensor.is_running is False

    def test_start_idempotent(self, sensor):
        sensor.start()
        sensor.start()
        assert sensor.is_running is True
        sensor.stop()

    def test_start_returns_self(self, sensor):
        result = sensor.start()
        assert result is sensor
        sensor.stop()
