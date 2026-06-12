"""
Tests for zerodb-prefect ZeroDBBlock.

Uses mocked HTTP responses -- no real API calls.
"""

import json
import os
from unittest.mock import MagicMock, patch, call

import pytest

os.environ.setdefault("ZERODB_API_KEY", "test-key-000")
os.environ.setdefault("ZERODB_PROJECT_ID", "test-project-000")

from zerodb_prefect.block import ZeroDBBlock


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_session():
    with patch("zerodb_prefect.block.requests.Session") as MockSession:
        session = MagicMock()
        MockSession.return_value = session
        yield session


@pytest.fixture
def block(mock_session):
    return ZeroDBBlock(api_key="test-key", project_id="test-project")


# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------

class TestConstructor:
    def test_default_table(self, block):
        assert block.table == "prefect_results"

    def test_custom_table(self, mock_session):
        b = ZeroDBBlock(api_key="k", project_id="p", table="my_results")
        assert b.table == "my_results"

    def test_custom_base_url(self, mock_session):
        b = ZeroDBBlock(api_key="k", project_id="p", base_url="https://custom.api")
        assert b._base_url == "https://custom.api"


# ---------------------------------------------------------------------------
# write_path
# ---------------------------------------------------------------------------

class TestWritePath:
    def test_write_dict(self, block, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"inserted": 1}
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        result = block.write_path("/results/run-1", {"status": "done"})
        assert result == {"inserted": 1}

        # Verify request body
        call_args = mock_session.post.call_args
        body = call_args[1]["json"]
        assert body["table"] == "prefect_results"
        assert body["rows"][0]["path"] == "/results/run-1"
        assert json.loads(body["rows"][0]["content"]) == {"status": "done"}
        assert body["rows"][0]["content_type"] == "dict"

    def test_write_string(self, block, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"inserted": 1}
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        block.write_path("/results/text", "hello world")
        body = mock_session.post.call_args[1]["json"]
        assert body["rows"][0]["content"] == "hello world"
        assert body["rows"][0]["content_type"] == "str"

    def test_write_bytes(self, block, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"inserted": 1}
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        block.write_path("/results/binary", b"raw bytes")
        body = mock_session.post.call_args[1]["json"]
        assert body["rows"][0]["content"] == "raw bytes"
        assert body["rows"][0]["content_type"] == "bytes"

    def test_write_list(self, block, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"inserted": 1}
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        block.write_path("/results/list", [1, 2, 3])
        body = mock_session.post.call_args[1]["json"]
        assert json.loads(body["rows"][0]["content"]) == [1, 2, 3]
        assert body["rows"][0]["content_type"] == "list"

    def test_write_int(self, block, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"inserted": 1}
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        block.write_path("/results/num", 42)
        body = mock_session.post.call_args[1]["json"]
        assert body["rows"][0]["content"] == "42"
        assert body["rows"][0]["content_type"] == "int"

    def test_write_raises_on_http_error(self, block, mock_session):
        import requests
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError("500")
        mock_session.post.return_value = mock_resp

        with pytest.raises(requests.HTTPError):
            block.write_path("/fail", {"x": 1})


# ---------------------------------------------------------------------------
# read_path
# ---------------------------------------------------------------------------

class TestReadPath:
    def test_read_dict(self, block, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "rows": [{"path": "/r/1", "content": '{"status": "done"}', "content_type": "dict"}]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        result = block.read_path("/r/1")
        assert result == {"status": "done"}

    def test_read_list(self, block, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "rows": [{"path": "/r/2", "content": "[1, 2, 3]", "content_type": "list"}]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        result = block.read_path("/r/2")
        assert result == [1, 2, 3]

    def test_read_string(self, block, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "rows": [{"path": "/r/3", "content": "hello", "content_type": "str"}]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        result = block.read_path("/r/3")
        assert result == "hello"

    def test_read_not_found(self, block, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"rows": []}
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        result = block.read_path("/missing")
        assert result is None

    def test_read_data_key_fallback(self, block, mock_session):
        """Some responses use 'data' instead of 'rows'."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "data": [{"path": "/r/4", "content": '42', "content_type": "int"}]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        result = block.read_path("/r/4")
        assert result == 42

    def test_read_invalid_json_returns_raw(self, block, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "rows": [{"path": "/r/5", "content": "not-json{", "content_type": "dict"}]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        result = block.read_path("/r/5")
        assert result == "not-json{"

    def test_read_bool_type(self, block, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "rows": [{"path": "/r/6", "content": "true", "content_type": "bool"}]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        result = block.read_path("/r/6")
        assert result is True

    def test_read_float_type(self, block, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "rows": [{"path": "/r/7", "content": "3.14", "content_type": "float"}]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        result = block.read_path("/r/7")
        assert result == 3.14


# ---------------------------------------------------------------------------
# exists
# ---------------------------------------------------------------------------

class TestExists:
    def test_exists_true(self, block, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"rows": [{"path": "/r/1"}]}
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        assert block.exists("/r/1") is True

    def test_exists_false(self, block, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"rows": []}
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        assert block.exists("/missing") is False

    def test_exists_error_returns_false(self, block, mock_session):
        import requests
        mock_session.post.side_effect = requests.RequestException("fail")
        assert block.exists("/err") is False


# ---------------------------------------------------------------------------
# delete_path
# ---------------------------------------------------------------------------

class TestDeletePath:
    def test_delete(self, block, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"deleted": 1}
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        result = block.delete_path("/r/1")
        assert result == {"deleted": 1}

        body = mock_session.post.call_args[1]["json"]
        assert body["table"] == "prefect_results"
        assert body["filters"][0]["value"] == "/r/1"


# ---------------------------------------------------------------------------
# list_paths
# ---------------------------------------------------------------------------

class TestListPaths:
    def test_list_all(self, block, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "rows": [{"path": "/a"}, {"path": "/b"}, {"path": "/c"}]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        paths = block.list_paths()
        assert paths == ["/a", "/b", "/c"]

    def test_list_with_prefix(self, block, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"rows": [{"path": "/results/a"}]}
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        paths = block.list_paths(prefix="/results/")
        body = mock_session.post.call_args[1]["json"]
        assert body["filters"][0]["op"] == "startswith"
        assert body["filters"][0]["value"] == "/results/"

    def test_list_empty(self, block, mock_session):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"rows": []}
        mock_resp.raise_for_status = MagicMock()
        mock_session.post.return_value = mock_resp

        paths = block.list_paths()
        assert paths == []
