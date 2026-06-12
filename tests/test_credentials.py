"""
Tests for zerodb-prefect ZeroDBCredentials.
"""

import os
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("ZERODB_API_KEY", "test-key-000")
os.environ.setdefault("ZERODB_PROJECT_ID", "test-project-000")

from zerodb_prefect.credentials import ZeroDBCredentials


@pytest.fixture
def creds():
    return ZeroDBCredentials(api_key="test-key", project_id="test-project")


class TestCredentials:
    def test_api_key(self, creds):
        assert creds.api_key == "test-key"

    def test_project_id(self, creds):
        assert creds.project_id == "test-project"

    def test_base_url_default(self, creds):
        assert "ainative.studio" in creds.base_url

    def test_custom_base_url(self):
        c = ZeroDBCredentials(api_key="k", project_id="p", base_url="https://custom.api")
        assert c.base_url == "https://custom.api"

    def test_get_client_returns_session(self, creds):
        client = creds.get_client()
        assert hasattr(client, "get")
        assert hasattr(client, "post")
        assert "Bearer test-key" in client.headers.get("Authorization", "")

    def test_get_client_has_project_header(self, creds):
        client = creds.get_client()
        assert client.headers.get("X-Project-ID") == "test-project"

    def test_get_headers(self, creds):
        headers = creds.get_headers()
        assert headers["Authorization"] == "Bearer test-key"
        assert headers["X-Project-ID"] == "test-project"
        assert headers["Content-Type"] == "application/json"

    def test_get_client_returns_new_session_each_call(self, creds):
        c1 = creds.get_client()
        c2 = creds.get_client()
        assert c1 is not c2
