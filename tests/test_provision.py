"""
Tests for zerodb-prefect auto-provisioning.
"""

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

import pytest

os.environ.setdefault("ZERODB_API_KEY", "test-key-000")
os.environ.setdefault("ZERODB_PROJECT_ID", "test-project-000")

from zerodb_prefect.provision import (
    resolve_credentials,
    _load_config_file,
    _save_config_file,
    _auto_provision,
    ZERODB_API_BASE,
)


class TestResolveCredentials:
    def test_explicit_args(self):
        key, proj, url = resolve_credentials(api_key="my-key", project_id="my-proj")
        assert key == "my-key"
        assert proj == "my-proj"
        assert url == ZERODB_API_BASE or "ZERODB_BASE_URL" in os.environ

    def test_env_vars(self):
        with patch.dict(os.environ, {"ZERODB_API_KEY": "env-key", "ZERODB_PROJECT_ID": "env-proj"}):
            key, proj, url = resolve_credentials()
            assert key == "env-key"
            assert proj == "env-proj"

    def test_custom_base_url_from_env(self):
        with patch.dict(os.environ, {
            "ZERODB_API_KEY": "k",
            "ZERODB_PROJECT_ID": "p",
            "ZERODB_BASE_URL": "https://custom.url",
        }):
            _, _, url = resolve_credentials()
            assert url == "https://custom.url"

    @patch("zerodb_prefect.provision._load_config_file")
    def test_config_file_fallback(self, mock_load):
        mock_load.return_value = ("file-key", "file-proj")
        with patch.dict(os.environ, {}, clear=True):
            # Clear env vars to force config file path
            env_backup_key = os.environ.pop("ZERODB_API_KEY", None)
            env_backup_proj = os.environ.pop("ZERODB_PROJECT_ID", None)
            try:
                key, proj, _ = resolve_credentials()
                assert key == "file-key"
                assert proj == "file-proj"
            finally:
                if env_backup_key:
                    os.environ["ZERODB_API_KEY"] = env_backup_key
                if env_backup_proj:
                    os.environ["ZERODB_PROJECT_ID"] = env_backup_proj

    @patch("zerodb_prefect.provision._auto_provision")
    @patch("zerodb_prefect.provision._load_config_file")
    def test_auto_provision_fallback(self, mock_load, mock_provision):
        mock_load.return_value = (None, None)
        mock_provision.return_value = ("auto-key", "auto-proj")
        env_backup_key = os.environ.pop("ZERODB_API_KEY", None)
        env_backup_proj = os.environ.pop("ZERODB_PROJECT_ID", None)
        try:
            key, proj, _ = resolve_credentials()
            assert key == "auto-key"
            assert proj == "auto-proj"
            mock_provision.assert_called_once()
        finally:
            if env_backup_key:
                os.environ["ZERODB_API_KEY"] = env_backup_key
            if env_backup_proj:
                os.environ["ZERODB_PROJECT_ID"] = env_backup_proj


class TestAutoProvision:
    @patch("zerodb_prefect.provision._save_config_file")
    @patch("zerodb_prefect.provision.requests.post")
    def test_auto_provision_success(self, mock_post, mock_save):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "api_key": "new-key",
            "project_id": "new-proj",
            "claim_url": "https://ainative.studio/claim/abc",
        }
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        key, proj = _auto_provision()
        assert key == "new-key"
        assert proj == "new-proj"
        mock_save.assert_called_once_with("new-key", "new-proj", "https://ainative.studio/claim/abc")

    @patch("zerodb_prefect.provision.requests.post")
    def test_auto_provision_http_error(self, mock_post):
        import requests
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError("503")
        mock_post.return_value = mock_resp

        with pytest.raises(requests.HTTPError):
            _auto_provision()

    @patch("zerodb_prefect.provision._save_config_file")
    @patch("zerodb_prefect.provision.requests.post")
    def test_auto_provision_no_claim_url(self, mock_post, mock_save):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "api_key": "k",
            "project_id": "p",
        }
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        key, proj = _auto_provision()
        assert key == "k"
        mock_save.assert_called_once_with("k", "p", None)


class TestConfigFile:
    @patch("zerodb_prefect.provision.CONFIG_FILE")
    def test_load_config_success(self, mock_path):
        mock_path.exists.return_value = True
        mock_path.read_text.return_value = json.dumps({"api_key": "fk", "project_id": "fp"})

        key, proj = _load_config_file()
        assert key == "fk"
        assert proj == "fp"

    @patch("zerodb_prefect.provision.CONFIG_FILE")
    def test_load_config_not_found(self, mock_path):
        mock_path.exists.return_value = False
        key, proj = _load_config_file()
        assert key is None
        assert proj is None

    @patch("zerodb_prefect.provision.CONFIG_FILE")
    def test_load_config_invalid_json(self, mock_path):
        mock_path.exists.return_value = True
        mock_path.read_text.return_value = "not json"
        key, proj = _load_config_file()
        assert key is None
        assert proj is None

    @patch("zerodb_prefect.provision.CONFIG_FILE")
    def test_load_config_missing_keys(self, mock_path):
        mock_path.exists.return_value = True
        mock_path.read_text.return_value = json.dumps({"api_key": "k"})
        key, proj = _load_config_file()
        assert key is None
        assert proj is None
