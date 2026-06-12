"""
ZeroDBCredentials -- Manages ZeroDB API key and session for Prefect flows.

    from zerodb_prefect import ZeroDBCredentials

    creds = ZeroDBCredentials(api_key='zdb_...', project_id='proj_...')
    client = creds.get_client()
"""

import requests

from zerodb_prefect.provision import resolve_credentials


class ZeroDBCredentials:
    """Manages ZeroDB API credentials and provides an authenticated client.

    Args:
        api_key: ZeroDB API key (auto-resolved if not provided).
        project_id: ZeroDB project ID.
        base_url: ZeroDB API base URL.
    """

    def __init__(self, api_key=None, project_id=None, base_url=None):
        self._api_key, self._project_id, self._base_url = resolve_credentials(
            api_key=api_key,
            project_id=project_id,
        )
        if base_url:
            self._base_url = base_url

    @property
    def api_key(self):
        """The resolved API key."""
        return self._api_key

    @property
    def project_id(self):
        """The resolved project ID."""
        return self._project_id

    @property
    def base_url(self):
        """The ZeroDB API base URL."""
        return self._base_url

    def get_client(self):
        """Return an authenticated requests.Session for ZeroDB API calls.

        Returns:
            requests.Session with auth headers set.
        """
        session = requests.Session()
        session.headers.update({
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._api_key}",
            "X-Project-ID": self._project_id,
        })
        return session

    def get_headers(self):
        """Return auth headers as a dict (useful for one-off requests).

        Returns:
            dict of HTTP headers.
        """
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._api_key}",
            "X-Project-ID": self._project_id,
        }
