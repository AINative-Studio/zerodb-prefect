"""
ZeroDBBlock -- Store and retrieve Prefect flow results in ZeroDB.

    from zerodb_prefect import ZeroDBBlock

    block = ZeroDBBlock()
    block.write_path('/results/my-flow/run-1', {'status': 'done', 'rows': 42})
    data = block.read_path('/results/my-flow/run-1')
"""

import json
import logging

import requests

from zerodb_prefect.provision import resolve_credentials

logger = logging.getLogger(__name__)


class ZeroDBBlock:
    """Stores and retrieves Prefect results in a ZeroDB table.

    Works as a Prefect result storage backend. Data is serialized to JSON
    and stored in a ZeroDB NoSQL table keyed by path.

    Args:
        api_key: ZeroDB API key (auto-resolved if not provided).
        project_id: ZeroDB project ID.
        base_url: ZeroDB API base URL.
        table: Table name for result storage (default 'prefect_results').
    """

    def __init__(self, api_key=None, project_id=None, base_url=None,
                 table="prefect_results"):
        self._api_key, self._project_id, self._base_url = resolve_credentials(
            api_key=api_key,
            project_id=project_id,
        )
        if base_url:
            self._base_url = base_url

        self._table = table

        self._session = requests.Session()
        self._session.headers.update({
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._api_key}",
            "X-Project-ID": self._project_id,
        })

    @property
    def table(self):
        """The ZeroDB table used for storage."""
        return self._table

    def write_path(self, path, content):
        """Write content to a ZeroDB table row keyed by path.

        Args:
            path: Unique path identifier (e.g. '/results/flow/run-1').
            content: Any JSON-serializable object.

        Returns:
            dict with the stored row data from ZeroDB.

        Raises:
            requests.HTTPError: If the API call fails.
        """
        if isinstance(content, (bytes, bytearray)):
            serialized = content.decode("utf-8", errors="replace")
        elif isinstance(content, str):
            serialized = content
        else:
            serialized = json.dumps(content)

        row = {
            "path": path,
            "content": serialized,
            "content_type": type(content).__name__,
        }

        resp = self._session.post(
            f"{self._base_url}/api/v1/public/tables/insert",
            json={"table": self._table, "rows": [row]},
        )
        resp.raise_for_status()
        return resp.json()

    def read_path(self, path):
        """Read content from a ZeroDB table row by path.

        Args:
            path: Path identifier to look up.

        Returns:
            The deserialized content, or None if not found.

        Raises:
            requests.HTTPError: If the API call fails (non-404).
        """
        body = {
            "table": self._table,
            "filters": [{"column": "path", "op": "eq", "value": path}],
            "limit": 1,
        }

        resp = self._session.post(
            f"{self._base_url}/api/v1/public/tables/query",
            json=body,
        )
        resp.raise_for_status()
        result = resp.json()

        rows = result.get("rows", result.get("data", []))
        if not rows:
            return None

        row = rows[0]
        raw = row.get("content", "")
        content_type = row.get("content_type", "str")

        # Try to deserialize JSON content
        if content_type in ("dict", "list", "int", "float", "bool"):
            try:
                return json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                return raw

        return raw

    def exists(self, path):
        """Check if a path exists in storage.

        Args:
            path: Path identifier to check.

        Returns:
            True if the path exists, False otherwise.
        """
        body = {
            "table": self._table,
            "filters": [{"column": "path", "op": "eq", "value": path}],
            "limit": 1,
        }

        try:
            resp = self._session.post(
                f"{self._base_url}/api/v1/public/tables/query",
                json=body,
            )
            resp.raise_for_status()
            result = resp.json()
            rows = result.get("rows", result.get("data", []))
            return len(rows) > 0
        except requests.RequestException:
            return False

    def delete_path(self, path):
        """Delete a stored result by path.

        Args:
            path: Path identifier to delete.

        Returns:
            dict with deletion result.
        """
        resp = self._session.post(
            f"{self._base_url}/api/v1/public/tables/delete",
            json={
                "table": self._table,
                "filters": [{"column": "path", "op": "eq", "value": path}],
            },
        )
        resp.raise_for_status()
        return resp.json()

    def list_paths(self, prefix=None, limit=100):
        """List stored paths, optionally filtering by prefix.

        Args:
            prefix: Optional path prefix to filter by.
            limit: Max results to return.

        Returns:
            list of path strings.
        """
        body = {"table": self._table, "limit": limit}
        if prefix:
            body["filters"] = [{"column": "path", "op": "startswith", "value": prefix}]

        resp = self._session.post(
            f"{self._base_url}/api/v1/public/tables/query",
            json=body,
        )
        resp.raise_for_status()
        result = resp.json()
        rows = result.get("rows", result.get("data", []))
        return [row.get("path", "") for row in rows]
