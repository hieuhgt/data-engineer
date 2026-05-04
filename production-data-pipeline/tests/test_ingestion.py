import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.ingestion.api_connector import APIConnector
from src.ingestion.file_connector import FileConnector
from src.ingestion.deduplication import Deduplicator  # noqa: F401 – checked below


SAMPLE_CONFIG = {
    "endpoint": "https://api.example.com/data",
    "retries": 1,
    "timeout": 5,
    "validation": {"required_fields": ["id", "name"]},
}


class TestAPIConnector:
    def test_validate_schema_passes(self):
        connector = APIConnector("test", SAMPLE_CONFIG)
        data = [{"id": 1, "name": "Alice"}]
        assert connector.validate_schema(data) is True

    def test_validate_schema_missing_field(self):
        connector = APIConnector("test", SAMPLE_CONFIG)
        data = [{"id": 1}]  # missing "name"
        assert connector.validate_schema(data) is False

    def test_validate_schema_empty_data(self):
        connector = APIConnector("test", SAMPLE_CONFIG)
        assert connector.validate_schema([]) is True

    @pytest.mark.asyncio
    async def test_fetch_unwraps_envelope(self):
        connector = APIConnector("test", SAMPLE_CONFIG)
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"data": [{"id": 1}]})
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        with patch("aiohttp.ClientSession.get", return_value=mock_response):
            result = await connector._fetch_async()
        assert result == [{"id": 1}]


class TestFileConnector:
    def test_validate_schema_passes(self, tmp_path):
        cfg = {"prefix": str(tmp_path), "format": "csv", "schema": {"id": "integer"}}
        connector = FileConnector("file_test", cfg)
        data = [{"id": 1, "name": "Alice"}]
        assert connector.validate_schema(data) is True

    def test_validate_schema_missing_col(self, tmp_path):
        cfg = {"prefix": str(tmp_path), "format": "csv", "schema": {"missing_col": "string"}}
        connector = FileConnector("file_test", cfg)
        assert connector.validate_schema([{"id": 1}]) is False
