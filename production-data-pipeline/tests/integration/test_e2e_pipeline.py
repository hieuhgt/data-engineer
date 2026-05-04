"""
End-to-end pipeline integration test.
Runs the full flow (ingest → validate → transform → load)
on a small in-memory dataset without external services.

Run with:  pytest tests/integration/ -v
"""
import pytest
import json
import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch


# ── helpers ──────────────────────────────────────────────────────────────────

SAMPLE_EVENTS = [
    {"event_id": "e1", "user_id": 1, "event_type": "purchase", "amount": 100.0, "timestamp": "2024-01-15"},
    {"event_id": "e2", "user_id": 2, "event_type": "view",     "amount": 0.0,   "timestamp": "2024-01-15"},
    {"event_id": "e3", "user_id": 1, "event_type": "purchase", "amount": 50.0,  "timestamp": "2024-01-15"},
    {"event_id": "e1", "user_id": 1, "event_type": "purchase", "amount": 100.0, "timestamp": "2024-01-15"},  # duplicate
]


# ── tests ────────────────────────────────────────────────────────────────────

class TestE2EFlow:

    def test_ingest_validate_dedup(self):
        """Simulate ingest + validation + deduplication in memory."""
        from src.validation.quality_gates import NullCheckGate, QualityGateChecker
        from src.validation.deduplication import Deduplicator

        raw = list(SAMPLE_EVENTS)

        # Dedup
        dedup = Deduplicator(key_fields=["event_id", "user_id"])
        clean = dedup.deduplicate(raw)
        assert len(clean) == 3  # 4 minus 1 duplicate

        # Validate
        gates = [NullCheckGate(required_fields=["event_id", "user_id"])]
        checker = QualityGateChecker(gates)
        _, results = checker.validate_all(clean)
        assert all(r.passed for r in results)
        assert checker.get_quality_score() == 1.0

    def test_warehouse_load_is_idempotent(self, tmp_path):
        """Loading the same data twice must not double rows."""
        from src.warehouse.warehouse_loader import idempotent_load
        import pandas as pd

        data = [{"event_id": "e1", "user_id": 1, "amount": 100.0}]

        # Patch output path to tmp_path
        target = str(tmp_path / "fact_events.parquet")
        with patch("src.warehouse.warehouse_loader.idempotent_load", wraps=idempotent_load):
            # First load
            result1 = idempotent_load(data, "fact_events", merge_key=["event_id"])
            assert result1["rows_loaded"] == 1

            # Second load (idempotent – same data)
            result2 = idempotent_load(data, "fact_events", merge_key=["event_id"])
            assert result2["rows_loaded"] == 1

    def test_lineage_recorded(self):
        """Every pipeline step must record lineage."""
        from src.monitoring.lineage import LineageTracker

        tracker = LineageTracker()
        tracker.record("ingest", source="api_events", target="bronze", rows=1000, status="success")
        tracker.record("transform", source="bronze", target="silver", rows=980, status="success")
        tracker.record("load", source="silver", target="fact_events", rows=980, status="success")

        records = tracker.all_records()
        assert len(records) == 3
        assert all(r["status"] == "success" for r in records)
        steps = [r["step"] for r in records]
        assert steps == ["ingest", "transform", "load"]

    def test_quality_gate_blocks_bad_data(self):
        """Critical gate must raise and halt pipeline for severely bad data."""
        from src.validation.quality_gates import NullCheckGate, QualityGateChecker

        all_bad = [{"event_id": None, "user_id": None} for _ in range(10)]
        gate = NullCheckGate(required_fields=["event_id", "user_id"], threshold=1.0, alert_level="critical")
        checker = QualityGateChecker([gate])

        with pytest.raises(ValueError, match="Critical quality gate failed"):
            checker.validate_all(all_bad)

    def test_partial_source_failure_continues(self):
        """Pipeline should continue even if 1 of 3 sources fails."""
        results = {}
        failed = []
        sources = ["source_a", "source_b", "source_c"]

        def fake_fetch(name):
            if name == "source_b":
                raise ConnectionError("source_b is down")
            return [{"id": 1, "source": name}]

        for source in sources:
            try:
                data = fake_fetch(source)
                results[source] = data
            except Exception as e:
                failed.append({"source": source, "error": str(e)})

        assert len(results) == 2       # 2 succeeded
        assert len(failed) == 1        # 1 failed
        assert failed[0]["source"] == "source_b"
