import pytest
from src.validation.quality_gates import (
    NullCheckGate,
    BusinessRuleGate,
    QualityGateChecker,
)
from src.validation.deduplication import Deduplicator
from src.validation.schema_validator import SchemaValidator


class TestNullCheckGate:
    def test_passes_when_no_nulls(self):
        gate = NullCheckGate(required_fields=["id", "email"])
        result = gate.validate([{"id": 1, "email": "a@b.com"}, {"id": 2, "email": "c@d.com"}])
        assert result.passed is True

    def test_fails_when_null_present(self):
        gate = NullCheckGate(required_fields=["id", "email"], threshold=1.0)
        result = gate.validate([{"id": 1, "email": None}])
        assert result.passed is False
        assert result.fail_count == 1

    def test_passes_empty_data(self):
        gate = NullCheckGate(required_fields=["id"])
        result = gate.validate([])
        assert result.passed is True


class TestBusinessRuleGate:
    def test_positive_amount_passes(self):
        gate = BusinessRuleGate(rules={"positive": "amount > 0"})
        result = gate.validate([{"amount": 100}, {"amount": 50}])
        assert result.passed is True

    def test_negative_amount_fails(self):
        gate = BusinessRuleGate(rules={"positive": "amount > 0"}, threshold=1.0)
        result = gate.validate([{"amount": -5}])
        assert result.passed is False


class TestQualityGateChecker:
    def test_all_pass(self):
        gates = [NullCheckGate(required_fields=["id"])]
        checker = QualityGateChecker(gates)
        _, results = checker.validate_all([{"id": 1}])
        assert all(r.passed for r in results)

    def test_critical_gate_raises(self):
        gate = NullCheckGate(required_fields=["id"], threshold=1.0, alert_level="critical")
        checker = QualityGateChecker([gate])
        with pytest.raises(ValueError, match="Critical quality gate failed"):
            checker.validate_all([{"id": None}])

    def test_quality_score_perfect(self):
        gate = NullCheckGate(required_fields=["id"])
        checker = QualityGateChecker([gate])
        checker.validate_all([{"id": 1}])
        assert checker.get_quality_score() == 1.0


class TestDeduplicator:
    def test_removes_exact_duplicates(self):
        dedup = Deduplicator(key_fields=["id", "source"])
        data = [
            {"id": 1, "source": "api"},
            {"id": 1, "source": "api"},  # duplicate
            {"id": 2, "source": "api"},
        ]
        result = dedup.deduplicate(data)
        assert len(result) == 2

    def test_keeps_unique(self):
        dedup = Deduplicator(key_fields=["id"])
        data = [{"id": 1}, {"id": 2}, {"id": 3}]
        assert len(dedup.deduplicate(data)) == 3


class TestSchemaValidator:
    def test_valid_records(self):
        validator = SchemaValidator({"id": "integer", "name": "string"})
        valid, invalid = validator.validate_batch([{"id": 1, "name": "Alice"}])
        assert len(valid) == 1
        assert len(invalid) == 0
