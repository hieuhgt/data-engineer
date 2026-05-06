# Data Quality Gates - Prevent bad data from reaching warehouse

import logging
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass
import json

logger = logging.getLogger(__name__)

@dataclass
class ValidationResult:
    """Result of validation gate"""
    passed: bool
    gate_name: str
    pass_count: int
    fail_count: int
    pass_rate: float
    threshold: float
    errors: List[str]

    def __repr__(self):
        return f"ValidationResult(gate={self.gate_name}, pass_rate={self.pass_rate:.1%}, threshold={self.threshold:.1%})"


class QualityGate:
    """Base quality gate"""

    def __init__(self, name: str, threshold: float = 0.95, alert_level: str = "warning"):
        self.name = name
        self.threshold = threshold
        self.alert_level = alert_level

    def validate(self, data: List[Dict]) -> ValidationResult:
        """Validate data against this gate"""
        pass

    def log_result(self, result: ValidationResult):
        """Log validation result"""
        if result.passed:
            logger.info(f"✓ {result.gate_name}: {result.pass_rate:.1%} passed (threshold: {result.threshold:.1%})")
        else:
            level = logging.ERROR if self.alert_level == "critical" else logging.WARNING
            logger.log(level, f"✗ {result.gate_name}: {result.pass_rate:.1%} passed, below threshold {result.threshold:.1%}")
            if result.errors:
                logger.log(level, f"  Errors: {result.errors[:5]}")  # Log first 5 errors


class SchemaValidationGate(QualityGate):
    """Validate schema matches expected"""

    def __init__(self, expected_schema: Dict[str, str], **kwargs):
        super().__init__("schema_validation", threshold=1.0, **kwargs)
        self.expected_schema = expected_schema

    def validate(self, data: List[Dict]) -> ValidationResult:
        """Check all records have expected columns"""
        if not data:
            return ValidationResult(
                passed=True,
                gate_name=self.name,
                pass_count=0,
                fail_count=0,
                pass_rate=1.0,
                threshold=self.threshold,
                errors=[]
            )

        errors = []
        fail_count = 0

        for i, record in enumerate(data):
            for field in self.expected_schema:
                if field not in record:
                    fail_count += 1
                    if len(errors) < 5:
                        errors.append(f"Row {i}: Missing field '{field}'")

        pass_count = len(data) - fail_count
        pass_rate = pass_count / len(data) if data else 0

        result = ValidationResult(
            passed=pass_rate >= self.threshold,
            gate_name=self.name,
            pass_count=pass_count,
            fail_count=fail_count,
            pass_rate=pass_rate,
            threshold=self.threshold,
            errors=errors
        )

        self.log_result(result)
        return result


class NullCheckGate(QualityGate):
    """Validate required fields are not null"""

    def __init__(self, required_fields: List[str], **kwargs):
        super().__init__("null_check", threshold=0.99, **kwargs)
        self.required_fields = required_fields

    def validate(self, data: List[Dict]) -> ValidationResult:
        """Check for nulls in required fields"""
        if not data:
            return ValidationResult(
                passed=True,
                gate_name=self.name,
                pass_count=0,
                fail_count=0,
                pass_rate=1.0,
                threshold=self.threshold,
                errors=[]
            )

        errors = []
        fail_count = 0

        for i, record in enumerate(data):
            record_failed = False
            for field in self.required_fields:
                if record.get(field) is None:
                    record_failed = True
                    if len(errors) < 5:
                        errors.append(f"Row {i}: Null in required field '{field}'")
            if record_failed:
                fail_count += 1

        pass_count = len(data) - fail_count
        pass_rate = pass_count / len(data)

        result = ValidationResult(
            passed=pass_rate >= self.threshold,
            gate_name=self.name,
            pass_count=pass_count,
            fail_count=fail_count,
            pass_rate=pass_rate,
            threshold=self.threshold,
            errors=errors
        )

        self.log_result(result)
        return result


class BusinessRuleGate(QualityGate):
    """Validate business rules (e.g., amount > 0)"""

    def __init__(self, rules: Dict[str, str], **kwargs):
        super().__init__("business_rules", threshold=0.95, **kwargs)
        self.rules = rules  # {"rule_name": "condition"}

    def validate(self, data: List[Dict]) -> ValidationResult:
        """Check business rules"""
        if not data:
            return ValidationResult(
                passed=True,
                gate_name=self.name,
                pass_count=0,
                fail_count=0,
                pass_rate=1.0,
                threshold=self.threshold,
                errors=[]
            )

        errors = []
        fail_count = 0

        for i, record in enumerate(data):
            for rule_name, condition in self.rules.items():
                try:
                    # Simple evaluation (in production, use safer evaluation)
                    # e.g., "amount > 0" → check record['amount'] > 0
                    if not self._evaluate_rule(record, condition):
                        fail_count += 1
                        if len(errors) < 5:
                            errors.append(f"Row {i}: Failed rule '{rule_name}'")
                except Exception as e:
                    fail_count += 1
                    if len(errors) < 5:
                        errors.append(f"Row {i}: Error evaluating '{rule_name}': {e}")

        pass_count = len(data) - fail_count
        pass_rate = pass_count / len(data) if data else 0

        result = ValidationResult(
            passed=pass_rate >= self.threshold,
            gate_name=self.name,
            pass_count=pass_count,
            fail_count=fail_count,
            pass_rate=pass_rate,
            threshold=self.threshold,
            errors=errors
        )

        self.log_result(result)
        return result

    def _evaluate_rule(self, record: Dict, condition: str) -> bool:
        """Evaluate condition for record"""
        # Simple evaluation: "amount > 0" → check amount
        # In production: Use safer evaluation (e.g., pandas eval, numexpr)
        import re

        # Parse condition like "amount > 0"
        match = re.match(r"(\w+)\s*([><=!]+)\s*(.+)", condition)
        if not match:
            raise ValueError(f"Invalid condition: {condition}")

        field, operator, value = match.groups()
        record_value = record.get(field)

        if record_value is None:
            return False

        # Safe evaluation
        if operator == '>':
            return record_value > float(value)
        elif operator == '<':
            return record_value < float(value)
        elif operator == '>=':
            return record_value >= float(value)
        elif operator == '<=':
            return record_value <= float(value)
        elif operator == '==':
            return record_value == float(value)
        elif operator == '!=':
            return record_value != float(value)

        raise ValueError(f"Unknown operator: {operator}")


class QualityGateChecker:
    """Orchestrates multiple quality gates"""

    def __init__(self, gates: List[QualityGate]):
        self.gates = gates
        self.results: List[ValidationResult] = []

    def validate_all(self, data: List[Dict]) -> Tuple[List[Dict], List[ValidationResult]]:
        """Run all gates, return valid data and results"""
        self.results = []
        valid_data = data

        for gate in self.gates:
            result = gate.validate(valid_data)
            self.results.append(result)

            if not result.passed and gate.alert_level == "critical":
                raise ValueError(
                    f"Critical quality gate failed: {gate.name} "
                    f"({result.pass_rate:.1%} < {result.threshold:.1%})"
                )

        # Log summary
        passed_gates = sum(1 for r in self.results if r.passed)
        total_gates = len(self.results)
        logger.info(f"Quality check: {passed_gates}/{total_gates} gates passed")

        return valid_data, self.results

    def get_quality_score(self) -> float:
        """Calculate overall quality score (0-1)"""
        if not self.results:
            return 1.0

        # Weight by importance
        score = sum(r.pass_rate for r in self.results) / len(self.results)
        return score

    def to_json(self) -> str:
        """Export results as JSON"""
        return json.dumps({
            'timestamp': str(__import__('datetime').datetime.now()),
            'quality_score': self.get_quality_score(),
            'gates': [
                {
                    'name': r.gate_name,
                    'passed': r.passed,
                    'pass_rate': f"{r.pass_rate:.1%}",
                    'threshold': f"{r.threshold:.1%}",
                    'pass_count': r.pass_count,
                    'fail_count': r.fail_count,
                    'errors': r.errors
                }
                for r in self.results
            ]
        }, indent=2)


# Factory function for creating gates from config
def create_gates_from_config(config: Dict[str, Any]) -> List[QualityGate]:
    """Create quality gates from YAML config"""
    gates = []

    for gate_config in config.get('quality_gates', []):
        gate_type = gate_config.get('type')
        name = gate_config.get('name')
        threshold = gate_config.get('threshold', 0.95)
        alert_level = gate_config.get('alert_level', 'warning')

        if gate_type == 'schema':
            # Schema validation configured separately
            pass
        elif gate_type == 'null_check':
            gate = NullCheckGate(
                required_fields=gate_config.get('required_columns', []),
                threshold=threshold,
                alert_level=alert_level
            )
            gates.append(gate)
        elif gate_type == 'custom':
            gate = BusinessRuleGate(
                rules={f"rule_{i}": rule for i, rule in enumerate(gate_config.get('rules', []))},
                threshold=threshold,
                alert_level=alert_level
            )
            gates.append(gate)

    return gates
