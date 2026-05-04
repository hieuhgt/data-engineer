import logging
from typing import Any, Dict, List, Tuple

import jsonschema

logger = logging.getLogger(__name__)

# Map config type strings to JSON-Schema types
_TYPE_MAP = {
    "string": "string",
    "integer": "integer",
    "decimal": "number",
    "float": "number",
    "boolean": "boolean",
    "timestamp": "string",  # stored as string for validation purposes
    "date": "string",
}


def build_jsonschema(schema_config: Dict[str, str]) -> Dict:
    """Convert pipeline_config.yaml schema dict to JSON Schema."""
    properties = {
        col: {"type": _TYPE_MAP.get(typ, "string")}
        for col, typ in schema_config.items()
    }
    return {
        "type": "object",
        "properties": properties,
        "additionalProperties": True,
    }


class SchemaValidator:
    """Validates each record matches the expected schema."""

    def __init__(self, schema_config: Dict[str, str]):
        self.json_schema = build_jsonschema(schema_config)

    def validate_record(self, record: Dict) -> Tuple[bool, str | None]:
        try:
            jsonschema.validate(record, self.json_schema)
            return True, None
        except jsonschema.ValidationError as e:
            return False, e.message

    def validate_batch(self, data: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        valid, invalid = [], []
        for record in data:
            ok, error = self.validate_record(record)
            if ok:
                valid.append(record)
            else:
                invalid.append({**record, "_validation_error": error})
        if invalid:
            logger.warning(f"Schema validation: {len(invalid)}/{len(data)} records invalid")
        return valid, invalid
