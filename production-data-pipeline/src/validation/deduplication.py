import hashlib
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)


class Deduplicator:
    """Remove duplicate records using hash-based detection."""

    def __init__(self, key_fields: List[str]):
        self.key_fields = key_fields

    def _record_hash(self, record: Dict) -> str:
        key = "|".join(str(record.get(f, "")) for f in self.key_fields)
        return hashlib.sha256(key.encode()).hexdigest()

    def deduplicate(self, records: List[Dict]) -> List[Dict]:
        seen: set[str] = set()
        unique: List[Dict] = []
        for record in records:
            h = self._record_hash(record)
            if h not in seen:
                unique.append(record)
                seen.add(h)
        removed = len(records) - len(unique)
        if removed:
            logger.info(f"Deduplication: removed {removed} duplicates ({len(unique)} unique remain)")
        return unique
