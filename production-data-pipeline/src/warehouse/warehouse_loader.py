import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def idempotent_load(
    source_data: str | List[Dict],
    target_table: str,
    merge_key: List[str],
    connection_string: str | None = None,
) -> Dict[str, Any]:
    """
    Load data to warehouse using MERGE/UPSERT (idempotent – safe to retry).

    For the local demo this writes a Parquet snapshot to MinIO.
    In production swap this body for a Snowflake or BigQuery MERGE statement.
    """
    import pandas as pd

    if isinstance(source_data, str):
        # Source is a path (e.g. s3://processed/…)
        df = pd.read_parquet(source_data)
    else:
        df = pd.DataFrame(source_data)

    rows = len(df)
    logger.info(f"Loading {rows} rows into {target_table} (merge on {merge_key})")

    # --- DEMO: write Parquet locally; replace with real MERGE in production ---
    out_path = f"/tmp/warehouse/{target_table}.parquet"
    df.to_parquet(out_path, index=False)
    logger.info(f"Written to {out_path}")
    # -------------------------------------------------------------------------

    return {"table": target_table, "rows_loaded": rows, "merge_key": merge_key}
