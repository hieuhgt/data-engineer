import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class SnowflakeLoader:
    """
    Idempotent loader for Snowflake using MERGE INTO.

    Requires: snowflake-connector-python
    """

    def __init__(self, connection_params: Dict[str, str]):
        self.params = connection_params

    def _connect(self):
        import snowflake.connector

        return snowflake.connector.connect(
            account=self.params["account"],
            user=self.params["user"],
            password=self.params["password"],
            database=self.params["database"],
            schema=self.params.get("schema", "PUBLIC"),
        )

    def merge(
        self,
        df_or_path: Any,
        target_table: str,
        merge_keys: List[str],
        columns: List[str],
    ) -> Dict[str, int]:
        """
        Stage data to Snowflake internal stage then MERGE INTO target table.
        Returns {"rows_inserted": int, "rows_updated": int}.
        """
        import pandas as pd
        import tempfile

        if isinstance(df_or_path, str):
            df = pd.read_parquet(df_or_path)
        else:
            df = pd.DataFrame(df_or_path)

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            df.to_parquet(tmp.name, index=False)
            stage_path = tmp.name

        conn = self._connect()
        cur = conn.cursor()

        try:
            stage = f"@%{target_table}"
            cur.execute(f"PUT file://{stage_path} {stage} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")

            # Build MERGE SQL
            match_cond = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
            update_set = ", ".join(f"t.{c} = s.{c}" for c in columns if c not in merge_keys)
            insert_cols = ", ".join(columns)
            insert_vals = ", ".join(f"s.{c}" for c in columns)

            merge_sql = f"""
            MERGE INTO {target_table} t
            USING (SELECT * FROM {stage}) s
            ON {match_cond}
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """
            cur.execute(merge_sql)

            result = cur.fetchone()
            rows_inserted = result[0] if result else 0
            rows_updated = result[1] if result else 0

            conn.commit()
            logger.info(f"[{target_table}] Merged: {rows_inserted} inserted, {rows_updated} updated")
            return {"rows_inserted": rows_inserted, "rows_updated": rows_updated}

        finally:
            cur.close()
            conn.close()
