import logging
import os
from io import BytesIO
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

    For the local demo this writes a Parquet snapshot to MinIO (warehouse bucket).
    In production swap this body for a Snowflake or BigQuery MERGE statement.
    """
    import boto3
    import pandas as pd
    from botocore.exceptions import ClientError

    if isinstance(source_data, str):
        raise ValueError("Pass data as a list of dicts, not a raw path. Read from MinIO first.")

    df = pd.DataFrame(source_data)

    # Serialize list/array columns to string so parquet stays portable
    for col in df.columns:
        if df[col].dtype == object and df[col].apply(lambda x: isinstance(x, list)).any():
            df[col] = df[col].apply(lambda x: str(x) if isinstance(x, list) else x)

    rows = len(df)
    logger.info(f"Loading {rows} rows into {target_table} (merge on {merge_key})")

    # Write Parquet to MinIO warehouse bucket
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
    )

    bucket = 'warehouse'
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)
        logger.info(f"Created bucket '{bucket}'")

    buf = BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    key = f"{target_table}/latest.parquet"
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    logger.info(f"Written s3://{bucket}/{key} ({rows} rows)")

    return {"table": target_table, "rows_loaded": rows, "merge_key": merge_key}
