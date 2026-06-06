"""
S3 (Garage) storage layer for CVR "virksomhed" data.

This is the object-storage counterpart to the on-disk helpers in
utils_virksomhed.py. The fetch + build logic (fetch_query, build_panel,
founding_year, query_updated_since) is storage-agnostic and reused from there;
only the read/write of parquet files and _state.json differs.

Layout in the bucket mirrors the local one, one object per table and founding
year under a prefix:

    <prefix>/main_<year>.parquet
    <prefix>/navne_<year>.parquet
    ...
    <prefix>/_state.json

Unlike a local rewrite, an S3 PutObject is atomic: the object is either fully
replaced or not at all, so upsert_year_s3 cannot leave a half-written file.

Config comes from .env (see .env.example):
    S3_ENDPOINT_URL, S3_BUCKET, S3_VIRKSOMHED_PREFIX, S3_REGION,
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
"""

import io
import os
import json

import pandas as pd
from dotenv import load_dotenv

import utils_virksomhed as utils

load_dotenv()


S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_VIRKSOMHED_PREFIX = os.getenv("S3_VIRKSOMHED_PREFIX")
S3_REGION = os.getenv("S3_REGION")

_STATE_KEY = "_state.json"
_NOT_FOUND = {"404", "NoSuchKey", "NotFound"}


def make_client():
    """Create a boto3 S3 client pointed at the configured endpoint.

    Credentials are read explicitly from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
    when present; otherwise boto3's default chain (e.g. ~/.aws/credentials) is used.
    """
    try:
        import boto3  # lazy import so local-only workflows don't require boto3
    except ModuleNotFoundError:
        raise SystemExit(
            "boto3 is required for the S3 scripts. Install it, e.g. "
            "`conda install -n cvr boto3` or `pip install boto3`."
        )

    if not S3_ENDPOINT_URL:
        raise SystemExit("No S3 endpoint. Set S3_ENDPOINT_URL in .env.")
    if not S3_BUCKET:
        raise SystemExit("No S3 bucket. Set S3_BUCKET in .env.")

    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        region_name=S3_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID") or None,
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY") or None,
    )


def _key(prefix, name):
    """Join a prefix and an object name into a key, e.g. ('virksomhed', 'main_2024.parquet')."""
    prefix = prefix.strip("/")
    return f"{prefix}/{name}" if prefix else name


def _read_parquet_if_exists(client, bucket, key):
    """Return the object as a DataFrame, or None if the key does not exist.

    We decide existence by attempting the GET we need anyway, rather than a
    separate HeadObject: Garage can answer HEAD with a bare 400 even for keys
    that exist, so we rely on GET, which returns a clean NoSuchKey/404 when the
    object is genuinely absent.
    """
    from botocore.exceptions import ClientError

    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response["Error"]["Code"] in _NOT_FOUND:
            return None
        raise
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def _write_parquet(client, bucket, key, df):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())


# Last successful run timestamp (mirrors utils.read_last_run / write_last_run)
def read_last_run(client, bucket, prefix):
    """Return the stored last-run UTC timestamp from <prefix>/_state.json, or None."""
    from botocore.exceptions import ClientError

    key = _key(prefix, _STATE_KEY)
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response["Error"]["Code"] in _NOT_FOUND:
            return None
        raise
    return json.loads(obj["Body"].read()).get("last_run_utc")


def write_last_run(client, bucket, prefix, when_iso):
    """Persist the next-run start timestamp to <prefix>/_state.json."""
    key = _key(prefix, _STATE_KEY)
    body = json.dumps({"last_run_utc": when_iso}).encode("utf-8")
    client.put_object(Bucket=bucket, Key=key, Body=body)


# Minimal upsert: drop changed companies from each existing table, append the fresh rows.
def upsert_year(client, bucket, prefix, year, hits_for_year):
    """Merge changed companies into a founding year's table objects in the bucket."""
    panel = utils.build_panel(hits_for_year)

    changed_cvrs = set()
    for hit in hits_for_year:
        changed_cvrs.add(hit["_source"]["Vrvirksomhed"].get("cvrNummer"))

    for name, new_df in panel.items():
        key = _key(prefix, f"{name}_{year}.parquet")
        existing = _read_parquet_if_exists(client, bucket, key)
        if existing is not None:
            if "cvrNummer" in existing.columns:
                existing = existing[~existing["cvrNummer"].isin(changed_cvrs)]
            combined = pd.concat([existing, new_df], ignore_index=True)
        else:
            combined = new_df
        _write_parquet(client, bucket, key, combined)
