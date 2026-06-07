"""
Flask app serving CVR virksomhed parquet data from S3 object storage

need the .env secrets and variables used by the data extraction scripts and S3 storage:

S3_ENDPOINT_URL, S3_BUCKET, S3_VIRKSOMHED_PREFIX, S3_REGION, AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY

endpoints a vailable at /api endpoint.
"""

import io
import json
import os
import uuid

import duckdb
import boto3
import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from flask import Flask, Response, jsonify, render_template, request, stream_with_context

# .env lives at the project root (one level up from flask_app/)
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

app = Flask(__name__)

S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_VIRKSOMHED_PREFIX", "virksomhed")
S3_REGION = os.getenv("S3_REGION", "us-east-1")

TABLES = [
    "main", "navne", "binavne", "beliggenhedsadresse", "postadresse",
    "hovedbranche", "bibranche1", "bibranche2", "bibranche3",
    "aarsbeskaeftigelse", "kvartalsbeskaeftigelse", "maanedsbeskaeftigelse",
    "virksomhedsstatus", "telefonNummer", "telefaxNummer", "elektroniskPost",
    "hjemmeside", "virksomhedsform", "regNummer", "livsforloeb",
]

_NOT_FOUND_CODES = {"404", "NoSuchKey", "NotFound"}


def connect_s3():
    if not S3_ENDPOINT:
        raise RuntimeError("S3_ENDPOINT_URL not set in .env")
    if not S3_BUCKET:
        raise RuntimeError("S3_BUCKET not set in .env")
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        region_name=S3_REGION,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID") or None,
        aws_secret_access_key=os.getenv("AWS_ACCESS_KEY") or None,
    )

def add_prefix(name):
    prefix = S3_PREFIX.strip("/")
    return f"{prefix}/{name}"


def _read(table, year):
    """Fetch a parquet file from S3 and return a DataFrame, or None if absent."""
    client = connect_s3()
    try:
        obj = client.get_object(Bucket=S3_BUCKET,
                                Key=add_prefix(f"{table}_{year}.parquet"))
        return pd.read_parquet(io.BytesIO(obj["Body"].read()))
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        msg = exc.response["Error"].get("Message", "")
        if code in _NOT_FOUND_CODES or (code == "AccessDenied" and "No such key" in msg):
            return None
        raise


def _list_years():
    client = connect_s3()
    paginator = client.get_paginator("list_objects_v2")
    years = []
    # note: paginate is needed because of the hundreds of files (years)
    for page in paginator.paginate(Bucket=S3_BUCKET,
                                   Prefix=add_prefix("main_")):
        for obj in page.get("Contents", []):
            base = obj["Key"].rsplit("/", 1)[-1]
            if base.startswith("main_") and base.endswith(".parquet"):
                years.append(base[len("main_") : -len(".parquet")])
    return sorted(years)

def _error(msg, status=400):
    return jsonify({"error": msg}), status

#----------------------------------------#
#---------------- Routes ----------------#
#----------------------------------------#

@app.route("/")
def index():
    return render_template("index.html", tables=TABLES)


@app.route("/api")
def api_overview():
    return jsonify({
        "endpoints": {
            "GET /api": "This overview",
            "GET /health": "S3 connectivity check",
            "GET /virksomhed/years": "List available founding years",
            "GET /virksomhed/<table>/<year>": (
                "Rows from a table/year. "
                "Query params: page (default 1), page_size (default 100, max 1000), "
                "columns (comma-separated)"
            ),
            "GET /download": (
                "Merged download across years using DuckDB. "
                "Params: table, years (comma-separated or 'all'), format (csv|json)"
            ),
        },
        "tables": TABLES,
    })


@app.route("/health")
def health():
    years = _list_years()
    return jsonify({"status": "ok",
                    "bucket": S3_BUCKET,
                    "years_available": len(years)})

@app.route("/virksomhed/years")
def years():
    return jsonify({"years": _list_years()})


@app.route("/virksomhed/<table>/<year>")
def table_year(table, year):
    if table not in TABLES:
        return _error(f"Unknown table '{table}'. See GET / for the list.", 404)

    page = request.args.get("page", 1, type=int)
    page_size = min(request.args.get("page_size", 100, type=int), 1000)
    col_filter = request.args.get("columns")

    try:
        df = _read(table, year)
    except Exception as exc:
        return _error(str(exc), 503)

    if df is None:
        return _error(f"No data found for table='{table}' year='{year}'.", 404)

    if col_filter:
        cols = [c.strip() for c in col_filter.split(",") if c.strip() in df.columns]
        if cols:
            df = df[cols]

    total = len(df)
    start = (page - 1) * page_size
    df_page = df.iloc[start : start + page_size]

    return jsonify({
        "table": table,
        "year": year,
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": -(-total // page_size),
        "data": df_page.to_dict(orient="records"),
    })

@app.route("/last_update")
def last_update():
    try:
        client = connect_s3()
        obj = client.get_object(Bucket=S3_BUCKET,
                                Key=add_prefix("_state.json"))
        state = json.loads(obj["Body"].read())
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        msg = exc.response["Error"].get("Message", "")
        if code in _NOT_FOUND_CODES or (code == "AccessDenied" and "No such key" in msg):
            return jsonify({"last_run_utc": None})
        return _error(str(exc), 503)
    except Exception as exc:
        return _error(str(exc), 503)

    last_run = state.get("last_run_utc") or max(
        state.get("years", {}).values(), default=None
    )
    return jsonify({"last_run_utc": last_run})

# needed for progress bar
_pending: dict[str, tuple[str, str, bytes]] = {}

# needed for progress bar
def _sse(payload: dict) -> str:
    return f"data: {json.dumps(payload)}\n\n"


@app.route("/download/stream")
def download_stream():
    table = request.args.get("table", "").strip()
    years_param = request.args.get("years", "").strip()
    fmt = request.args.get("format", "csv").strip().lower()

    if not table or table not in TABLES:
        return _error("Valid 'table' param required.", 400)
    if not years_param:
        return _error("'years' param required.", 400)
    if fmt not in ("csv", "excel"):
        return _error("'format' must be 'csv' or 'excel'.", 400)

    def generate():
        if years_param == "all":
            try:
                years = _list_years()
            except Exception as exc:
                yield _sse({"error": str(exc)})
                return
        else:
            years = [y.strip() for y in years_param.split(",") if y.strip()]

        total = len(years)
        dfs = []

        for i, year in enumerate(years):
            pct = int(i / total * 80)
            yield _sse({"pct": pct, "msg": f"Fetching {year}…"})
            try:
                df = _read(table, year)
            except Exception as exc:
                yield _sse({"error": str(exc)})
                return
            if df is not None and not df.empty and len(df.columns) > 0:
                dfs.append(df)
        # first and last year os the list
        year_from, year_to = years[0], years[-1]
        if not dfs:
            yield _sse({"error": (
                f"No data available for \"{table}\" between {year_from} and {year_to}. "
                f"Try selecting more recent founding years."
            )})
            return

        yield _sse({"pct": 85, "msg": "Merging…"})
        combined = pd.concat(dfs, ignore_index=True)
        if len(combined.columns) == 0:
            yield _sse({"error": (
                f"No data available for \"{table}\" between {year_from} and {year_to}. "
                f"Try selecting more recent founding years."
            )})
            return

        conn = duckdb.connect()
        conn.register("data", combined)
        merged = conn.execute("SELECT * FROM data").df()

        yield _sse({"pct": 95, "msg": "Serializing…"})
        year_label = "all" if years_param == "all" else f"{years[0]}-{years[-1]}"
        if fmt == "csv":
            file_bytes = merged.to_csv(index=False).encode()
            filename   = f"{table}_{year_label}.csv"
        else:
            buf = io.BytesIO()
            merged.to_excel(buf, index=False, engine="openpyxl")
            file_bytes = buf.getvalue()
            filename   = f"{table}_{year_label}.xlsx"

        token = uuid.uuid4().hex
        _pending[token] = (fmt, filename, file_bytes)
        yield _sse({"pct": 100, "done": True, "token": token})

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/download/file/<token>")
def download_file(token):
    entry = _pending.pop(token, None)
    if entry is None:
        return _error("File not found or already downloaded.", 404)
    fmt, filename, file_bytes = entry
    mimetype = (
        "text/csv" if fmt == "csv"
        else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    return Response(
        file_bytes,
        mimetype=mimetype,
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/download")
def download():
    table = request.args.get("table", "").strip()
    years_param = request.args.get("years", "").strip()
    fmt = request.args.get("format", "csv").strip().lower()

    if not table or table not in TABLES:
        return _error(f"Valid 'table' param required. Options: {TABLES}", 400)
    if not years_param:
        return _error("'years' param required (comma-separated or 'all').", 400)
    if fmt not in ("csv", "excel"):
        return _error("'format' must be 'csv' or 'excel'.", 400)

    try:
        all_years = _list_years()
    except Exception as exc:
        return _error(str(exc), 503)

    years = all_years if years_param == "all" else [
        y.strip() for y in years_param.split(",") if y.strip()
    ]

    dfs = []
    for year in years:
        try:
            df = _read(table, year)
        except Exception as exc:
            return _error(str(exc), 503)
        if df is not None:
            dfs.append(df)

    if not dfs:
        return _error(f"No data found for table='{table}' years={years}.", 404)

    conn = duckdb.connect()
    conn.register("data", pd.concat(dfs, ignore_index=True))
    merged = conn.execute("SELECT * FROM data").df()

    year_label = "all" if years_param == "all" else years_param.replace(",", "-")

    if fmt == "csv":
        filename = f"{table}_{year_label}.csv"
        return Response(
            merged.to_csv(index=False),
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )
    else:
        import io as _io
        filename = f"{table}_{year_label}.xlsx"
        buf = _io.BytesIO()
        merged.to_excel(buf, index=False, engine="openpyxl")
        buf.seek(0)
        return Response(
            buf.getvalue(),
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
