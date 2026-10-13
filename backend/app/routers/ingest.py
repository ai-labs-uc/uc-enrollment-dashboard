"""Upload endpoints.

The upload *screen* is a later phase; the endpoints exist now so the CLI loader and the
future form share one validation path instead of growing two. Two routes:

    POST /api/ingest/validate   dry run — parses, validates, writes nothing
    POST /api/ingest            validates and loads, inside one transaction

Both require X-API-Key. With INGEST_API_KEY unset the routes return 503, which is the
right default for a deployment whose auth story is not finished.
"""

from __future__ import annotations

import hashlib
import io
import shutil
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, File, Header, HTTPException, Query, Request, UploadFile
from psycopg import Connection

from ..config import get_settings
from ..db import get_conn
from ..etl.boundaries import discover_sources, load_boundaries
from ..etl.enrollments import load_csv, load_stream, resolve_geography, validate_stream
from ..gpkg import GeoPackageError
from ..validate import HeaderError

router = APIRouter(prefix="/api/ingest", tags=["ingest"])

Conn = Annotated[Connection, Depends(get_conn)]


@dataclass
class InitialUploadSession:
    directory: Path
    created_at: float


# These sessions are deliberately short-lived and only live on the running API instance.
# They exist so the browser can send the 200+ MB GeoPackages in small requests instead of
# relying on one giant request surviving a CDN/proxy timeout.
_INITIAL_UPLOADS: dict[str, InitialUploadSession] = {}
_INITIAL_UPLOAD_TTL_SECONDS = 60 * 60
_INITIAL_FILENAMES = {
    "country": "All Enrollees International (Country-level).gpkg",
    "province": "All Enrollees Per Program (Provincial-level).gpkg",
    "municipality": "All Enrollees Per Program and Total (Municipal-level).gpkg",
}


def require_api_key(x_api_key: Annotated[str | None, Header()] = None) -> None:
    configured = get_settings().ingest_api_key
    if not configured:
        raise HTTPException(503, "ingest is disabled: INGEST_API_KEY is not set")
    if x_api_key != configured:
        raise HTTPException(401, "invalid or missing X-API-Key")


async def _read_upload(file: UploadFile) -> tuple[str, str]:
    limit = get_settings().max_upload_mb * 1024 * 1024
    data = await file.read(limit + 1)
    if len(data) > limit:
        raise HTTPException(413, f"file exceeds {get_settings().max_upload_mb} MB")
    if not data:
        raise HTTPException(400, "file is empty")
    return data.decode("utf-8-sig", errors="replace"), hashlib.sha256(data).hexdigest()


async def _save_upload(file: UploadFile, destination: Path) -> None:
    """Copy an uploaded CSV or GeoPackage to disk without holding it all in memory."""
    limit = get_settings().max_upload_mb * 1024 * 1024
    total = 0
    with destination.open("wb") as output:
        while chunk := await file.read(1024 * 1024):
            total += len(chunk)
            if total > limit:
                raise HTTPException(413, f"file exceeds {get_settings().max_upload_mb} MB")
            output.write(chunk)
    if total == 0:
        raise HTTPException(400, "file is empty")


def _require_suffix(file: UploadFile, suffix: str) -> None:
    if not (file.filename or "").lower().endswith(suffix):
        raise HTTPException(422, f"{file.filename or 'file'} must be a {suffix} file")


def _result_payload(result, boundaries: dict[str, int]) -> dict:
    return {
        "batch_id": result.batch_id,
        "rows_read": result.report.rows_read,
        "rows_inserted": result.rows_inserted,
        "rows_duplicate": result.rows_duplicate,
        "rows_rejected": result.report.rows_rejected,
        "boundaries": boundaries,
        "unmatched_municipalities": [
            {"municity": m, "province": p, "rows": n}
            for m, p, n in result.unmatched_municipality[:25]
        ],
    }


def _load_initial_data(conn: Connection, data_dir: Path):
    sources = discover_sources(data_dir)
    if len(sources) != len(_INITIAL_FILENAMES):
        raise HTTPException(422, "all three boundary GeoPackages are required")
    # A browser initial import is also the recovery path for a partial prior import.
    # Replacing stale geometry is safe: enrollment rows are retained and re-linked once
    # every boundary level has been loaded.
    boundaries = load_boundaries(conn, sources, replace=True)
    resolve_geography(conn)
    result = load_csv(conn, data_dir / "Data for dashboard.csv", note="initial dashboard upload")
    return _result_payload(result, boundaries)


def _get_initial_session(upload_id: str) -> InitialUploadSession:
    session = _INITIAL_UPLOADS.get(upload_id)
    if session is None:
        raise HTTPException(404, "upload session was not found; start the upload again")
    return session


def _discard_initial_session(upload_id: str) -> None:
    session = _INITIAL_UPLOADS.pop(upload_id, None)
    if session is not None:
        shutil.rmtree(session.directory, ignore_errors=True)


@router.post("/validate", dependencies=[Depends(require_api_key)])
async def validate_upload(file: Annotated[UploadFile, File()]) -> dict:
    """Parse and check a file without touching the database."""
    text, _ = await _read_upload(file)
    try:
        rows, report = validate_stream(io.StringIO(text))
    except HeaderError as exc:
        raise HTTPException(422, str(exc)) from exc
    for _ in rows:
        pass
    return {
        "filename": file.filename,
        "rows_read": report.rows_read,
        "rows_valid": report.rows_valid,
        "rows_rejected": report.rows_rejected,
        "ignored_columns": report.ignored_columns,
        "rejections": [
            {"row": r.row_number, "reason": r.reason} for r in report.rejections[:100]
        ],
    }


@router.post("", dependencies=[Depends(require_api_key)])
async def ingest(
    conn: Conn,
    file: Annotated[UploadFile, File()],
    note: Annotated[str | None, Header()] = None,
) -> dict:
    """Validate and load. Rows that already exist are skipped, not duplicated."""
    text, digest = await _read_upload(file)
    try:
        result = load_stream(
            conn, io.StringIO(text), filename=file.filename or "upload.csv",
            sha256=digest, note=note,
        )
    except HeaderError as exc:
        conn.rollback()
        raise HTTPException(422, str(exc)) from exc
    return {
        "batch_id": result.batch_id,
        "rows_read": result.report.rows_read,
        "rows_inserted": result.rows_inserted,
        "rows_duplicate": result.rows_duplicate,
        "rows_rejected": result.report.rows_rejected,
        "unmatched_municipalities": [
            {"municity": m, "province": p, "rows": n}
            for m, p, n in result.unmatched_municipality[:25]
        ],
    }


@router.post("/initial/start", dependencies=[Depends(require_api_key)])
async def start_initial_upload() -> dict:
    """Reserve a short-lived server directory for a chunked first-time import."""
    now = time.time()
    for old_id, old in list(_INITIAL_UPLOADS.items()):
        if now - old.created_at > _INITIAL_UPLOAD_TTL_SECONDS:
            _discard_initial_session(old_id)
    upload_id = uuid.uuid4().hex
    _INITIAL_UPLOADS[upload_id] = InitialUploadSession(
        directory=Path(tempfile.mkdtemp(prefix="uc-initial-")),
        created_at=now,
    )
    return {"upload_id": upload_id}


@router.put("/initial/{upload_id}/boundary/{level}", dependencies=[Depends(require_api_key)])
async def upload_initial_boundary(
    upload_id: str,
    level: str,
    request: Request,
    offset: Annotated[int, Query(ge=0)],
    total_size: Annotated[int, Query(gt=0)],
) -> dict:
    """Append one small browser-uploaded chunk of a boundary GeoPackage."""
    if level not in _INITIAL_FILENAMES:
        raise HTTPException(404, "unknown boundary level")
    limit = get_settings().max_upload_mb * 1024 * 1024
    if total_size > limit:
        raise HTTPException(413, f"file exceeds {get_settings().max_upload_mb} MB")
    session = _get_initial_session(upload_id)
    destination = session.directory / _INITIAL_FILENAMES[level]
    current_size = destination.stat().st_size if destination.exists() else 0
    if offset != current_size:
        raise HTTPException(409, f"expected chunk offset {current_size}, received {offset}")

    with destination.open("ab") as output:
        async for chunk in request.stream():
            next_size = current_size + len(chunk)
            if next_size > total_size:
                raise HTTPException(400, "chunk exceeds the declared file size")
            output.write(chunk)
            current_size = next_size
    return {"received": current_size, "complete": current_size == total_size}


@router.post("/initial/{upload_id}/complete", dependencies=[Depends(require_api_key)])
async def complete_initial_upload(
    conn: Conn,
    upload_id: str,
    csv: Annotated[UploadFile, File()],
) -> dict:
    """Load the completed boundary uploads and the CSV into the database."""
    _require_suffix(csv, ".csv")
    session = _get_initial_session(upload_id)
    try:
        for filename in _INITIAL_FILENAMES.values():
            path = session.directory / filename
            if not path.exists() or path.stat().st_size == 0:
                raise HTTPException(422, "all three boundary GeoPackages must finish uploading")
        await _save_upload(csv, session.directory / "Data for dashboard.csv")
        return _load_initial_data(conn, session.directory)
    except GeoPackageError as exc:
        conn.rollback()
        raise HTTPException(422, str(exc)) from exc
    except HeaderError as exc:
        conn.rollback()
        raise HTTPException(422, str(exc)) from exc
    finally:
        _discard_initial_session(upload_id)


@router.post("/initial", dependencies=[Depends(require_api_key)])
async def ingest_initial(
    conn: Conn,
    csv: Annotated[UploadFile, File()],
    country_gpkg: Annotated[UploadFile, File()],
    province_gpkg: Annotated[UploadFile, File()],
    municipality_gpkg: Annotated[UploadFile, File()],
) -> dict:
    """Load the first dashboard dataset: one registrar CSV and all map boundaries.

    This is deliberately separate from the recurring CSV endpoint. It lets an
    administrator initialise a new database through the dashboard without needing a
    local Python environment, while still requiring the same upload key as term loads.
    """
    _require_suffix(csv, ".csv")
    for upload in (country_gpkg, province_gpkg, municipality_gpkg):
        _require_suffix(upload, ".gpkg")

    try:
        with tempfile.TemporaryDirectory(prefix="uc-ingest-") as temporary:
            data_dir = Path(temporary)
            csv_path = data_dir / "Data for dashboard.csv"
            await _save_upload(csv, csv_path)
            await _save_upload(country_gpkg, data_dir / _INITIAL_FILENAMES["country"])
            await _save_upload(province_gpkg, data_dir / _INITIAL_FILENAMES["province"])
            await _save_upload(municipality_gpkg, data_dir / _INITIAL_FILENAMES["municipality"])
            return _load_initial_data(conn, data_dir)
    except GeoPackageError as exc:
        conn.rollback()
        raise HTTPException(422, str(exc)) from exc
    except HeaderError as exc:
        conn.rollback()
        raise HTTPException(422, str(exc)) from exc
