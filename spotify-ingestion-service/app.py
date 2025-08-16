# app.py
import uvicorn
import logging
from uuid import UUID
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
from fastapi.requests import Request
from fastapi.exceptions import RequestValidationError
from ingestion import ingest_spotify_file
from db import engine, get_connection
from models import IngestResponse, JobRow
from sqlalchemy import text
import json
import asyncio

logger = logging.getLogger("spotify-ingestion-service")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Spotify Ingestion Service (FastAPI + SQLAlchemy Core + raw SQL)")

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(status_code=422, content={
        "error": {"code": "validation_error", "message": "Invalid request parameters", "details": exc.errors()}
    })

@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(status_code=500, content={
        "error": {"code": "internal_error", "message": "An unexpected error occurred"}
    })

@app.on_event("startup")
async def startup():
    try:
        conn = await get_connection()
        try:
            await conn.execute(text("SELECT 1"))
        finally:
            await conn.close()
    except Exception as e:
        logger.error("Database connection failed during startup: %s", e)

@app.get("/health")
async def health():
    try:
        conn = await get_connection()
        try:
            await conn.execute(text("SELECT 1"))
        finally:
            await conn.close()
        return {"status": "ok"}
    except Exception:
        return JSONResponse(status_code=503, content={"status": "degraded", "error": {"code": "db_unavailable"}})

@app.post("/spotify/upload", response_model=IngestResponse)
async def spotify_upload(user_id: str = Form(...), file: UploadFile = File(...)):
    """
    Upload a Spotify Extended Streaming History JSON file.
    Form fields:
      - user_id: UUID string (replace with real auth in prod)
      - file: file upload (JSON)
    """
    # Validate user_id
    try:
        UUID(user_id)
    except Exception:
        raise HTTPException(status_code=400, detail={"code": "invalid_user_id", "message": "user_id must be a valid UUID"})

    # Validate content type
    if file.content_type not in ("application/json", "application/octet-stream", "text/plain"):
        raise HTTPException(status_code=415, detail={"code": "unsupported_media_type", "message": f"Unsupported content type: {file.content_type}"})

    # Read file into memory — for large files switch to streaming JSON parsers
    contents = await file.read()
    if not contents:
        raise HTTPException(status_code=400, detail={"code": "empty_file", "message": "Uploaded file is empty"})

    # We need a file-like object with read() for ingestion function — use BytesIO
    from io import BytesIO
    file_obj = BytesIO(contents)

    job_row = await ingest_spotify_file(user_id=user_id, file_obj=file_obj, filename=file.filename or "upload.json", filesize=len(contents))

    return IngestResponse(job_id=job_row.id, status=job_row.status)

@app.get("/ingestion_jobs")
async def list_jobs(limit: int = 50):
    if not (1 <= limit <= 200):
        raise HTTPException(status_code=400, detail={"code": "invalid_limit", "message": "limit must be between 1 and 200"})
    conn = await get_connection()
    try:
        res = await conn.execute(text("SELECT id, user_id, source_id, status, uploaded_at, stats FROM ingestion_job ORDER BY uploaded_at DESC LIMIT :limit"), {"limit": limit})
        rows = res.fetchall()
        out = []
        for r in rows:
            out.append({
                "id": r.id,
                "user_id": r.user_id,
                "source_id": r.source_id,
                "status": r.status,
                "uploaded_at": str(r.uploaded_at),
                "stats": r.stats
            })
        return out
    finally:
        await conn.close()

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=3000, reload=True)
