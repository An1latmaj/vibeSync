# app.py
import uvicorn
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from ingestion import ingest_apple_music_file
from db import get_connection
from models import IngestResponse
from sqlalchemy import text

app = FastAPI(title="Apple Music Ingestion Service (FastAPI + SQLAlchemy Core + raw SQL)")

@app.on_event("startup")
async def startup():
    # Optionally test DB connection
    conn = await get_connection()
    try:
        await conn.execute(text("SELECT 1"))
    finally:
        await conn.close()

@app.post("/apple_music/upload", response_model=IngestResponse)
async def apple_music_upload(user_id: str = Form(...), file: UploadFile = File(...)):
    """
    Upload an Apple Music Track Play History CSV file.
    Form fields:
      - user_id: UUID string (replace with real auth in prod)
      - file: file upload (CSV)
    """
    if file.content_type not in ("text/csv", "application/csv", "application/octet-stream", "text/plain"):
        # many clients send application/octet-stream; allow it
        pass

    # Read file into memory — for large files switch to streaming CSV parsers
    contents = await file.read()
    # We need a file-like object with read() for ingestion function — use BytesIO
    from io import BytesIO
    file_obj = BytesIO(contents)

    try:
        job_row = await ingest_apple_music_file(user_id=user_id, file_obj=file_obj, filename=file.filename, filesize=len(contents))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return IngestResponse(job_id=job_row.id, status=job_row.status)

@app.get("/ingestion_jobs")
async def list_jobs(limit: int = 50):
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
    uvicorn.run("app:app", host="0.0.0.0", port=3002, reload=True)
