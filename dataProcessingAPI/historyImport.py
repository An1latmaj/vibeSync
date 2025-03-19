from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks, status, Query
from fastapi.responses import JSONResponse
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
import os
import shutil
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware


#import my existing functions
from apiFuncts import (
    get_db_connection, read_files, filter_data,
    get_or_create_user, insert_artists, insert_albums,
    insert_tracks, insert_listening_history,
    fetch_top_items
)

app = FastAPI(
    title="Spotify History API",
    description="API for importing and querying Spotify listening history",
    version="1.0.0"
)
# Add this after creating your FastAPI app instance
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)
class StatusResponse(BaseModel):
    status: str
    message: str
    task_id: Optional[str]=None

class ImportRequest(BaseModel):
    username: str=Field(..., min_length=1, max_length=15)

class TimeRangeRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=15)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    top_n: int = Field(..., gt=0)
    category: str = Field(..., pattern="^(artists|tracks|albums)$")
task_status = {}

@app.get("/")
async def root():
    return {
        "status": "ok",
        "message": "Spotify History API is running",
    }
@contextmanager
def get_db():
    conn = None
    try:
        conn=get_db_connection()
        yield conn
    except Exception as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        if conn:
            conn.close()


def process_files(task_id: str, temp_dir: str, username: str):
    try:
        task_status[task_id] = {"status": "processing", "message": "Processing files..."}
        with get_db() as conn:
            raw_data=read_files(temp_dir)
            processed_data=filter_data(raw_data)

            user_id=get_or_create_user(conn, username)

            #insert data
            artist_mapping = insert_artists(conn, processed_data)
            album_mapping = insert_albums(conn, processed_data, artist_mapping)
            track_mapping = insert_tracks(conn, processed_data, artist_mapping, album_mapping)
            insert_listening_history(conn, processed_data, user_id,artist_mapping, album_mapping, track_mapping)
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET history_imported = TRUE WHERE user_id = %s", (user_id,))
            cursor.connection.commit()

            #commit changes
            conn.commit()
            task_status[task_id] = {
                "status": "completed",
                "message": f"Successfully imported {len(processed_data)} records for user '{username}' :D!"
            }
    except Exception as e:
        task_status[task_id] = {"status": "failed", "message": f"Error: {str(e)}"}
    finally:
        #clean up temp directory
        try:
            shutil.rmtree(temp_dir)
        except (OSError, FileNotFoundError) as e:
            raise HTTPException(status_code=500,detail=f"Error cleaning up temporary directory: {str(e)}")


#parameter handling use fastAPIs dependency injection model
@app.post("/import/files", response_model=StatusResponse, status_code=status.HTTP_202_ACCEPTED)
async def import_spotify_files(
        background_tasks: BackgroundTasks,
        username: str = Query(..., min_length=1, max_length=15),
        files: List[UploadFile] = File(...),
):
    #validate request
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")
    #create temporary directory
    temp_dir = tempfile.mkdtemp()
    task_id = f"import_{int(time.time())}_{username}"

    try:
        #save uploaded files to temp directory
        for file in files:
            if not file.filename.lower().endswith('.json'):
                raise HTTPException(status_code=400, detail=f"File {file.filename} is not a JSON file")
            file_path =os.path.join(temp_dir, file.filename)
            #fastapi prvided file like objects being copied into buffer to perform actions on them
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
        #start background processing
        background_tasks.add_task(process_files, task_id, temp_dir, username)
        return StatusResponse(
            status="accepted",
            message="Files uploaded successfully. Processing has started.",
            task_id=task_id
        )
    except Exception as e:
        #clean up on error
        shutil.rmtree(temp_dir)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/import/status/{task_id}", response_model=StatusResponse)
async def get_import_status(task_id: str):
    if task_id not in task_status:
        raise HTTPException(status_code=404, detail="Task not found")

    task = task_status[task_id]
    return StatusResponse(
        status=task["status"],
        message=task["message"],
        task_id=task_id
    )


@app.post("/top", response_model=List[Dict[str, Any]])
async def get_top_items(request: TimeRangeRequest):
    #set default values if dates are not provided
    if request.start_time is None:
        request.start_time = datetime(2000, 1, 1)

    if request.end_time is None:
        #default to current time
        request.end_time = datetime.now()

    with get_db() as conn:
        user_id = get_or_create_user(conn, request.username)
        result = fetch_top_items(conn, user_id, request.start_time, request.end_time, request.top_n, request.category)
    return result

@app.get("/health")
async def health_check():
    try:
        #test database connection
        with get_db():
            pass
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "unhealthy", "database": "disconnected", "error": str(e)}
        )


@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={"status": "error", "message": exc.detail}
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={"status": "error", "message": f"Unexpected error: {str(exc)}"}
    )
