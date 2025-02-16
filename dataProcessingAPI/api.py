from fastapi import FastAPI, HTTPException
import sqlite3
import pandas as pd
import os
from typing import Dict, Any

app = FastAPI()


def read_files(dir_path: str)->pd.DataFrame:
    if not os.path.exists(dir_path):
        raise FileNotFoundError(f"Directory not found: {dir_path}")

    dir_files=os.listdir(dir_path)
    entire_history=pd.DataFrame()

    json_files=[f for f in dir_files if f.endswith('.json')]
    if not json_files:
        raise FileNotFoundError(f"No JSON files found in directory: {dir_path}")

    for file in json_files:
        full_file_path=os.path.join(dir_path,file)
        try:
            entire_history = pd.concat(
                [entire_history,pd.read_json(full_file_path)],
                ignore_index=True
            )
        except Exception as e:
            raise ValueError(f"Error reading file{file}:{str(e)}")

    return entire_history

#filter and process streaming history data
def filter_data(data: pd.DataFrame)->pd.DataFrame:
    columns_to_drop=[
        "platform", "ip_addr", "shuffle", "offline",
        "offline_timestamp", "conn_country", "incognito_mode",
        "reason_start", "spotify_track_uri", "spotify_episode_uri",
        "audiobook_chapter_title", "audiobook_chapter_uri",
        "audiobook_title", "audiobook_uri", "episode_show_name",
        "episode_name", "skipped"
    ]

    processed=data.drop(columns=[col for col in columns_to_drop if col in data.columns])
    processed=processed[~processed["master_metadata_track_name"].isna()]
    processed=processed[processed["ms_played"] > 25000]

    processed["ts"]=pd.to_datetime(processed["ts"]).dt.date
    processed.reset_index(inplace=True,drop=True)
    #rename columns
    column_mapping = {
        "master_metadata_track_name":"track_name",
        "master_metadata_album_artist_name":"artist_name",
        "master_metadata_album_album_name":"album_name"
    }
    processed.rename(columns=column_mapping, inplace=True)
    return processed[["ts","ms_played","track_name","artist_name","album_name","reason_end"]]

#save processed data to SQLite database and return number of records saved
def save_to_sql(filtered_data: pd.DataFrame, username: str, db_path: str = "spotify_data.db")->int:
    with sqlite3.connect(db_path) as conn:
        #create Users table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS Users(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE
            )
        """)

        #insert or get user
        cursor = conn.cursor()
        cursor.execute("INSERT OR IGNORE INTO Users (username) VALUES (?)",(username,))
        cursor.execute("SELECT id FROM Users WHERE username = ?",(username,))
        user_id = cursor.fetchone()[0]

        #create StreamingHistory table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS StreamingHistory(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                ts TEXT,
                ms_played INTEGER,
                track_name TEXT,
                artist_name TEXT,
                album_name TEXT,
                reason_end TEXT,
                FOREIGN KEY (user_id) REFERENCES Users(id)
            )
        """)

        # Prepare and save data
        filtered_data["user_id"] = user_id
        filtered_data.to_sql("StreamingHistory", conn, if_exists="append", index=False)

        return len(filtered_data)

#FastAPI endpoints
@app.get("/")
async def read_root()->Dict[str, str]:
    return {"message":"Spotify History Processing API is running"}

#Process Spotify history files for a user and save to database
@app.post("/process/{username}")
async def process_files(username: str,dir_path: str)->Dict[str, Any]:
    try:
        if not username or not username.strip():
            raise HTTPException(status_code=400,detail="Username cannot be empty")

        raw_data = read_files(dir_path)
        filtered = filter_data(raw_data)
        records_saved = save_to_sql(filtered,username)

        return {
            "status": "success",
            "message": f"Successfully processed and saved {records_saved} records for user {username}",
            "records_processed": records_saved
        }
    except FileNotFoundError as e:
        raise HTTPException(status_code=404,detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400,detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500,detail=f"An unexpected error occurred: {str(e)}")
