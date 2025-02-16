from fastapi import FastAPI, HTTPException
import sqlite3
import pandas as pd
import os


def readFiles(DirPath):
    if not os.path.exists(DirPath):
        raise FileNotFoundError(f"Directory not found: {DirPath}")
    dirFiles=os.listdir(DirPath)
    entireHistory=pd.DataFrame()
    for file in dirFiles:
        if file.endswith(".json"):
            entireHistory=pd.concat([entireHistory,pd.read_json(DirPath+file)])
    return entireHistory

def filterData(Data):
    processed=Data.drop(["platform","ip_addr","shuffle","offline","offline_timestamp","conn_country","incognito_mode","reason_start","spotify_track_uri","spotify_episode_uri","audiobook_chapter_title","audiobook_chapter_uri","audiobook_title","audiobook_uri","episode_show_name","episode_name","skipped"],axis=1) #removing columns that don't give us any useful information
    processed=processed[~processed["master_metadata_track_name"].isna()] #dropping rows that aren't songs (podcasts, audiobooks etc...)
    processed=processed[processed["ms_played"]>25000]
    processed["ts"]= pd.to_datetime(processed["ts"]).dt.date
    processed.reset_index(inplace=True,drop=True)
    processed.columns=["ts","ms_played","track_name","artist_name","album_name","reason_end"]
    return processed



def saveToSql(filteredData: pd.DataFrame,username: str,db_path: str="spotify_data.db"):
    conn=sqlite3.connect(db_path)
    createTableUnique = """CREATE TABLE IF NOT EXISTS Users(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE
    )"""
    conn.execute(createTableUnique)
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO Users (username) VALUES (?)", (username,))
    conn.commit()
    cursor.execute("SELECT id FROM Users WHERE username = ?", (username,))
    user_id = cursor.fetchone()[0]

    create_table_query="""
    CREATE TABLE IF NOT EXISTS StreamingHistory(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    ts TEXT,
    ms_played INTEGER,
    track_name TEXT,
    artist_name TEXT,
    album_name TEXT,
    reason_end TEXT
    )
    """
    conn.execute(create_table_query)
    filteredData["user_id"]=user_id
    filteredData.drop(columns=["username"], errors="ignore", inplace=True)  # Drop username if it exists

    filteredData.to_sql("StreamingHistory",conn,if_exists="append",index=False)
    conn.close()


# fastapi endpoint
app= FastAPI()

@app.get("/")
def read_root():
    return {"message": "API is working"}
@app.get("/files/{username}/{dirPath:path}")
async def process_files(username: str, dirPath:str):
    try:
        rawData=readFiles(dirPath)
        filtered=filterData(rawData)
        saveToSql(filtered,username)
        return {
            "status": "success",
            "message": f"Processed and saved {len(filtered)} records for user {username}."
        }
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: str(e))")
