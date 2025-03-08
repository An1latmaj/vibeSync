import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import os
from typing import Dict, Any, Tuple, List
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


def get_db_connection():
    conn= psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    return conn;


def read_files(dir_path: str) -> pd.DataFrame:
    """Read all JSON files from directory and combine them"""
    if not os.path.exists(dir_path):
        raise FileNotFoundError(f"Directory not found: {dir_path}")

    json_files = [f for f in os.listdir(dir_path) if f.endswith('.json')]

    if not json_files:
        raise FileNotFoundError(f"No JSON files found in directory: {dir_path}")

    dfs = []
    for file in json_files:
        try:
            df = pd.read_json(os.path.join(dir_path, file))
            dfs.append(df)
        except Exception as e:
            raise ValueError(f"Error reading file {file}: {str(e)}")

    return pd.concat(dfs, ignore_index=True)


def filter_data(data: pd.DataFrame) -> pd.DataFrame:
    """Filter and process streaming history data"""
    # Keep only essential columns
    essential_columns = [
        "ts",
        "ms_played",
        "master_metadata_track_name",
        "master_metadata_album_artist_name",
        "master_metadata_album_album_name"
    ]
    # Filter data
    processed = data[essential_columns].copy()

    # Apply filters
    processed = processed[
        (~processed["master_metadata_track_name"].isna()) &
        (processed["ms_played"] > 30000)  # 30 seconds minimum
        & (~processed["master_metadata_album_artist_name"].isna())
        ]
    processed = processed.dropna()

    # Convert timestamp
    processed["ts"] = pd.to_datetime(processed["ts"])
    # Cleanign strings
    string_columns = [
        "master_metadata_track_name",
        "master_metadata_album_artist_name",
        "master_metadata_album_album_name"
    ]
    for col in string_columns:
        processed[col] = processed[col].str.strip()
        processed[col] = processed[col].str.replace('\n', '')  # Remove newlines
        processed[col] = processed[col].str.replace('\r', '')  # Remove carriage returns
        processed[col] = processed[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    # Rename columns
    column_mapping = {
        "master_metadata_track_name": "track_name",
        "master_metadata_album_artist_name": "artist_name",
        "master_metadata_album_album_name": "album_name"
    }
    processed.rename(columns=column_mapping, inplace=True)
    print(len(processed.groupby("artist_name").count()))
    return processed



def insert_artists(conn, df: pd.DataFrame) -> Dict[str, int]:
    """Insert artists and return name-to-id mapping"""
    cursor = conn.cursor()

    # Prepare unique artists data
    artists = df[['artist_name']].drop_duplicates()['artist_name'].tolist()
    artist_records = [(name,) for name in artists]

    # First, insert all artists
    execute_values(
        cursor,
        """
        INSERT INTO artists (name)
        VALUES %s
        ON CONFLICT (name) DO UPDATE 
        SET name = EXCLUDED.name
        """,
        artist_records,
        template="(%s)"
    )

    # Then, fetch all artists separately
    cursor.execute("""
        SELECT artist_id, name 
        FROM artists 
        WHERE name = ANY(%s)
    """, (artists,))

    results = cursor.fetchall()
    artist_mapping = {name: artist_id for artist_id, name in results}

    # Validate we got all mappings
    missing_artists = set(artists) - set(artist_mapping.keys())
    if missing_artists:
        raise ValueError(f"Failed to get mappings for artists: {missing_artists}")

    return artist_mapping


def insert_albums(conn, df: pd.DataFrame, artist_mapping: Dict[str, int]) -> Dict[Tuple[str, str], int]:
    """Insert albums and return (name, artist_id)-to-id mapping"""
    cursor = conn.cursor()

    # Prepare unique albums data
    albums_data = df[['album_name', 'artist_name']].drop_duplicates()
    album_records = [(
        row['album_name'],
        artist_mapping[row['artist_name']]
    ) for _, row in albums_data.iterrows()]

    # First, insert all albums
    execute_values(
        cursor,
        """
        INSERT INTO albums (name, artist_id)
        VALUES %s
        ON CONFLICT (name, artist_id) DO UPDATE 
        SET name = EXCLUDED.name
        """,
        album_records,
        template="(%s, %s)"
    )

    # Then fetch all albums separately
    # Create lists for the WHERE clause
    album_names = albums_data['album_name'].tolist()
    artist_ids = [artist_mapping[artist_name] for artist_name in albums_data['artist_name']]

    cursor.execute("""
        SELECT album_id, name, artist_id 
        FROM albums 
        WHERE (name, artist_id) IN (
            SELECT unnest(%s), unnest(%s)
        )
    """, (album_names, artist_ids))

    results = cursor.fetchall()
    album_mapping = {(name, artist_id): album_id for album_id, name, artist_id in results}

    # Validate we got all mappings
    expected_keys = set((row['album_name'], artist_mapping[row['artist_name']])
                        for _, row in albums_data.iterrows())
    missing_albums = expected_keys - set(album_mapping.keys())

    if missing_albums:
        print(f"Number of albums expected: {len(expected_keys)}")
        print(f"Number of albums received: {len(album_mapping)}")
        print(f"Missing albums: {missing_albums}")
        raise ValueError(f"Failed to get mappings for albums: {missing_albums}")

    return album_mapping


def insert_tracks(conn, df: pd.DataFrame, artist_mapping: Dict[str, int],
                  album_mapping: Dict[Tuple[str, str], int]) -> Dict[Tuple[str, str, str], int]:
    """Insert tracks and return (name, artist_id, album_id)-to-id mapping"""
    cursor = conn.cursor()

    # Prepare unique tracks data
    tracks_data = df[['track_name', 'album_name', 'artist_name']].drop_duplicates()
    track_records = [(
        row['track_name'],
        artist_mapping[row['artist_name']],
        album_mapping[(row['album_name'], artist_mapping[row['artist_name']])]
    ) for _, row in tracks_data.iterrows()]

    # First, insert all tracks
    execute_values(
        cursor,
        """
        INSERT INTO tracks (name, artist_id, album_id)
        VALUES %s
        ON CONFLICT (name, artist_id, album_id) DO UPDATE 
        SET name = EXCLUDED.name
        """,
        track_records,
        template="(%s, %s, %s)"
    )

    # Then fetch all tracks
    cursor.execute("""
        SELECT track_id, name, artist_id, album_id 
        FROM tracks 
        WHERE (name, artist_id, album_id) IN (
            SELECT t.name, t.artist_id, t.album_id
            FROM unnest(%s::text[], %s::integer[], %s::integer[]) 
            AS t(name, artist_id, album_id)
        )
    """, (
        [rec[0] for rec in track_records],
        [rec[1] for rec in track_records],
        [rec[2] for rec in track_records]
    ))

    results = cursor.fetchall()
    track_mapping = {(name, artist_id, album_id): track_id
                     for track_id, name, artist_id, album_id in results}

    # Validate we got all mappings
    expected_keys = set((rec[0], rec[1], rec[2]) for rec in track_records)
    missing_tracks = expected_keys - set(track_mapping.keys())

    if missing_tracks:
        print(f"Number of tracks expected: {len(expected_keys)}")
        print(f"Number of tracks received: {len(track_mapping)}")
        print(f"Missing tracks: {missing_tracks}")
        raise ValueError(f"Failed to get mappings for tracks: {missing_tracks}")

    return track_mapping

def insert_listening_history(conn, df: pd.DataFrame, user_id: int,
                             artist_mapping: Dict[str, int],
                             album_mapping: Dict[Tuple[str, str], int],
                             track_mapping: Dict[Tuple[str, str, str], int]):
    """Insert listening history records"""
    cursor = conn.cursor()

    # Prepare listening history records
    history_records = []
    for _, row in df.iterrows():
        artist_id = artist_mapping[row['artist_name']]
        album_id = album_mapping[(row['album_name'], artist_id)]
        track_id = track_mapping[(row['track_name'], artist_id, album_id)]

        history_records.append((
            user_id,
            track_id,
            row['ts'],
            row['ms_played']
        ))

    # Insert history
    execute_values(
        cursor,
        """
        INSERT INTO listening_history (user_id, track_id, listened_at, ms_played)
        VALUES %s
        ON CONFLICT (user_id, track_id, listened_at) DO NOTHING
        """,
        history_records,
        template="(%s, %s, %s, %s)"
    )


def get_or_create_user(conn, username: str) -> int:
    """
    Get user ID by username or create new user if doesn't exist
    Returns the user_id
    """
    cursor = conn.cursor()
    try:
        # First try to get existing user
        cursor.execute("""
            SELECT user_id FROM users 
            WHERE uname = %s
        """, (username,))

        result = cursor.fetchone()

        if result:
            user_id = result[0]
            print(f"Found existing user '{username}' with ID: {user_id}")
            return user_id

        # If user doesn't exist, create new user
        cursor.execute("""
            INSERT INTO users (uname)
            VALUES (%s)
            RETURNING user_id
        """, (username,))

        user_id = cursor.fetchone()[0]
        conn.commit()
        print(f"Created new user '{username}' with ID: {user_id}")
        return user_id

    except Exception as e:
        conn.rollback()
        raise ValueError(f"Failed to get or create user: {str(e)}")
    finally:
        cursor.close()

