import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import os
from typing import Dict, Any, Tuple, List
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


def get_db_connection():
    """Create a database connection"""
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )


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
    return processed


def insert_artists(conn, df: pd.DataFrame) -> Dict[str, int]:
    """Insert artists and return name-to-id mapping"""
    cursor = conn.cursor()

    # Prepare unique artists data
    artists = df[['artist_name']].drop_duplicates()['artist_name'].tolist()
    artist_records = [(name,) for name in artists]

    # Insert artists and get their IDs
    execute_values(
        cursor,
        """
        INSERT INTO artists (name)
        VALUES %s
        ON CONFLICT (name) DO UPDATE 
        SET name = EXCLUDED.name
        RETURNING artist_id, name
        """,
        artist_records,
        template="(%s)"
    )

    results = cursor.fetchall()
    return {name: artist_id for artist_id, name in results}


def insert_albums(conn, df: pd.DataFrame, artist_mapping: Dict[str, int]) -> Dict[Tuple[str, str], int]:
    """Insert albums and return (name, artist_name)-to-id mapping"""
    cursor = conn.cursor()

    # Prepare unique albums data
    albums_data = df[['album_name', 'artist_name']].drop_duplicates()
    album_records = [(
        row['album_name'],
        artist_mapping[row['artist_name']]
    ) for _, row in albums_data.iterrows()]

    # Insert albums
    execute_values(
        cursor,
        """
        INSERT INTO albums (name, artist_id)
        VALUES %s
        ON CONFLICT (name, artist_id) DO UPDATE 
        SET name = EXCLUDED.name
        RETURNING album_id, name, artist_id
        """,
        album_records,
        template="(%s, %s)"
    )

    results = cursor.fetchall()
    # Create composite key mapping using album name and artist_id
    return {(name, artist_id): album_id for album_id, name, artist_id in results}


def insert_tracks(conn, df: pd.DataFrame, artist_mapping: Dict[str, int],
                  album_mapping: Dict[Tuple[str, str], int]) -> Dict[Tuple[str, str, str], int]:
    """Insert tracks and return (name, album_name, artist_name)-to-id mapping"""
    cursor = conn.cursor()

    # Prepare unique tracks data
    tracks_data = df[['track_name', 'album_name', 'artist_name']].drop_duplicates()
    track_records = [(
        row['track_name'],
        artist_mapping[row['artist_name']],
        album_mapping[(row['album_name'], artist_mapping[row['artist_name']])]
    ) for _, row in tracks_data.iterrows()]

    # Insert tracks
    execute_values(
        cursor,
        """
        INSERT INTO tracks (name, artist_id, album_id)
        VALUES %s
        ON CONFLICT (name, artist_id, album_id) DO UPDATE 
        SET name = EXCLUDED.name
        RETURNING track_id, name, artist_id, album_id
        """,
        track_records,
        template="(%s, %s, %s)"
    )

    results = cursor.fetchall()
    # Create composite key mapping
    return {(name, artist_id, album_id): track_id for track_id, name, artist_id, album_id in results}


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


def import_spotify_history(user_id: int, directory_path: str):
    """Main function to import Spotify history"""
    try:
        # Read and process data
        raw_data = read_files(directory_path)
        processed_data = filter_data(raw_data)

        # Connect to database
        conn = get_db_connection()

        try:
            # Insert data in order of dependencies
            artist_mapping = insert_artists(conn, processed_data)
            album_mapping = insert_albums(conn, processed_data, artist_mapping)
            track_mapping = insert_tracks(conn, processed_data, artist_mapping, album_mapping)
            insert_listening_history(conn, processed_data, user_id,
                                     artist_mapping, album_mapping, track_mapping)

            # Commit all changes
            conn.commit()
            print(f"Successfully imported {len(processed_data)} listening records!")

        except Exception as e:
            conn.rollback()
            raise e

        finally:
            conn.close()

    except Exception as e:
        print(f"Error importing Spotify history: {str(e)}")
        raise


if __name__ == "__main__":
    USER_ID = 1  # Replace with actual user ID
    SPOTIFY_DATA_DIR = "/home/anilatmaj/Downloads/tehee/Spotify Extended Streaming History"  # Replace with actual path
    import_spotify_history(USER_ID, SPOTIFY_DATA_DIR)
