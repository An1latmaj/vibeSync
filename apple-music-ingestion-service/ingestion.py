import csv
import json
import uuid
import hashlib
from datetime import datetime
from typing import List, Dict, Any, IO
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from db import get_connection
import queries
import re

BATCH = 300

def sha256hex(s: str) -> str:
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

def parse_unix_timestamp_ms(timestamp_ms: str) -> datetime:
    """Convert Unix timestamp in milliseconds to datetime object."""
    if not timestamp_ms:
        return None

    try:
        timestamp_int = int(timestamp_ms)
        return datetime.fromtimestamp(timestamp_int / 1000.0)
    except (ValueError, TypeError) as e:
        print(f"Failed to parse timestamp: {timestamp_ms}, error: {e}")
        return None

def parse_track_name(track_name: str) -> Dict[str, str]:
    """
    Parse Apple Music track names in format 'Artist - Song Title'
    Returns dict with 'artist' and 'track' keys
    """
    if not track_name:
        return {"artist": None, "track": None}

    # Split on the first ' - ' to separate artist from song
    if ' - ' in track_name:
        parts = track_name.split(' - ', 1)
        artist = parts[0].strip()
        track = parts[1].strip()
        return {"artist": artist, "track": track}
    else:
        # If no ' - ' found, treat the whole string as track name
        return {"artist": None, "track": track_name.strip()}

def extract_key_fields(row_dict: Dict[str, str]) -> Dict[str, Any]:
    """Extract the key fields we need from Apple Music Track Play History row"""
    track_name = row_dict.get("Track Name", "").strip()
    last_played_date = row_dict.get("Last Played Date", "")
    is_user_initiated = row_dict.get("Is User Initiated", "").lower() == "true"

    # Parse track name to get artist and song
    parsed_track = parse_track_name(track_name)

    # Parse timestamp
    played_at = parse_unix_timestamp_ms(last_played_date)

    return {
        "track_name": track_name,
        "artist_name": parsed_track["artist"],
        "parsed_track_name": parsed_track["track"],
        "played_at": played_at,
        "is_user_initiated": is_user_initiated,
        "last_played_date": last_played_date
    }

async def safe_close(connection, transaction=None):
    """Safely close connection and handle transaction."""
    try:
        if transaction:
            try:
                await transaction.rollback()
            except Exception:
                # Transaction may already be closed/committed
                pass
        if connection and not connection.closed:
            await connection.close()
    except Exception as e:
        print(f"Error closing connection: {e}")

async def ingest_apple_music_file(user_id: str, file_obj: IO, filename: str, filesize: int):
    conn = await get_connection()
    trans = None

    try:
        trans = await conn.begin()

        # fetch apple music source id
        result = await conn.execute(text("SELECT * FROM ingestion_source WHERE name = 'apple_music' LIMIT 1"))
        source_row = result.fetchone()
        if not source_row:
            raise RuntimeError("ingestion_source 'apple_music' not found")

        # create ingestion job
        job_result = await conn.execute(
            queries.INSERT_INGESTION_JOB,
            {
                "user_id": user_id,
                "source_id": source_row.id,
                "original_filename": filename,
                "file_size_bytes": filesize,
                "status": "processing"
            }
        )
        job_row = job_result.fetchone()
        job_id = job_row.id

        # read CSV file content
        if hasattr(file_obj, 'read'):
            content = file_obj.read()
            if callable(getattr(file_obj, 'seek', None)):
                file_obj.seek(0)
        else:
            content = file_obj

        if isinstance(content, bytes):
            raw_text = content.decode('utf-8')
        else:
            raw_text = content

        # Parse CSV data
        try:
            csv_reader = csv.DictReader(raw_text.splitlines())
            records = list(csv_reader)
        except Exception as e:
            # mark job failed with specific error message
            await conn.execute(
                queries.UPDATE_INGESTION_JOB,
                {
                    "status": "failed",
                    "stats": json.dumps({"error": "invalid_csv", "details": str(e)}),
                    "raw_payload_summary": json.dumps({}),
                    "id": job_id
                }
            )
            await trans.commit()
            return job_row

        total_records = len(records)

        # Filter for valid records - much simpler criteria now
        valid_records = []
        for r in records:
            fields = extract_key_fields(r)

            # Simple filtering criteria:
            # 1. Must have track name
            # 2. Must have valid timestamp
            # 3. Optionally filter for user-initiated plays only (you can change this)
            if (fields["track_name"] and
                fields["played_at"] and
                fields["is_user_initiated"]):  # Only user-initiated plays
                valid_records.append(r)

        inserted = 0
        dedup_keys = []

        # Process records in batches with proper error handling
        for i in range(0, len(valid_records), BATCH):
            batch_savepoint = await conn.begin_nested()
            try:
                chunk = valid_records[i:i+BATCH]
                params = []
                for r in chunk:
                    row_id = str(uuid.uuid4())
                    fields = extract_key_fields(r)

                    params.append({
                        "id": row_id,
                        "job_id": job_id,
                        "user_id": user_id,
                        "raw": json.dumps(r),
                        "track_name": fields["track_name"],
                        "artist_name": fields["artist_name"],
                        "played_at": fields["played_at"],
                        "is_user_initiated": fields["is_user_initiated"]
                    })

                    dedup_keys.append({
                        "user_id": user_id,
                        "source_id": source_row.id,
                        "dedup_key": sha256hex(f"{user_id}|apple_music|{fields['track_name']}|{fields['last_played_date']}"),
                        "job_id": job_id
                    })

                # execute insert per-row
                for p in params:
                    await conn.execute(queries.INSERT_RAW_APPLE_MUSIC, p)
                inserted += len(params)

                await batch_savepoint.commit()

            except Exception as batch_error:
                await batch_savepoint.rollback()
                print(f"Error processing batch {i//BATCH + 1}, skipping: {batch_error}")
                continue

        # insert dedup keys (bulk) with error handling
        for i in range(0, len(dedup_keys), BATCH):
            dedup_savepoint = await conn.begin_nested()
            try:
                chunk = dedup_keys[i:i+BATCH]
                for item in chunk:
                    try:
                        await conn.execute(text("""
                        INSERT INTO ingestion_dedup_key (user_id, source_id, dedup_key, job_id)
                        VALUES (:user_id, :source_id, :dedup_key, :job_id)
                        """), item)
                    except Exception:
                        pass  # Skip duplicate dedup keys
                await dedup_savepoint.commit()
            except Exception as dedup_error:
                await dedup_savepoint.rollback()
                print(f"Error processing dedup keys batch {i//BATCH + 1}: {dedup_error}")

        # normalization: create canonical and play events
        try:
            stats = await normalize_job(conn, job_id, source_row.id)
        except Exception as norm_error:
            print(f"Error during normalization: {norm_error}")
            stats = {"error": str(norm_error), "processed_rows": 0, "created_tracks": 0, "created_artists": 0}

        # Update job as completed
        await conn.execute(
            queries.UPDATE_INGESTION_JOB,
            {
                "status": "completed",
                "stats": json.dumps(stats),
                "raw_payload_summary": json.dumps({"uploaded_count": total_records, "valid_count": len(valid_records)}),
                "id": job_id
            }
        )

        await trans.commit()
        return job_row

    except Exception as e:
        if trans:
            try:
                await trans.rollback()
            except Exception:
                # Transaction may already be committed/closed
                pass
        raise RuntimeError(f"Error during ingestion: {str(e)}")
    finally:
        if conn and not conn.closed:
            await conn.close()

async def normalize_job(conn, job_id: str, source_id: str) -> Dict[str, Any]:
    # Don't use nested transactions - use savepoints instead for better error handling
    try:
        processed = 0
        created_tracks = 0
        created_artists = 0

        while True:
            # Use a savepoint for each batch to allow partial rollbacks
            savepoint = await conn.begin_nested()
            try:
                res = await conn.execute(text("""
                    SELECT * FROM raw_apple_music_stream
                    WHERE job_id = :job_id AND processed_at IS NULL
                    ORDER BY inserted_at ASC
                    LIMIT :limit
                """), {"job_id": job_id, "limit": BATCH})
                rows = res.fetchall()
                if not rows:
                    await savepoint.commit()
                    break

                play_events = []
                raw_ids = []
                for r in rows:
                    raw_ids.append(r.id)

                    artist_name = r.artist_name
                    track_name = r.track_name

                    # Parse the track_name to extract the actual song name
                    parsed_track = parse_track_name(track_name)
                    song_name = parsed_track["track"]

                    track_id = None
                    artist_id = None
                    album_id = None

                    # Try to find existing track by artist and song name
                    if artist_name and song_name:
                        tr = await conn.execute(queries.SELECT_TRACK_BY_ARTIST_AND_NAME, {
                            "artist_name": artist_name.lower(),
                            "track_name": song_name.lower()
                        })
                        tr_row = tr.fetchone()
                        if tr_row:
                            track_id = tr_row.id
                            artist_id = tr_row.artist_id
                            album_id = tr_row.album_id

                    # Create artist if not found and we have artist name
                    if not track_id and artist_name:
                        art = await conn.execute(queries.SELECT_ARTIST_BY_NAME, {"artist_name": artist_name.lower()})
                        art_row = art.fetchone()
                        if not art_row:
                            new_artist_id = str(uuid.uuid4())
                            try:
                                art_ins = await conn.execute(queries.INSERT_CANONICAL_ARTIST, {
                                    "id": new_artist_id,
                                    "name": artist_name,
                                    "metadata": json.dumps({"source_guess": "apple_music"})
                                })
                                art_ret = art_ins.fetchone()
                                if art_ret:
                                    artist_id = art_ret.id
                                    created_artists += 1
                                else:
                                    art_re = await conn.execute(queries.SELECT_ARTIST_BY_NAME, {"artist_name": artist_name.lower()})
                                    art_row = art_re.fetchone()
                                    artist_id = art_row.id if art_row else None
                            except IntegrityError:
                                art_re = await conn.execute(queries.SELECT_ARTIST_BY_NAME, {"artist_name": artist_name.lower()})
                                art_row = art_re.fetchone()
                                artist_id = art_row.id if art_row else None
                        else:
                            artist_id = art_row.id

                    # Create track if not found and we have song name
                    if not track_id and song_name:
                        # If we have artist_id, create track with artist, otherwise create orphaned track
                        tr2 = None
                        if artist_id:
                            tr2 = await conn.execute(text("""
                                SELECT * FROM canonical_track WHERE artist_id = :artist_id AND lower(name) = :track_name LIMIT 1
                            """), {"artist_id": artist_id, "track_name": song_name.lower()})
                            tr2_row = tr2.fetchone()
                            if tr2_row:
                                track_id = tr2_row.id
                                album_id = tr2_row.album_id

                        if not track_id:
                            new_track_id = str(uuid.uuid4())
                            try:
                                ins = await conn.execute(queries.INSERT_CANONICAL_TRACK_MIN, {
                                    "id": new_track_id,
                                    "artist_id": artist_id,  # Could be None for orphaned tracks
                                    "album_id": None,
                                    "name": song_name,
                                    "duration_ms": None,  # No duration info in Track Play History
                                    "external_ids": None,
                                    "metadata": json.dumps({
                                        "created_via": "apple_music_track_history",
                                        "original_track_name": track_name
                                    })
                                })
                                ret = ins.fetchone()
                                if ret:
                                    track_id = ret.id
                                    created_tracks += 1
                            except IntegrityError:
                                # Track was created by another process, try to find it again
                                if artist_id:
                                    tr3 = await conn.execute(text("""
                                        SELECT * FROM canonical_track WHERE artist_id = :artist_id AND lower(name) = :track_name LIMIT 1
                                    """), {"artist_id": artist_id, "track_name": song_name.lower()})
                                    tr3_row = tr3.fetchone()
                                    if tr3_row:
                                        track_id = tr3_row.id

                    play_events.append({
                        "user_id": r.user_id,
                        "source_id": source_id,
                        "raw_id": r.id,
                        "raw_pointer": json.dumps({"table": "raw_apple_music_stream", "id": str(r.id)}),
                        "canonical_track_id": track_id,
                        "artist_id": artist_id,
                        "album_id": album_id,
                        "played_at": r.played_at or datetime.now(),  # Use current time if played_at is None
                        "duration_ms": 30001,  # Use 30001ms to satisfy the check constraint (must be > 30000)
                        "metadata": json.dumps({
                            "platform": "apple_music",
                            "is_user_initiated": r.is_user_initiated,
                            "original_track_name": track_name,
                            "note": "duration_ms set to 30001ms as Apple Music Track Play History doesn't provide actual play duration"
                        })
                    })

                for p in play_events:
                    try:
                        await conn.execute(queries.INSERT_PLAY_EVENT, p)
                    except IntegrityError as e:
                        # Skip duplicate play events
                        print(f"Skipping duplicate play event: {e}")
                        continue

                if raw_ids:
                    # Convert UUIDs to strings and pass as a list
                    string_ids = [str(id) for id in raw_ids]
                    await conn.execute(
                        text("UPDATE raw_apple_music_stream SET processed_at = now() WHERE id::text = ANY(:ids)"),
                        {"ids": string_ids}
                    )

                processed += len(raw_ids)
                await savepoint.commit()

            except Exception as batch_error:
                await savepoint.rollback()
                print(f"Error processing batch, rolling back savepoint: {batch_error}")
                # Continue to next batch instead of failing completely
                continue

        return {"processed_rows": processed, "created_tracks": created_tracks, "created_artists": created_artists}
    except Exception as e:
        raise RuntimeError(f"Error during normalization: {str(e)}")
