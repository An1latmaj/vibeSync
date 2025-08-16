import json
import uuid
import hashlib
from datetime import datetime
from typing import List, Dict, Any
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from db import get_connection
import queries

MIN_MS_PLAYED = 30000
BATCH = 300

def sha256hex(s: str) -> str:
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

def parse_timestamp(ts_str: str) -> datetime:
    """Parse various timestamp formats to datetime object."""
    if not ts_str:
        return None

    # Try different formats commonly used in Spotify data
    formats = [
        '%Y-%m-%d %H:%M',
        '%Y-%m-%dT%H:%M:%SZ',
        '%Y-%m-%dT%H:%M:%S.%fZ',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%dT%H:%M:%S'
    ]

    for fmt in formats:
        try:
            return datetime.strptime(ts_str, fmt)
        except ValueError:
            continue

    # If no format worked, raise an error
    raise ValueError(f"Unable to parse timestamp: {ts_str}")

async def safe_close(connection, transaction=None):
    """Safely close connection and handle transaction."""
    try:
        if transaction and not transaction.is_closed():
            await transaction.rollback()
        if connection and not connection.closed:
            await connection.close()
    except Exception as e:
        print(f"Error closing connection: {e}")

async def ingest_spotify_file(user_id: str, file_obj, filename: str, filesize: int):
    conn = await get_connection()
    trans = await conn.begin()
    try:
        # fetch spotify source id
        result = await conn.execute(text("SELECT * FROM ingestion_source WHERE name = 'spotify' LIMIT 1"))
        source_row = result.fetchone()
        if not source_row:
            raise RuntimeError("ingestion_source 'spotify' not found")

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

        # read file content (improved robustness)
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

        # Improved JSON error handling
        try:
            records = json.loads(raw_text)
            if not isinstance(records, list):
                records = []
                raise ValueError("JSON file must contain an array of records")
        except json.JSONDecodeError as e:
            # mark job failed with specific error message
            await conn.execute(
                queries.UPDATE_INGESTION_JOB,
                {
                    "status": "failed",
                    "stats": json.dumps({"error": "invalid_json", "details": str(e)}),
                    "raw_payload_summary": json.dumps({}),
                    "id": job_id
                }
            )
            await trans.commit()
            await safe_close(conn)
            raise ValueError(f"Invalid JSON format: {e}")

        total_records = len(records)
        valid_records = [r for r in records if int(r.get("ms_played", r.get("msPlayed", 0)) or 0) >= MIN_MS_PLAYED]

        inserted = 0
        dedup_keys = []

        for i in range(0, len(valid_records), BATCH):
            chunk = valid_records[i:i+BATCH]
            params = []
            for r in chunk:
                row_id = str(uuid.uuid4())
                spotify_uri = r.get("spotify_track_uri") or r.get("spotifyTrackUri") or r.get("spotify_uri")
                ts_str = r.get("ts") or r.get("endTime") or r.get("end_time")
                ts = parse_timestamp(ts_str) if ts_str else None
                ms_played = int(r.get("ms_played", r.get("msPlayed", 0)) or 0)
                master_track_name = r.get("master_metadata_track_name") or r.get("trackName") or r.get("track_name")
                master_artist_name = r.get("master_metadata_album_artist_name") or r.get("artistName") or r.get("artist_name")
                platform = r.get("platform")
                conn_country = r.get("conn_country")

                params.append({
                    "id": row_id,
                    "job_id": job_id,
                    "user_id": user_id,
                    "raw": json.dumps(r),
                    "ts": ts,
                    "ms_played": ms_played,
                    "platform_text": platform,
                    "conn_country": conn_country,
                    "spotify_track_uri": spotify_uri,
                    "master_track_name": master_track_name,
                    "master_artist_name": master_artist_name
                })
                dedup_keys.append({
                    "user_id": user_id,
                    "source_id": source_row.id,
                    "dedup_key": sha256hex(f"{user_id}|spotify|{spotify_uri or ''}|{ts_str}|{ms_played}"),
                    "job_id": job_id
                })

            # execute insert per-row
            for p in params:
                await conn.execute(queries.INSERT_RAW_SPOTIFY, p)
            inserted += len(params)

        # insert dedup keys (bulk)
        for i in range(0, len(dedup_keys), BATCH):
            chunk = dedup_keys[i:i+BATCH]
            for item in chunk:
                try:
                    await conn.execute(text("""
                    INSERT INTO ingestion_dedup_key (user_id, source_id, dedup_key, job_id)
                    VALUES (:user_id, :source_id, :dedup_key, :job_id)
                    ON CONFLICT (user_id, source_id, dedup_key) DO NOTHING
                    """), item)
                except Exception:
                    pass

        # normalization: create canonical and play events
        stats = await normalize_job(conn, job_id, source_row.id)

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
        await safe_close(conn)
        return job_row

    except Exception as e:
        await trans.rollback()
        await safe_close(conn)
        raise RuntimeError(f"Error during ingestion: {str(e)}")

async def normalize_job(conn, job_id: str, source_id: str) -> Dict[str, Any]:
    # Start a nested transaction for this operation
    nested_trans = await conn.begin_nested()
    try:
        processed = 0
        created_tracks = 0
        created_artists = 0

        while True:
            res = await conn.execute(text("""
                SELECT * FROM raw_spotify_stream
                WHERE job_id = :job_id AND processed_at IS NULL AND ms_played >= :min_ms
                ORDER BY inserted_at ASC
                LIMIT :limit
            """), {"job_id": job_id, "min_ms": MIN_MS_PLAYED, "limit": BATCH})
            rows = res.fetchall()
            if not rows:
                break

            play_events = []
            raw_ids = []
            for r in rows:
                raw_ids.append(r.id)
                try:
                    raw_obj = r.raw if isinstance(r.raw, dict) else json.loads(r.raw)
                except Exception:
                    raw_obj = None

                spotify_uri = r.spotify_track_uri
                master_track = r.master_track_name or (raw_obj and raw_obj.get("master_metadata_track_name")) or (raw_obj and raw_obj.get("trackName"))
                master_artist = r.master_artist_name or (raw_obj and raw_obj.get("master_metadata_album_artist_name")) or (raw_obj and raw_obj.get("artistName"))
                track_id = None
                artist_id = None
                album_id = None

                if spotify_uri:
                    tr = await conn.execute(queries.SELECT_TRACK_BY_SPOTIFY_URI, {"spotify_uri": spotify_uri})
                    tr_row = tr.fetchone()
                    if tr_row:
                        track_id = tr_row.id
                        artist_id = tr_row.artist_id
                        album_id = tr_row.album_id

                if not track_id and master_artist:
                    art = await conn.execute(text("SELECT * FROM canonical_artist WHERE lower(name) = :lname LIMIT 1"),
                                            {"lname": master_artist.lower()})
                    art_row = art.fetchone()
                    if not art_row:
                        new_artist_id = str(uuid.uuid4())
                        try:
                            art_ins = await conn.execute(queries.INSERT_CANONICAL_ARTIST, {
                                "id": new_artist_id,
                                "name": master_artist,
                                "metadata": json.dumps({"source_guess": "spotify"})
                            })
                            art_ret = art_ins.fetchone()
                            if art_ret:
                                artist_id = art_ret.id
                                created_artists += 1
                            else:
                                art_re = await conn.execute(text("SELECT * FROM canonical_artist WHERE lower(name) = :lname LIMIT 1"),
                                                        {"lname": master_artist.lower()})
                                art_row = art_re.fetchone()
                                artist_id = art_row.id if art_row else None
                        except IntegrityError:
                            art_re = await conn.execute(text("SELECT * FROM canonical_artist WHERE lower(name) = :lname LIMIT 1"),
                                                    {"lname": master_artist.lower()})
                            art_row = art_re.fetchone()
                            artist_id = art_row.id if art_row else None
                    else:
                        artist_id = art_row.id

                if not track_id and artist_id and master_track:
                    tr2 = await conn.execute(text("""
                        SELECT * FROM canonical_track WHERE artist_id = :artist_id AND lower(name) = :lname LIMIT 1
                    """), {"artist_id": artist_id, "lname": master_track.lower()})
                    tr2_row = tr2.fetchone()
                    if tr2_row:
                        track_id = tr2_row.id
                        album_id = tr2_row.album_id
                    else:
                        new_track_id = str(uuid.uuid4())
                        try:
                            extjson = json.dumps({"spotify": spotify_uri}) if spotify_uri else None
                            ins = await conn.execute(queries.INSERT_CANONICAL_TRACK_MIN, {
                                "id": new_track_id,
                                "artist_id": artist_id,
                                "album_id": None,
                                "name": master_track,
                                "duration_ms": r.ms_played,
                                "external_ids": extjson,
                                "metadata": json.dumps({"created_via": "spotify_ingest"})
                            })
                            ret = ins.fetchone()
                            if ret:
                                track_id = ret.id
                                created_tracks += 1
                            else:
                                if spotify_uri:
                                    tr = await conn.execute(queries.SELECT_TRACK_BY_SPOTIFY_URI, {"spotify_uri": spotify_uri})
                                    trrow = tr.fetchone()
                                    if trrow:
                                        track_id = trrow.id
                        except IntegrityError:
                            if spotify_uri:
                                tr = await conn.execute(queries.SELECT_TRACK_BY_SPOTIFY_URI, {"spotify_uri": spotify_uri})
                                trrow = tr.fetchone()
                                if trrow:
                                    track_id = trrow.id

                play_events.append({
                    "user_id": r.user_id,
                    "source_id": source_id,
                    "raw_id": r.id,
                    "raw_pointer": json.dumps({"table": "raw_spotify_stream", "id": str(r.id)}),
                    "canonical_track_id": track_id,
                    "artist_id": artist_id,
                    "album_id": album_id,
                    "played_at": r.ts,
                    "duration_ms": r.ms_played,
                    "metadata": json.dumps({
                        "platform": r.platform_text,
                        "conn_country": r.conn_country,
                        "raw_summary": master_track
                    })
                })

            for p in play_events:
                await conn.execute(queries.INSERT_PLAY_EVENT, p)

            if raw_ids:
                # Convert UUIDs to strings and pass as a list
                string_ids = [str(id) for id in raw_ids]
                await conn.execute(
                    text("UPDATE raw_spotify_stream SET processed_at = now() WHERE id::text = ANY(:ids)"),
                    {"ids": string_ids}
                )

            processed += len(raw_ids)

        # Commit the nested transaction
        await nested_trans.commit()
        return {"processed_rows": processed, "created_tracks": created_tracks, "created_artists": created_artists}
    except Exception as e:
        await nested_trans.rollback()
        raise RuntimeError(f"Error during normalization: {str(e)}")