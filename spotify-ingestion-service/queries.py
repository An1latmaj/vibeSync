# queries.py
from sqlalchemy import text, bindparam, JSON

# Raw SQL for inserting ingestion_job and returning the row
INSERT_INGESTION_JOB = text("""
INSERT INTO ingestion_job (user_id, source_id, original_filename, file_size_bytes, status, uploaded_at)
VALUES (:user_id, :source_id, :original_filename, :file_size_bytes, :status, now())
RETURNING *
""")

# Raw SQL to insert raw_spotify_stream with ON CONFLICT DO NOTHING (we'll execute many times)
INSERT_RAW_SPOTIFY = text("""
INSERT INTO raw_spotify_stream (id, job_id, user_id, raw, ts, ms_played, platform_text, conn_country, spotify_track_uri, master_track_name, master_artist_name, inserted_at)
VALUES (:id, :job_id, :user_id, :raw, :ts, :ms_played, :platform_text, :conn_country, :spotify_track_uri, :master_track_name, :master_artist_name, now())
ON CONFLICT (user_id, spotify_track_uri, ts, ms_played) DO NOTHING
""")

# Insert play_event with ON CONFLICT DO NOTHING for duplicate plays
INSERT_PLAY_EVENT = text("""
INSERT INTO play_event (user_id, source_id, raw_id, raw_pointer, canonical_track_id, artist_id, album_id, played_at, duration_ms, metadata, created_at)
VALUES (:user_id, :source_id, :raw_id, :raw_pointer, :canonical_track_id, :artist_id, :album_id, :played_at, :duration_ms, :metadata, now())
ON CONFLICT (user_id, canonical_track_id, played_at) DO NOTHING
""")

# Simple fetch canonical track by spotify external id (raw SQL)
SELECT_TRACK_BY_SPOTIFY_URI = text("""
SELECT * FROM canonical_track WHERE external_ids->>'spotify' = :spotify_uri
""")

# Insert canonical_artist (raw SQL, handle race via ON CONFLICT)
INSERT_CANONICAL_ARTIST = text("""
INSERT INTO canonical_artist (id, name, metadata, created_at)
VALUES (:id, :name, :metadata, now())
ON CONFLICT (lower(name)) DO NOTHING
RETURNING *
""")

# Insert canonical_track (raw SQL — minimal)
INSERT_CANONICAL_TRACK_MIN = text("""
INSERT INTO canonical_track (id, artist_id, album_id, name, duration_ms, external_ids, metadata, created_at)
VALUES (:id, :artist_id, :album_id, :name, :duration_ms, :external_ids, :metadata, now())
RETURNING *
""")

# Update raw_spotify_stream processed_at
UPDATE_RAW_PROCESSED_AT = text("""
UPDATE raw_spotify_stream SET processed_at = now() WHERE id = ANY(:ids)
""")

# Update ingestion_job status/stats
UPDATE_INGESTION_JOB = text("""
UPDATE ingestion_job SET 
    status = :status, 
    stats = :stats, 
    raw_payload_summary = :raw_payload_summary 
WHERE id = :id
""")
