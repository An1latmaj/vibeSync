# queries.py
from sqlalchemy import text

# Raw SQL for inserting ingestion_job and returning the row
INSERT_INGESTION_JOB = text("""
INSERT INTO ingestion_job (user_id, source_id, original_filename, file_size_bytes, status, uploaded_at)
VALUES (:user_id, :source_id, :original_filename, :file_size_bytes, :status, now())
RETURNING *
""")

# Raw SQL to insert raw_apple_music_stream with simplified schema for Track Play History
INSERT_RAW_APPLE_MUSIC = text("""
INSERT INTO raw_apple_music_stream (id, job_id, user_id, raw, track_name, artist_name, played_at, is_user_initiated, inserted_at)
VALUES (:id, :job_id, :user_id, :raw, :track_name, :artist_name, :played_at, :is_user_initiated, now())
ON CONFLICT (user_id, track_name, played_at) DO NOTHING
""")

# Insert play_event with ON CONFLICT DO NOTHING for duplicate plays
INSERT_PLAY_EVENT = text("""
INSERT INTO play_event (user_id, source_id, raw_id, raw_pointer, canonical_track_id, artist_id, album_id, played_at, duration_ms, metadata, created_at)
VALUES (:user_id, :source_id, :raw_id, :raw_pointer, :canonical_track_id, :artist_id, :album_id, :played_at, :duration_ms, :metadata, now())
ON CONFLICT (user_id, canonical_track_id, played_at) DO NOTHING
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

# Update ingestion_job status/stats
UPDATE_INGESTION_JOB = text("""
UPDATE ingestion_job SET 
    status = :status, 
    stats = :stats, 
    raw_payload_summary = :raw_payload_summary 
WHERE id = :id
""")

# Find track by artist and track name
SELECT_TRACK_BY_ARTIST_AND_NAME = text("""
SELECT ct.* FROM canonical_track ct
JOIN canonical_artist ca ON ct.artist_id = ca.id
WHERE lower(ca.name) = :artist_name AND lower(ct.name) = :track_name
LIMIT 1
""")

# Find artist by name
SELECT_ARTIST_BY_NAME = text("""
SELECT * FROM canonical_artist WHERE lower(name) = :artist_name LIMIT 1
""")
