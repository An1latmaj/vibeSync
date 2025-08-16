-- Enable pgcrypto for gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS app_user (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email TEXT UNIQUE,
  display_name TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  metadata JSONB
);

CREATE TABLE IF NOT EXISTS ingestion_source (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  description TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ingestion_job (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES app_user(id) ON DELETE CASCADE,
  source_id UUID NOT NULL REFERENCES ingestion_source(id),
  original_filename TEXT,
  file_size_bytes BIGINT,
  uploaded_at TIMESTAMPTZ DEFAULT now(),
  status TEXT NOT NULL DEFAULT 'uploaded',
  stats JSONB DEFAULT '{}' ,
  raw_payload_summary JSONB
);

CREATE TABLE IF NOT EXISTS raw_spotify_stream (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id UUID NOT NULL REFERENCES ingestion_job(id) ON DELETE CASCADE,
  user_id UUID NOT NULL REFERENCES app_user(id) ON DELETE CASCADE,
  raw JSONB NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  ms_played INTEGER,
  platform_text TEXT,
  conn_country TEXT,
  spotify_track_uri TEXT,
  master_track_name TEXT,
  master_artist_name TEXT,
  processed_at TIMESTAMPTZ,
  inserted_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE (user_id, spotify_track_uri, ts, ms_played)
);
CREATE INDEX IF NOT EXISTS idx_raw_spotify_user_ts ON raw_spotify_stream (user_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_raw_spotify_uri ON raw_spotify_stream (spotify_track_uri);

CREATE TABLE raw_apple_music_stream (
    id UUID PRIMARY KEY,
    job_id UUID REFERENCES ingestion_job(id),
    user_id UUID NOT NULL,
    raw JSONB,
    track_name TEXT,
    artist_name TEXT,
    played_at TIMESTAMPTZ,
    is_user_initiated BOOLEAN,
    processed_at TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (user_id, track_name, played_at)
);

CREATE TABLE IF NOT EXISTS canonical_artist (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  metadata JSONB,
  created_at TIMESTAMPTZ DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_canonical_artist_lower_name ON canonical_artist (lower(name));

CREATE TABLE IF NOT EXISTS canonical_album (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  artist_id UUID REFERENCES canonical_artist(id),
  name TEXT NOT NULL,
  metadata JSONB,
  release_date DATE
);

CREATE TABLE IF NOT EXISTS canonical_track (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  artist_id UUID REFERENCES canonical_artist(id),
  album_id UUID REFERENCES canonical_album(id),
  name TEXT NOT NULL,
  duration_ms INTEGER,
  external_ids JSONB,
  metadata JSONB,
  created_at TIMESTAMPTZ DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_canonical_track_artist_lower_name ON canonical_track (artist_id, lower(name));
CREATE INDEX IF NOT EXISTS idx_track_name_tsv ON canonical_track USING gin (to_tsvector('simple', name));

CREATE TABLE IF NOT EXISTS play_event (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES app_user(id) ON DELETE CASCADE,
  source_id UUID NOT NULL REFERENCES ingestion_source(id),
  raw_id UUID,
  raw_pointer JSONB,
  canonical_track_id UUID REFERENCES canonical_track(id),
  artist_id UUID REFERENCES canonical_artist(id),
  album_id UUID REFERENCES canonical_album(id),
  played_at TIMESTAMPTZ NOT NULL,
  duration_ms INTEGER NOT NULL CHECK (duration_ms >= 30000),
  metadata JSONB,
  created_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_play_user_ts ON play_event (user_id, played_at DESC);
CREATE INDEX IF NOT EXISTS idx_play_track ON play_event (canonical_track_id);
CREATE INDEX IF NOT EXISTS idx_play_artist ON play_event (artist_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_play_event_user_track_time ON play_event (user_id, canonical_track_id, played_at);

CREATE TABLE IF NOT EXISTS ingestion_dedup_key (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES app_user(id) ON DELETE CASCADE,
  source_id UUID NOT NULL REFERENCES ingestion_source(id),
  dedup_key TEXT NOT NULL,
  job_id UUID REFERENCES ingestion_job(id),
  inserted_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE (user_id, source_id, dedup_key)
);

CREATE TABLE IF NOT EXISTS ingestion_error (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id UUID REFERENCES ingestion_job(id),
  raw_table TEXT,
  raw_id UUID,
  error TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- seed spotify ingestion_source if missing
INSERT INTO ingestion_source (id, name, description)
SELECT gen_random_uuid(), 'spotify', 'Spotify extended streaming history'
WHERE NOT EXISTS (SELECT 1 FROM ingestion_source WHERE name = 'spotify');
-- Add Apple Music as an ingestion source
INSERT INTO ingestion_source (id, name, description)
VALUES (gen_random_uuid(), 'apple_music', 'Apple Music Track Play History');