import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os

load_dotenv()

host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
dbname = os.getenv("DB_NAME")


def create_tables(conn, cursor):
    """Create the necessary tables for the music database"""
    try:
        # Create artists table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS artists (
                artist_id BIGINT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                formed_year INTEGER,
                image_url VARCHAR(255),
                UNIQUE(name)
            );
        """)

        # Create albums table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS albums (
                album_id BIGINT PRIMARY KEY,
                artist_id BIGINT REFERENCES artists(artist_id),
                name VARCHAR(255) NOT NULL,
                release_date DATE,
                image_url VARCHAR(255),
                UNIQUE(artist_id, name)
            );
        """)

        # Create tracks table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tracks (
                track_id BIGINT PRIMARY KEY,
                artist_id BIGINT REFERENCES artists(artist_id),
                album_id BIGINT REFERENCES albums(album_id),
                name VARCHAR(255) NOT NULL,
                duration_seconds INTEGER,
                track_number INTEGER,
                UNIQUE(artist_id, album_id, name)
            );
        """)

        # Create indexes for better query performance
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_tracks_artist ON tracks(artist_id);
            CREATE INDEX IF NOT EXISTS idx_tracks_album ON tracks(album_id);
            CREATE INDEX IF NOT EXISTS idx_albums_artist ON albums(artist_id);
        """)

        conn.commit()
        print("Successfully created all tables and indexes!")

    except Exception as e:
        conn.rollback()
        print(f"Error creating tables: {e}")


def verify_tables(cursor):
    """Verify that all tables were created correctly"""
    tables = ['artists', 'albums', 'tracks']
    for table in tables:
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            );
        """, (table,))
        exists = cursor.fetchone()[0]
        print(f"Table {table}: {'✓' if exists else '✗'}")


def main():
    try:
        # First connection to create database if it doesn't exist
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname='postgres',  # Connect to default db first
            user=user,
            password=password
        )
        conn.autocommit = True  # Required for creating database
        cursor = conn.cursor()

        # Check if database exists
        cursor.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s;"), [dbname])
        exists = cursor.fetchone()

        if not exists:
            print(f"Database {dbname} does not exist, creating database...")
            cursor.execute(sql.SQL("CREATE DATABASE {};").format(sql.Identifier(dbname)))
            print(f"Successfully created database {dbname}")

        cursor.close()
        conn.close()

        # Connect to the target database and create tables
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        cursor = conn.cursor()

        # Create all tables
        create_tables(conn, cursor)

        # Verify tables were created
        verify_tables(cursor)

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()