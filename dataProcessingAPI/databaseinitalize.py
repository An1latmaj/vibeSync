import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv
import os
import sys

load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'dbname': os.getenv('DB_NAME')
}


def connect_to_postgres():
    """Create initial connection to postgres database"""
    try:
        # Connect to default postgres database first
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            dbname='postgres',
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return conn
    except psycopg2.Error as e:
        print(f"Unable to connect to postgres database: {e}")
        sys.exit(1)


def initialize_database():
    """Initialize the database if it doesn't exist"""
    conn = connect_to_postgres()
    cursor = conn.cursor()

    try:
        # Check if database exists
        cursor.execute(
            sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s;"),
            [DB_CONFIG['dbname']]
        )
        exists = cursor.fetchone()

        if not exists:
            print(f"Creating database {DB_CONFIG['dbname']}...")
            cursor.execute(
                sql.SQL("CREATE DATABASE {};").format(
                    sql.Identifier(DB_CONFIG['dbname'])
                )
            )
            print(f"Database {DB_CONFIG['dbname']} created successfully!")
    except psycopg2.Error as e:
        print(f"Error creating database: {e}")
        sys.exit(1)
    finally:
        cursor.close()
        conn.close()


def connect_to_app_db():
    """Connect to the application database"""
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        return conn
    except psycopg2.Error as e:
        print(f"Unable to connect to {DB_CONFIG['dbname']}: {e}")
        sys.exit(1)


def create_tables(conn, cursor):
    """Create the necessary tables for the music database"""
    try:
        # Create users table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            uname VARCHAR(15) NOT NULL,
            email VARCHAR(320),
            passhash VARCHAR(128),
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_login TIMESTAMP,
            country VARCHAR(2),
            profile_img_url VARCHAR(1042)
        );
        """)

        # Create artists table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS artists (
                artist_id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                formed_year INTEGER,
                image_url VARCHAR(255),
                UNIQUE(name)
            );
        """)

        # Create albums table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS albums (
                album_id SERIAL PRIMARY KEY,
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
                track_id SERIAL PRIMARY KEY,
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

    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error creating tables: {e}")
        sys.exit(1)


def verify_tables(cursor):
    """Verify that all tables were created correctly"""
    tables = ['users', 'artists', 'albums', 'tracks']
    for table in tables:
        try:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
            """, (table,))
            exists = cursor.fetchone()[0]
            print(f"Table {table}: {'✓' if exists else '✗'}")
        except psycopg2.Error as e:
            print(f"Error verifying table {table}: {e}")


def main():
    # Validate environment variables
    required_vars = ['DB_HOST', 'DB_PORT', 'DB_USER', 'DB_PASSWORD', 'DB_NAME']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        print(f"Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)

    # Initialize database if it doesn't exist
    initialize_database()

    # Connect to the application database
    conn = connect_to_app_db()
    cursor = conn.cursor()

    try:
        # Create all tables
        create_tables(conn, cursor)

        # Verify tables were created
        verify_tables(cursor)

    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
