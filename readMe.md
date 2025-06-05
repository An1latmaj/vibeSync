# VibeSync

VibeSync is a comprehensive music tracking platform that offers users the unique ability to discover people with similar music tastes through an innovative algorithm inspired by Shazam's audio fingerprinting techniques.

## Project Overview

VibeSync combines music tracking with social discovery by:

1. **Importing Listening History**: Users can upload their Spotify listening history to build their musical profile.
2. **Audio Fingerprinting**: The platform creates unique audio fingerprints by superimposing soundwaves, allowing for precise music identification and matching.
3. **Taste Matching**: Users can find others with similar music tastes using our proprietary algorithm.

## Project Architecture

VibeSync consists of two main components:

### 1. Data Processing API

The data processing API handles user data, listening history imports, and music taste analytics:

- **Database Integration**: PostgreSQL database for storing user profiles and listening history
- **Spotify Data Import**: Functionality to parse and import Spotify listening history JSON files
- **Analytics**: Endpoints for retrieving top artists, tracks, and albums for specific time periods

### 2. Audio Fingerprinting System

The audio fingerprinting component identifies music and creates unique signatures:

- **Audio Conversion**: Converts audio files to a standardized format for processing
- **Signature Generation**: Creates unique signatures using FFT and frequency peak detection
- **Fingerprint Matching**: Compares signatures to identify similar music tastes

## Getting Started

### Prerequisites

- Python 3.8+
- PostgreSQL database
- Required Python packages (see installation steps)

### Installation

1. Clone the repository:
```
git clone git@github.com:An1latmaj/vibeSync.git
cd vibeSync
```

2. Install the required packages:
```
pip install -r requirements.txt
```

3. Set up environment variables:
Create a `.env` file in the project root with the following variables:
```
DB_HOST=localhost
DB_PORT=5432
DB_USER=your_username
DB_PASSWORD=your_password
DB_NAME=vibesync_db
```

4. Initialize the database:
```
python dataProcessingAPI/databaseinitalize.py
```

### Running the FastAPI Server

1. Start the FastAPI server:
```
cd dataProcessingAPI
uvicorn historyImport:app --reload --host 0.0.0.0 --port 8000
```

2. Access the API documentation at `http://localhost:8000/docs`

### API Endpoints

- `POST /import/files`: Upload Spotify listening history JSON files
- `GET /import/status/{task_id}`: Check the status of an import task
- `POST /top`: Get top artists/tracks/albums for a given time period
- `GET /health`: Check the health status of the API

## Audio Fingerprinting Usage

To generate an audio fingerprint from a file:

```python
from fingerprinting.audioConverter import convert_audio_to_raw_samples
from fingerprinting.algorithm import SignatureGenerator

# Convert audio file to raw samples
samples, original_sr = convert_audio_to_raw_samples("path/to/audio_file.mp3")

# Generate signature
generator = SignatureGenerator()
generator.feed_input(samples)
signature = generator.get_next_signature()
```

## Development

### Project Structure

```
vibeSync/
├── dataProcessingAPI/
│   ├── apiFuncts.py         # Core API functions
│   ├── databaseinitalize.py # Database setup
│   └── historyImport.py     # FastAPI application
├── fingerprinting/
│   ├── algorithm.py         # Fingerprinting algorithm
│   ├── audioConverter.py    # Audio conversion utilities
│   └── signatureFormat.py   # Signature data structures
└── requirements.txt
```


