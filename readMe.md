# VibeSync 🎵

VibeSync is a comprehensive music tracking platform that offers users the unique ability to discover people with similar music tastes through an innovative algorithm inspired by Shazam's audio fingerprinting techniques.

## 🌟 Features

- **Spotify Integration**: Import and analyze your Spotify listening history from JSON files
- **Audio Fingerprinting**: Create unique audio fingerprints using FFT and frequency peak detection
- **Taste Matching**: Find users with similar music preferences through proprietary algorithms
- **Music Analytics**: Get top artists, tracks, and albums for specific time periods
- **RESTful API**: FastAPI-based backend with comprehensive endpoints

## 🏗️ Project Architecture

VibeSync consists of two main components:

### 1. Data Processing API
The data processing API handles user data, listening history imports, and music taste analytics:
- **Database Integration**: PostgreSQL database (with Azure Database for PostgreSQL support) for storing user profiles and listening history
- **Spotify Data Import**: Functionality to parse and import Spotify listening history JSON files
- **Analytics**: Endpoints for retrieving top artists, tracks, and albums for specific time periods
- **Asynchronous Processing**: Task-based processing for handling large file imports

### 2. Audio Fingerprinting System
The audio fingerprinting component identifies music and creates unique signatures:
- **Audio Conversion**: Converts audio files to a standardized format for processing
- **Signature Generation**: Creates unique signatures using FFT and frequency peak detection
- **Fingerprint Matching**: Compares signatures to identify similar music tastes

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- PostgreSQL database (local or Azure Database for PostgreSQL)
- Required Python packages (see installation steps)

### Installation

1. **Clone the repository**:
```bash
git clone git@github.com:An1latmaj/vibeSync.git
cd vibeSync
```

2. **Install the required packages**:
```bash
pip install -r requirements.txt
```

3. **Set up environment variables**:

Create a `.env` file in the project root with the following variables:

For local PostgreSQL:
```env
DB_HOST=localhost
DB_PORT=5432
DB_USER=your_username
DB_PASSWORD=your_password
DB_NAME=vibesync_db
```

For Azure Database for PostgreSQL:
```env
DB_HOST=your-server-name.postgres.database.azure.com
DB_PORT=5432
DB_USER=your_username@your-server-name
DB_PASSWORD=your_password
DB_NAME=vibesync_db
DB_SSL_MODE=require
```

4. **Initialize the database**:
```bash
python dataProcessingAPI/databaseinitalize.py
```

### Running the Application

1. **Start the FastAPI server**:
```bash
cd dataProcessingAPI
uvicorn historyImport:app --reload --host 0.0.0.0 --port 8000
```

2. **Access the API documentation**: 
   Navigate to `http://localhost:8000/docs` to view the interactive API documentation

## 📖 API Endpoints

### Core Endpoints

- `POST /import/files` - Upload Spotify listening history JSON files
- `GET /import/status/{task_id}` - Check the status of an import task
- `POST /top` - Get top artists/tracks/albums for a given time period
- `GET /health` - Check the health status of the API

### Example Usage

```python
import requests

# Upload Spotify listening history
files = {'file': open('spotify_listening_history.json', 'rb')}
response = requests.post('http://localhost:8000/import/files', files=files)
task_id = response.json()['task_id']

# Check import status
status_response = requests.get(f'http://localhost:8000/import/status/{task_id}')
print(status_response.json())

# Get top artists for last month
payload = {
    "time_period": "last_month",
    "limit": 10,
    "type": "artists"
}
top_artists = requests.post('http://localhost:8000/top', json=payload)
print(top_artists.json())
```

## 🎯 Audio Fingerprinting

### Generating Audio Fingerprints

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

### How it Works

The fingerprinting system uses:
- **Audio Conversion**: Standardizes audio format for consistent processing
- **FFT Analysis**: Fast Fourier Transform for frequency domain analysis  
- **Peak Detection**: Identifies significant frequency peaks in the audio
- **Signature Generation**: Creates unique fingerprint signatures by superimposing soundwaves

## 🗂️ Project Structure

```
vibeSync/
├── dataProcessingAPI/
│   ├── apiFuncts.py         # Core API functions
│   ├── databaseinitalize.py # Database setup and initialization
│   └── historyImport.py     # FastAPI application and endpoints
├── fingerprinting/
│   ├── algorithm.py         # Audio fingerprinting algorithm
│   ├── audioConverter.py    # Audio conversion utilities
│   └── signatureFormat.py   # Signature data structures
├── requirements.txt         # Python dependencies
└── README.md
```

## 🗄️ Database Setup

### PostgreSQL Configuration

The application uses PostgreSQL to store:
- User listening history from Spotify imports
- Generated audio fingerprints
- User preference data for taste matching

### Azure Database for PostgreSQL

To use Azure Database for PostgreSQL:

1. **Create Azure PostgreSQL server**:
```bash
az postgres server create \
  --resource-group your-resource-group \
  --name your-server-name \
  --location eastus \
  --admin-user your-admin-user \
  --admin-password your-secure-password \
  --sku-name GP_Gen5_2
```

2. **Configure firewall rules**:
```bash
az postgres server firewall-rule create \
  --resource-group your-resource-group \
  --server your-server-name \
  --name AllowAllAzureIps \
  --start-ip-address 0.0.0.0 \
  --end-ip-address 0.0.0.0
```

3. **Update your `.env` file** with the Azure connection details

## 🛠️ Development

### Requirements

The main dependencies include:
- **FastAPI**: Modern, fast web framework for building APIs
- **PostgreSQL**: Database for data persistence
- **Audio Processing Libraries**: For audio file conversion and analysis
- **NumPy/SciPy**: For numerical computations in fingerprinting

### Adding New Features

1. **API Endpoints**: Add new routes in `historyImport.py`
2. **Database Operations**: Extend functions in `apiFuncts.py`
3. **Audio Processing**: Modify algorithms in the `fingerprinting/` directory

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-feature`)
3. Commit your changes (`git commit -m 'Add new feature'`)
4. Push to the branch (`git push origin feature/new-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Inspired by Shazam's audio fingerprinting algorithms
- Spotify Web API for music data access
- FastAPI framework for rapid API development

---

**VibeSync - Connecting people through music** 🎵
