# Music Analytics Service

A comprehensive analytics service for music streaming data that provides insights into user listening habits, top charts, and time-series analysis.

## Features

### 📊 Top Charts
- **Top Artists**: Most played artists by period (weekly/monthly/yearly)
- **Top Tracks**: Most played songs with detailed statistics
- **Top Albums**: Most played albums and their performance

### 📈 Time Series Analytics
- **Listening Patterns**: Hourly, daily, weekly, monthly aggregations
- **Multiple Metrics**: Play counts, total duration, unique tracks
- **Custom Date Ranges**: Flexible time period selection

### 🎵 User Statistics
- **Comprehensive Stats**: Total plays, listening time, unique content
- **Behavioral Insights**: Most active hours/days, listening streaks
- **Discovery Analysis**: New music found in time periods

### 🔍 Advanced Analytics
- **Listening Patterns**: Analyze habits by time of day, day of week
- **Music Discovery**: Track new artists/tracks discovered over time
- **Genre Distribution**: Music taste analysis (when genre data available)

## API Endpoints

### Top Charts
```bash
# Get top artists for a user (monthly by default)
GET /analytics/artists/top?user_id={user_id}&period=monthly&limit=50

# Get top tracks with custom date range
GET /analytics/tracks/top?user_id={user_id}&start_date=2024-01-01&end_date=2024-01-31

# Get top albums for the past year
GET /analytics/albums/top?user_id={user_id}&period=yearly&limit=20
```

### Time Series Data
```bash
# Daily listening activity for the past month
GET /analytics/listening/timeseries?user_id={user_id}&period=daily&granularity=count

# Weekly listening duration over time
GET /analytics/listening/timeseries?user_id={user_id}&period=weekly&granularity=duration

# Unique tracks discovered over time
GET /analytics/listening/timeseries?user_id={user_id}&period=monthly&granularity=unique_tracks
```

### User Statistics
```bash
# Comprehensive listening statistics
GET /analytics/stats/user?user_id={user_id}&period=monthly

# Listening patterns by hour of day
GET /analytics/listening/patterns?user_id={user_id}&analysis_type=hourly

# Listening patterns by day of week
GET /analytics/listening/patterns?user_id={user_id}&analysis_type=daily_of_week
```

### Discovery Analytics
```bash
# Find newly discovered music
GET /analytics/discovery?user_id={user_id}&period=monthly&threshold=3

# Genre distribution (requires genre metadata)
GET /analytics/genres/distribution?user_id={user_id}&period=monthly
```

## Query Parameters

### Time Periods
- `weekly`: Last 7 days
- `monthly`: Last 30 days (default)
- `yearly`: Last 365 days
- `all_time`: All available data
- Custom: Use `start_date` and `end_date` (YYYY-MM-DD format)

### Granularity Options
- `count`: Number of plays (default)
- `duration`: Total listening time in milliseconds
- `unique_tracks`: Number of unique tracks

### Analysis Types
- `hourly`: Patterns by hour of day (0-23)
- `daily_of_week`: Patterns by day of week (Sunday=0)

## Response Examples

### Top Artists Response
```json
{
  "user_id": "123e4567-e89b-12d3-a456-426614174000",
  "period": "monthly",
  "start_date": "2024-07-16T00:00:00Z",
  "end_date": "2024-08-16T00:00:00Z",
  "total_artists": 45,
  "artists": [
    {
      "artist_id": "456e7890-e89b-12d3-a456-426614174001",
      "artist_name": "Taylor Swift",
      "play_count": 127,
      "total_duration_ms": 32400000,
      "total_duration_minutes": 540.0,
      "unique_tracks": 23,
      "percentage_of_total": 12.5
    }
  ]
}
```

### Time Series Response
```json
{
  "user_id": "123e4567-e89b-12d3-a456-426614174000",
  "period": "daily",
  "granularity": "count",
  "start_date": "2024-07-16T00:00:00Z",
  "end_date": "2024-08-16T00:00:00Z",
  "data_points": [
    {
      "timestamp": "2024-08-01T00:00:00Z",
      "value": 45.0,
      "period_label": "2024-08-01 00:00"
    }
  ],
  "total_value": 1250.0,
  "average_value": 41.67
}
```

### User Statistics Response
```json
{
  "user_id": "123e4567-e89b-12d3-a456-426614174000",
  "period": "monthly",
  "total_plays": 1250,
  "total_listening_time_ms": 312000000,
  "total_listening_time_hours": 86.67,
  "unique_tracks": 345,
  "unique_artists": 67,
  "unique_albums": 89,
  "average_plays_per_day": 41.67,
  "average_listening_time_per_day_hours": 2.89,
  "most_active_day": "Saturday",
  "most_active_hour": 14,
  "listening_streak_days": 25
}
```

## Setup and Running

1. **Environment Setup**: Ensure your database connection is configured in `.env`:
   ```bash
   PG_USER=postgres
   PG_PASSWORD=your_password
   PG_HOST=localhost
   PG_PORT=5432
   PG_DATABASE=music_app
   ```

2. **Install Dependencies**: All required packages are in `requirements.txt`

3. **Run the Service**:
   ```bash
   cd analytics-service
   python app.py
   ```
   
   The service will start on `http://localhost:8002`

4. **API Documentation**: Visit `http://localhost:8002/docs` for interactive API documentation

## Database Requirements

The analytics service uses the existing database schema with these key tables:
- `play_event`: Core listening events with timestamps
- `canonical_artist`, `canonical_track`, `canonical_album`: Normalized music metadata
- `app_user`: User information

## Performance Considerations

- All queries use appropriate indexes on `user_id` and `played_at`
- Time-series queries are optimized with `DATE_TRUNC` for aggregation
- Large datasets benefit from date range restrictions
- Consider adding caching for frequently requested analytics

## Future Enhancements

- **Genre Analytics**: Add genre metadata to tracks for music taste analysis
- **Social Features**: Compare listening habits between users
- **Recommendations**: Use analytics data for music recommendations
- **Export Features**: CSV/JSON export of analytics data
- **Real-time Analytics**: WebSocket support for live listening updates
