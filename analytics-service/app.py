# app.py
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from analytics import AnalyticsService
from db import get_connection
from models import (
    TopArtistsResponse, TopTracksResponse, TopAlbumsResponse,
    TimeSeriesData, UserListeningStats
)
from sqlalchemy import text
from typing import Optional

app = FastAPI(title="Music Analytics Service", version="1.0.0")

analytics_service = AnalyticsService()

@app.on_event("startup")
async def startup():
    # Test DB connection
    conn = await get_connection()
    try:
        await conn.execute(text("SELECT 1"))
    finally:
        await conn.close()

@app.get("/analytics/artists/top", response_model=TopArtistsResponse)
async def get_top_artists(
    user_id: str,
    period: str = Query("monthly", description="Time period: weekly, monthly, yearly, all_time"),
    limit: int = Query(50, description="Number of results to return"),
    start_date: Optional[str] = Query(None, description="Custom start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Custom end date (YYYY-MM-DD)")
):
    """Get top artists for a user within a specified time period."""
    try:
        return await analytics_service.get_top_artists(
            user_id=user_id,
            period=period,
            limit=limit,
            start_date=start_date,
            end_date=end_date
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/tracks/top", response_model=TopTracksResponse)
async def get_top_tracks(
    user_id: str,
    period: str = Query("monthly", description="Time period: weekly, monthly, yearly, all_time"),
    limit: int = Query(50, description="Number of results to return"),
    start_date: Optional[str] = Query(None, description="Custom start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Custom end date (YYYY-MM-DD)")
):
    """Get top tracks for a user within a specified time period."""
    try:
        return await analytics_service.get_top_tracks(
            user_id=user_id,
            period=period,
            limit=limit,
            start_date=start_date,
            end_date=end_date
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/albums/top", response_model=TopAlbumsResponse)
async def get_top_albums(
    user_id: str,
    period: str = Query("monthly", description="Time period: weekly, monthly, yearly, all_time"),
    limit: int = Query(50, description="Number of results to return"),
    start_date: Optional[str] = Query(None, description="Custom start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Custom end date (YYYY-MM-DD)")
):
    """Get top albums for a user within a specified time period."""
    try:
        return await analytics_service.get_top_albums(
            user_id=user_id,
            period=period,
            limit=limit,
            start_date=start_date,
            end_date=end_date
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/listening/timeseries", response_model=TimeSeriesData)
async def get_listening_timeseries(
    user_id: str,
    period: str = Query("daily", description="Aggregation period: hourly, daily, weekly, monthly"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    granularity: str = Query("count", description="Metric type: count, duration, unique_tracks")
):
    """Get time series data of user's listening habits."""
    try:
        return await analytics_service.get_listening_timeseries(
            user_id=user_id,
            period=period,
            start_date=start_date,
            end_date=end_date,
            granularity=granularity
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/stats/user", response_model=UserListeningStats)
async def get_user_listening_stats(
    user_id: str,
    period: str = Query("monthly", description="Time period: weekly, monthly, yearly, all_time")
):
    """Get comprehensive listening statistics for a user."""
    try:
        return await analytics_service.get_user_listening_stats(
            user_id=user_id,
            period=period
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/discovery", response_model=dict)
async def get_music_discovery(
    user_id: str,
    period: str = Query("monthly", description="Time period to analyze"),
    threshold: int = Query(3, description="Minimum plays to consider as 'discovered'")
):
    """Get newly discovered music in a time period."""
    try:
        return await analytics_service.get_music_discovery(
            user_id=user_id,
            period=period,
            threshold=threshold
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/genres/distribution")
async def get_genre_distribution(
    user_id: str,
    period: str = Query("monthly", description="Time period: weekly, monthly, yearly, all_time")
):
    """Get genre distribution for user's listening habits (if genre data is available)."""
    try:
        return await analytics_service.get_genre_distribution(
            user_id=user_id,
            period=period
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/listening/patterns")
async def get_listening_patterns(
    user_id: str,
    analysis_type: str = Query("hourly", description="Analysis type: hourly, daily_of_week, seasonal")
):
    """Analyze user's listening patterns by time of day, day of week, etc."""
    try:
        return await analytics_service.get_listening_patterns(
            user_id=user_id,
            analysis_type=analysis_type
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)
