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
from uuid import UUID
import logging
from fastapi.responses import JSONResponse
from fastapi.requests import Request
from fastapi.exceptions import RequestValidationError

ALLOWED_PERIODS = {"weekly", "monthly", "yearly", "all_time"}
ALLOWED_TIMESERIES_PERIODS = {"hourly", "daily", "weekly", "monthly"}
ALLOWED_GRANULARITY = {"count", "duration", "unique_tracks"}
ALLOWED_ANALYSIS_TYPES = {"hourly", "daily_of_week", "seasonal"}

logger = logging.getLogger("analytics-service")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Music Analytics Service", version="1.0.0")

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(status_code=422, content={
        "error": {"code": "validation_error", "message": "Invalid request parameters", "details": exc.errors()}
    })

@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(status_code=500, content={
        "error": {"code": "internal_error", "message": "An unexpected error occurred"}
    })

analytics_service = AnalyticsService()

@app.on_event("startup")
async def startup():
    # Test DB connection
    try:
        conn = await get_connection()
        try:
            await conn.execute(text("SELECT 1"))
        finally:
            await conn.close()
    except Exception as e:
        logger.error("Database connection failed during startup: %s", e)

@app.get("/health")
async def health():
    try:
        conn = await get_connection()
        try:
            await conn.execute(text("SELECT 1"))
        finally:
            await conn.close()
        return {"status": "ok"}
    except Exception:
        return JSONResponse(status_code=503, content={"status": "degraded", "error": {"code": "db_unavailable"}})

@app.get("/analytics/artists/top", response_model=TopArtistsResponse)
async def get_top_artists(
    user_id: str,
    period: str = Query("monthly", description="Time period: weekly, monthly, yearly, all_time"),
    limit: int = Query(50, description="Number of results to return (1-100)"),
    start_date: Optional[str] = Query(None, description="Custom start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Custom end date (YYYY-MM-DD)")
):
    """Get top artists for a user within a specified time period."""
    # Validate inputs
    try:
        UUID(user_id)
    except Exception:
        raise HTTPException(status_code=400, detail={"code": "invalid_user_id", "message": "user_id must be a valid UUID"})
    if period not in ALLOWED_PERIODS:
        raise HTTPException(status_code=400, detail={"code": "invalid_period", "message": f"period must be one of {sorted(ALLOWED_PERIODS)}"})
    if not (1 <= limit <= 100):
        raise HTTPException(status_code=400, detail={"code": "invalid_limit", "message": "limit must be between 1 and 100"})

    return await analytics_service.get_top_artists(
        user_id=user_id,
        period=period,
        limit=limit,
        start_date=start_date,
        end_date=end_date
    )

@app.get("/analytics/tracks/top", response_model=TopTracksResponse)
async def get_top_tracks(
    user_id: str,
    period: str = Query("monthly", description="Time period: weekly, monthly, yearly, all_time"),
    limit: int = Query(50, description="Number of results to return (1-100)"),
    start_date: Optional[str] = Query(None, description="Custom start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Custom end date (YYYY-MM-DD)")
):
    """Get top tracks for a user within a specified time period."""
    try:
        UUID(user_id)
    except Exception:
        raise HTTPException(status_code=400, detail={"code": "invalid_user_id", "message": "user_id must be a valid UUID"})
    if period not in ALLOWED_PERIODS:
        raise HTTPException(status_code=400, detail={"code": "invalid_period", "message": f"period must be one of {sorted(ALLOWED_PERIODS)}"})
    if not (1 <= limit <= 100):
        raise HTTPException(status_code=400, detail={"code": "invalid_limit", "message": "limit must be between 1 and 100"})

    return await analytics_service.get_top_tracks(
        user_id=user_id,
        period=period,
        limit=limit,
        start_date=start_date,
        end_date=end_date
    )

@app.get("/analytics/albums/top", response_model=TopAlbumsResponse)
async def get_top_albums(
    user_id: str,
    period: str = Query("monthly", description="Time period: weekly, monthly, yearly, all_time"),
    limit: int = Query(50, description="Number of results to return (1-100)"),
    start_date: Optional[str] = Query(None, description="Custom start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Custom end date (YYYY-MM-DD)")
):
    """Get top albums for a user within a specified time period."""
    try:
        UUID(user_id)
    except Exception:
        raise HTTPException(status_code=400, detail={"code": "invalid_user_id", "message": "user_id must be a valid UUID"})
    if period not in ALLOWED_PERIODS:
        raise HTTPException(status_code=400, detail={"code": "invalid_period", "message": f"period must be one of {sorted(ALLOWED_PERIODS)}"})
    if not (1 <= limit <= 100):
        raise HTTPException(status_code=400, detail={"code": "invalid_limit", "message": "limit must be between 1 and 100"})

    return await analytics_service.get_top_albums(
        user_id=user_id,
        period=period,
        limit=limit,
        start_date=start_date,
        end_date=end_date
    )

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
        UUID(user_id)
    except Exception:
        raise HTTPException(status_code=400, detail={"code": "invalid_user_id", "message": "user_id must be a valid UUID"})
    if period not in ALLOWED_TIMESERIES_PERIODS:
        raise HTTPException(status_code=400, detail={"code": "invalid_period", "message": f"period must be one of {sorted(ALLOWED_TIMESERIES_PERIODS)}"})
    if granularity not in ALLOWED_GRANULARITY:
        raise HTTPException(status_code=400, detail={"code": "invalid_granularity", "message": f"granularity must be one of {sorted(ALLOWED_GRANULARITY)}"})

    return await analytics_service.get_listening_timeseries(
        user_id=user_id,
        period=period,
        start_date=start_date,
        end_date=end_date,
        granularity=granularity
    )

@app.get("/analytics/stats/user", response_model=UserListeningStats)
async def get_user_listening_stats(
    user_id: str,
    period: str = Query("monthly", description="Time period: weekly, monthly, yearly, all_time")
):
    """Get comprehensive listening statistics for a user."""
    try:
        UUID(user_id)
    except Exception:
        raise HTTPException(status_code=400, detail={"code": "invalid_user_id", "message": "user_id must be a valid UUID"})
    if period not in ALLOWED_PERIODS:
        raise HTTPException(status_code=400, detail={"code": "invalid_period", "message": f"period must be one of {sorted(ALLOWED_PERIODS)}"})

    return await analytics_service.get_user_listening_stats(
        user_id=user_id,
        period=period
    )

@app.get("/analytics/discovery", response_model=dict)
async def get_music_discovery(
    user_id: str,
    period: str = Query("monthly", description="Time period to analyze"),
    threshold: int = Query(3, description="Minimum plays to consider as 'discovered'")
):
    """Get newly discovered music in a time period."""
    try:
        UUID(user_id)
    except Exception:
        raise HTTPException(status_code=400, detail={"code": "invalid_user_id", "message": "user_id must be a valid UUID"})
    if period not in ALLOWED_PERIODS:
        raise HTTPException(status_code=400, detail={"code": "invalid_period", "message": f"period must be one of {sorted(ALLOWED_PERIODS)}"})
    if threshold < 1:
        raise HTTPException(status_code=400, detail={"code": "invalid_threshold", "message": "threshold must be >= 1"})

    return await analytics_service.get_music_discovery(
        user_id=user_id,
        period=period,
        threshold=threshold
    )

@app.get("/analytics/genres/distribution")
async def get_genre_distribution(
    user_id: str,
    period: str = Query("monthly", description="Time period: weekly, monthly, yearly, all_time")
):
    """Get genre distribution for user's listening habits (if genre data is available)."""
    try:
        UUID(user_id)
    except Exception:
        raise HTTPException(status_code=400, detail={"code": "invalid_user_id", "message": "user_id must be a valid UUID"})
    if period not in ALLOWED_PERIODS:
        raise HTTPException(status_code=400, detail={"code": "invalid_period", "message": f"period must be one of {sorted(ALLOWED_PERIODS)}"})

    return await analytics_service.get_genre_distribution(
        user_id=user_id,
        period=period
    )

@app.get("/analytics/listening/patterns")
async def get_listening_patterns(
    user_id: str,
    analysis_type: str = Query("hourly", description="Analysis type: hourly, daily_of_week, seasonal")
):
    """Analyze user's listening patterns by time of day, day of week, etc."""
    try:
        UUID(user_id)
    except Exception:
        raise HTTPException(status_code=400, detail={"code": "invalid_user_id", "message": "user_id must be a valid UUID"})
    if analysis_type not in ALLOWED_ANALYSIS_TYPES:
        raise HTTPException(status_code=400, detail={"code": "invalid_analysis_type", "message": f"analysis_type must be one of {sorted(ALLOWED_ANALYSIS_TYPES)}"})

    return await analytics_service.get_listening_patterns(
        user_id=user_id,
        analysis_type=analysis_type
    )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)
