from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from sqlalchemy import text
from db import get_connection
from models import (
    TopArtistsResponse, TopTracksResponse, TopAlbumsResponse,
    TimeSeriesData, UserListeningStats, MusicDiscoveryResponse,
    ArtistStats, TrackStats, AlbumStats, TimeSeriesPoint,
    NewDiscovery
)
from uuid import UUID
import json

class AnalyticsService:

    def _get_date_range(self, period: str, start_date: Optional[str] = None, end_date: Optional[str] = None):
        """Calculate date range based on period or custom dates."""
        now = datetime.now()

        if start_date and end_date:
            return datetime.fromisoformat(start_date), datetime.fromisoformat(end_date)

        if period == "weekly":
            start = now - timedelta(days=7)
        elif period == "monthly":
            start = now - timedelta(days=30)
        elif period == "yearly":
            start = now - timedelta(days=365)
        elif period == "all_time":
            start = datetime(2000, 1, 1)  # Far enough back
        else:
            start = now - timedelta(days=30)  # Default to monthly

        return start, now

    async def get_top_artists(self, user_id: str, period: str, limit: int,
                            start_date: Optional[str] = None, end_date: Optional[str] = None) -> TopArtistsResponse:
        """Get top artists for a user in a given time period."""
        start_dt, end_dt = self._get_date_range(period, start_date, end_date)

        query = text("""
            WITH artist_stats AS (
                SELECT 
                    ca.id as artist_id,
                    ca.name as artist_name,
                    COUNT(*) as play_count,
                    SUM(pe.duration_ms) as total_duration_ms,
                    COUNT(DISTINCT pe.canonical_track_id) as unique_tracks
                FROM play_event pe
                JOIN canonical_artist ca ON pe.artist_id = ca.id
                WHERE pe.user_id = :user_id
                    AND pe.played_at >= :start_date
                    AND pe.played_at <= :end_date
                GROUP BY ca.id, ca.name
            ),
            total_plays AS (
                SELECT COUNT(*) as total FROM play_event pe
                WHERE pe.user_id = :user_id
                    AND pe.played_at >= :start_date
                    AND pe.played_at <= :end_date
            )
            SELECT 
                ast.artist_id,
                ast.artist_name,
                ast.play_count,
                ast.total_duration_ms,
                ROUND(ast.total_duration_ms::numeric / 60000, 2) as total_duration_minutes,
                ast.unique_tracks,
                ROUND((ast.play_count::numeric / tp.total * 100), 2) as percentage_of_total
            FROM artist_stats ast
            CROSS JOIN total_plays tp
            ORDER BY ast.play_count DESC
            LIMIT :limit
        """)

        conn = await get_connection()
        try:
            result = await conn.execute(query, {
                "user_id": user_id,
                "start_date": start_dt,
                "end_date": end_dt,
                "limit": limit
            })
            rows = result.fetchall()

            # Get total count of unique artists
            count_query = text("""
                SELECT COUNT(DISTINCT pe.artist_id) as total_artists
                FROM play_event pe
                WHERE pe.user_id = :user_id
                    AND pe.played_at >= :start_date
                    AND pe.played_at <= :end_date
            """)
            count_result = await conn.execute(count_query, {
                "user_id": user_id,
                "start_date": start_dt,
                "end_date": end_dt
            })
            total_artists = count_result.fetchone()[0]

            artists = [
                ArtistStats(
                    artist_id=row[0],
                    artist_name=row[1],
                    play_count=row[2],
                    total_duration_ms=row[3],
                    total_duration_minutes=float(row[4]),
                    unique_tracks=row[5],
                    percentage_of_total=float(row[6])
                ) for row in rows
            ]

            return TopArtistsResponse(
                user_id=UUID(user_id),
                period=period,
                start_date=start_dt,
                end_date=end_dt,
                total_artists=total_artists,
                artists=artists
            )
        finally:
            await conn.close()

    async def get_top_tracks(self, user_id: str, period: str, limit: int,
                           start_date: Optional[str] = None, end_date: Optional[str] = None) -> TopTracksResponse:
        """Get top tracks for a user in a given time period."""
        start_dt, end_dt = self._get_date_range(period, start_date, end_date)

        query = text("""
            WITH track_stats AS (
                SELECT 
                    ct.id as track_id,
                    ct.name as track_name,
                    ca.name as artist_name,
                    cal.name as album_name,
                    COUNT(*) as play_count,
                    SUM(pe.duration_ms) as total_duration_ms,
                    MIN(pe.played_at) as first_played,
                    MAX(pe.played_at) as last_played
                FROM play_event pe
                JOIN canonical_track ct ON pe.canonical_track_id = ct.id
                JOIN canonical_artist ca ON pe.artist_id = ca.id
                LEFT JOIN canonical_album cal ON pe.album_id = cal.id
                WHERE pe.user_id = :user_id
                    AND pe.played_at >= :start_date
                    AND pe.played_at <= :end_date
                GROUP BY ct.id, ct.name, ca.name, cal.name
            ),
            total_plays AS (
                SELECT COUNT(*) as total FROM play_event pe
                WHERE pe.user_id = :user_id
                    AND pe.played_at >= :start_date
                    AND pe.played_at <= :end_date
            )
            SELECT 
                ts.track_id,
                ts.track_name,
                ts.artist_name,
                ts.album_name,
                ts.play_count,
                ts.total_duration_ms,
                ROUND(ts.total_duration_ms::numeric / 60000, 2) as total_duration_minutes,
                ts.first_played,
                ts.last_played,
                ROUND((ts.play_count::numeric / tp.total * 100), 2) as percentage_of_total
            FROM track_stats ts
            CROSS JOIN total_plays tp
            ORDER BY ts.play_count DESC
            LIMIT :limit
        """)

        conn = await get_connection()
        try:
            result = await conn.execute(query, {
                "user_id": user_id,
                "start_date": start_dt,
                "end_date": end_dt,
                "limit": limit
            })
            rows = result.fetchall()

            # Get total unique tracks count
            count_query = text("""
                SELECT COUNT(DISTINCT pe.canonical_track_id) as total_tracks
                FROM play_event pe
                WHERE pe.user_id = :user_id
                    AND pe.played_at >= :start_date
                    AND pe.played_at <= :end_date
            """)
            count_result = await conn.execute(count_query, {
                "user_id": user_id,
                "start_date": start_dt,
                "end_date": end_dt
            })
            total_tracks = count_result.fetchone()[0]

            tracks = [
                TrackStats(
                    track_id=row[0],
                    track_name=row[1],
                    artist_name=row[2],
                    album_name=row[3],
                    play_count=row[4],
                    total_duration_ms=row[5],
                    total_duration_minutes=float(row[6]),
                    first_played=row[7],
                    last_played=row[8],
                    percentage_of_total=float(row[9])
                ) for row in rows
            ]

            return TopTracksResponse(
                user_id=UUID(user_id),
                period=period,
                start_date=start_dt,
                end_date=end_dt,
                total_tracks=total_tracks,
                tracks=tracks
            )
        finally:
            await conn.close()

    async def get_top_albums(self, user_id: str, period: str, limit: int,
                           start_date: Optional[str] = None, end_date: Optional[str] = None) -> TopAlbumsResponse:
        """Get top albums for a user in a given time period."""
        start_dt, end_dt = self._get_date_range(period, start_date, end_date)

        query = text("""
            WITH album_stats AS (
                SELECT 
                    cal.id as album_id,
                    cal.name as album_name,
                    ca.name as artist_name,
                    COUNT(*) as play_count,
                    SUM(pe.duration_ms) as total_duration_ms,
                    COUNT(DISTINCT pe.canonical_track_id) as unique_tracks
                FROM play_event pe
                JOIN canonical_artist ca ON pe.artist_id = ca.id
                LEFT JOIN canonical_album cal ON pe.album_id = cal.id
                WHERE pe.user_id = :user_id
                    AND pe.played_at >= :start_date
                    AND pe.played_at <= :end_date
                    AND cal.id IS NOT NULL
                GROUP BY cal.id, cal.name, ca.name
            ),
            total_plays AS (
                SELECT COUNT(*) as total FROM play_event pe
                WHERE pe.user_id = :user_id
                    AND pe.played_at >= :start_date
                    AND pe.played_at <= :end_date
            )
            SELECT 
                ast.album_id,
                ast.album_name,
                ast.artist_name,
                ast.play_count,
                ast.total_duration_ms,
                ROUND(ast.total_duration_ms::numeric / 60000, 2) as total_duration_minutes,
                ast.unique_tracks,
                ROUND((ast.play_count::numeric / tp.total * 100), 2) as percentage_of_total
            FROM album_stats ast
            CROSS JOIN total_plays tp
            ORDER BY ast.play_count DESC
            LIMIT :limit
        """)

        conn = await get_connection()
        try:
            result = await conn.execute(query, {
                "user_id": user_id,
                "start_date": start_dt,
                "end_date": end_dt,
                "limit": limit
            })
            rows = result.fetchall()

            # Get total albums count
            count_query = text("""
                SELECT COUNT(DISTINCT pe.album_id) as total_albums
                FROM play_event pe
                WHERE pe.user_id = :user_id
                    AND pe.played_at >= :start_date
                    AND pe.played_at <= :end_date
                    AND pe.album_id IS NOT NULL
            """)
            count_result = await conn.execute(count_query, {
                "user_id": user_id,
                "start_date": start_dt,
                "end_date": end_dt
            })
            total_albums = count_result.fetchone()[0] or 0

            albums = [
                AlbumStats(
                    album_id=row[0],
                    album_name=row[1],
                    artist_name=row[2],
                    play_count=row[3],
                    total_duration_ms=row[4],
                    total_duration_minutes=float(row[5]),
                    unique_tracks=row[6],
                    percentage_of_total=float(row[7])
                ) for row in rows
            ]

            return TopAlbumsResponse(
                user_id=UUID(user_id),
                period=period,
                start_date=start_dt,
                end_date=end_dt,
                total_albums=total_albums,
                albums=albums
            )
        finally:
            await conn.close()

    async def get_listening_timeseries(self, user_id: str, period: str,
                                     start_date: Optional[str] = None, end_date: Optional[str] = None,
                                     granularity: str = "count") -> TimeSeriesData:
        """Get time series data for user's listening habits."""
        start_dt, end_dt = self._get_date_range("monthly" if not start_date else "custom", start_date, end_date)

        # Define date truncation based on period
        trunc_format = {
            "hourly": "hour",
            "daily": "day",
            "weekly": "week",
            "monthly": "month"
        }.get(period, "day")

        # Define aggregation based on granularity
        if granularity == "count":
            agg_field = "COUNT(*)"
        elif granularity == "duration":
            agg_field = "SUM(pe.duration_ms)"
        elif granularity == "unique_tracks":
            agg_field = "COUNT(DISTINCT pe.canonical_track_id)"
        else:
            agg_field = "COUNT(*)"

        query = text(f"""
            WITH time_series AS (
                SELECT 
                    DATE_TRUNC(:trunc_format, pe.played_at) as time_bucket,
                    {agg_field} as value
                FROM play_event pe
                WHERE pe.user_id = :user_id
                    AND pe.played_at >= :start_date
                    AND pe.played_at <= :end_date
                GROUP BY DATE_TRUNC(:trunc_format, pe.played_at)
                ORDER BY time_bucket
            )
            SELECT 
                time_bucket,
                value,
                TO_CHAR(time_bucket, 'YYYY-MM-DD HH24:MI') as period_label
            FROM time_series
        """)

        conn = await get_connection()
        try:
            result = await conn.execute(query, {
                "user_id": user_id,
                "start_date": start_dt,
                "end_date": end_dt,
                "trunc_format": trunc_format
            })
            rows = result.fetchall()

            data_points = [
                TimeSeriesPoint(
                    timestamp=row[0],
                    value=float(row[1]),
                    period_label=row[2]
                ) for row in rows
            ]

            total_value = sum(point.value for point in data_points)
            average_value = total_value / len(data_points) if data_points else 0

            return TimeSeriesData(
                user_id=UUID(user_id),
                period=period,
                granularity=granularity,
                start_date=start_dt,
                end_date=end_dt,
                data_points=data_points,
                total_value=total_value,
                average_value=average_value
            )
        finally:
            await conn.close()

    async def get_user_listening_stats(self, user_id: str, period: str) -> UserListeningStats:
        """Get comprehensive listening statistics for a user."""
        start_dt, end_dt = self._get_date_range(period)

        query = text("""
            WITH listening_stats AS (
                SELECT 
                    COUNT(*) as total_plays,
                    SUM(pe.duration_ms) as total_duration_ms,
                    COUNT(DISTINCT pe.canonical_track_id) as unique_tracks,
                    COUNT(DISTINCT pe.artist_id) as unique_artists,
                    COUNT(DISTINCT pe.album_id) as unique_albums,
                    EXTRACT(DOW FROM pe.played_at) as day_of_week,
                    EXTRACT(HOUR FROM pe.played_at) as hour_of_day
                FROM play_event pe
                WHERE pe.user_id = :user_id
                    AND pe.played_at >= :start_date
                    AND pe.played_at <= :end_date
                GROUP BY ()
            ),
            daily_stats AS (
                SELECT 
                    DATE_TRUNC('day', pe.played_at) as play_date,
                    COUNT(*) as daily_plays
                FROM play_event pe
                WHERE pe.user_id = :user_id
                    AND pe.played_at >= :start_date
                    AND pe.played_at <= :end_date
                GROUP BY DATE_TRUNC('day', pe.played_at)
            ),
            hourly_distribution AS (
                SELECT 
                    EXTRACT(HOUR FROM pe.played_at) as hour,
                    COUNT(*) as plays
                FROM play_event pe
                WHERE pe.user_id = :user_id
                    AND pe.played_at >= :start_date
                    AND pe.played_at <= :end_date
                GROUP BY EXTRACT(HOUR FROM pe.played_at)
                ORDER BY plays DESC
                LIMIT 1
            ),
            dow_distribution AS (
                SELECT 
                    EXTRACT(DOW FROM pe.played_at) as dow,
                    COUNT(*) as plays
                FROM play_event pe
                WHERE pe.user_id = :user_id
                    AND pe.played_at >= :start_date
                    AND pe.played_at <= :end_date
                GROUP BY EXTRACT(DOW FROM pe.played_at)
                ORDER BY plays DESC
                LIMIT 1
            )
            SELECT 
                ls.total_plays,
                ls.total_duration_ms,
                ROUND(ls.total_duration_ms::numeric / 3600000, 2) as total_hours,
                ls.unique_tracks,
                ls.unique_artists,
                ls.unique_albums,
                ROUND(ls.total_plays::numeric / GREATEST(EXTRACT(DAYS FROM (:end_date - :start_date)), 1), 2) as avg_plays_per_day,
                ROUND((ls.total_duration_ms::numeric / 3600000) / GREATEST(EXTRACT(DAYS FROM (:end_date - :start_date)), 1), 2) as avg_hours_per_day,
                hd.hour as most_active_hour,
                dd.dow as most_active_dow,
                COUNT(DISTINCT ds.play_date) as active_days
            FROM listening_stats ls
            CROSS JOIN hourly_distribution hd
            CROSS JOIN dow_distribution dd
            CROSS JOIN daily_stats ds
            GROUP BY ls.total_plays, ls.total_duration_ms, ls.unique_tracks, 
                     ls.unique_artists, ls.unique_albums, hd.hour, dd.dow
        """)

        conn = await get_connection()
        try:
            result = await conn.execute(query, {
                "user_id": user_id,
                "start_date": start_dt,
                "end_date": end_dt
            })
            row = result.fetchone()

            if not row:
                # Return empty stats if no data
                return UserListeningStats(
                    user_id=UUID(user_id),
                    period=period,
                    start_date=start_dt,
                    end_date=end_dt,
                    total_plays=0,
                    total_listening_time_ms=0,
                    total_listening_time_hours=0.0,
                    unique_tracks=0,
                    unique_artists=0,
                    unique_albums=0,
                    average_plays_per_day=0.0,
                    average_listening_time_per_day_hours=0.0,
                    most_active_day=None,
                    most_active_hour=None,
                    listening_streak_days=0
                )

            # Map day of week number to name
            dow_names = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
            most_active_day = dow_names[int(row[9])] if row[9] is not None else None

            return UserListeningStats(
                user_id=UUID(user_id),
                period=period,
                start_date=start_dt,
                end_date=end_dt,
                total_plays=row[0] or 0,
                total_listening_time_ms=row[1] or 0,
                total_listening_time_hours=float(row[2]) if row[2] else 0.0,
                unique_tracks=row[3] or 0,
                unique_artists=row[4] or 0,
                unique_albums=row[5] or 0,
                average_plays_per_day=float(row[6]) if row[6] else 0.0,
                average_listening_time_per_day_hours=float(row[7]) if row[7] else 0.0,
                most_active_day=most_active_day,
                most_active_hour=int(row[8]) if row[8] is not None else None,
                listening_streak_days=row[10] or 0
            )
        finally:
            await conn.close()

    async def get_music_discovery(self, user_id: str, period: str, threshold: int = 3) -> Dict[str, Any]:
        """Get newly discovered music in a time period."""
        start_dt, end_dt = self._get_date_range(period)

        # Find tracks first played in this period
        query = text("""
            WITH first_plays AS (
                SELECT 
                    pe.canonical_track_id,
                    pe.artist_id,
                    pe.album_id,
                    MIN(pe.played_at) as first_played_ever
                FROM play_event pe
                WHERE pe.user_id = :user_id
                GROUP BY pe.canonical_track_id, pe.artist_id, pe.album_id
            ),
            period_discoveries AS (
                SELECT 
                    fp.canonical_track_id,
                    fp.artist_id,
                    fp.album_id,
                    fp.first_played_ever
                FROM first_plays fp
                WHERE fp.first_played_ever >= :start_date
                    AND fp.first_played_ever <= :end_date
            ),
            discovery_stats AS (
                SELECT 
                    pd.canonical_track_id,
                    ct.name as track_name,
                    ca.name as artist_name,
                    cal.name as album_name,
                    pd.first_played_ever,
                    COUNT(*) as plays_in_period,
                    SUM(pe.duration_ms) as total_duration_ms
                FROM period_discoveries pd
                JOIN canonical_track ct ON pd.canonical_track_id = ct.id
                JOIN canonical_artist ca ON pd.artist_id = ca.id
                LEFT JOIN canonical_album cal ON pd.album_id = cal.id
                JOIN play_event pe ON pe.canonical_track_id = pd.canonical_track_id 
                    AND pe.user_id = :user_id
                    AND pe.played_at >= :start_date
                    AND pe.played_at <= :end_date
                GROUP BY pd.canonical_track_id, ct.name, ca.name, cal.name, pd.first_played_ever
                HAVING COUNT(*) >= :threshold
                ORDER BY plays_in_period DESC
            )
            SELECT * FROM discovery_stats
        """)

        conn = await get_connection()
        try:
            result = await conn.execute(query, {
                "user_id": user_id,
                "start_date": start_dt,
                "end_date": end_dt,
                "threshold": threshold
            })
            rows = result.fetchall()

            new_tracks = [
                NewDiscovery(
                    track_id=row[0],
                    track_name=row[1],
                    artist_name=row[2],
                    album_name=row[3],
                    first_played=row[4],
                    play_count_in_period=row[5],
                    total_duration_ms=row[6]
                ) for row in rows
            ]

            # Calculate discovery rate
            total_plays_query = text("""
                SELECT COUNT(*) FROM play_event pe
                WHERE pe.user_id = :user_id
                    AND pe.played_at >= :start_date
                    AND pe.played_at <= :end_date
            """)
            total_result = await conn.execute(total_plays_query, {
                "user_id": user_id,
                "start_date": start_dt,
                "end_date": end_dt
            })
            total_plays = total_result.fetchone()[0]

            discovery_plays = sum(track.play_count_in_period for track in new_tracks)
            discovery_rate = (discovery_plays / total_plays * 100) if total_plays > 0 else 0

            return {
                "user_id": user_id,
                "period": period,
                "start_date": start_dt,
                "end_date": end_dt,
                "new_tracks": [track.dict() for track in new_tracks],
                "discovery_rate": round(discovery_rate, 2),
                "total_new_tracks": len(new_tracks)
            }
        finally:
            await conn.close()

    async def get_genre_distribution(self, user_id: str, period: str) -> Dict[str, Any]:
        """Get genre distribution (placeholder - requires genre data in metadata)."""
        start_dt, end_dt = self._get_date_range(period)

        # This is a placeholder implementation since genre data would need to be in metadata
        return {
            "user_id": user_id,
            "period": period,
            "message": "Genre distribution requires genre metadata in the canonical_track or canonical_artist tables",
            "suggestion": "Add genre information to the metadata JSONB field and implement genre extraction logic"
        }

    async def get_listening_patterns(self, user_id: str, analysis_type: str) -> Dict[str, Any]:
        """Analyze user's listening patterns by time of day, day of week, etc."""

        if analysis_type == "hourly":
            query = text("""
                SELECT 
                    EXTRACT(HOUR FROM pe.played_at) as hour,
                    COUNT(*) as play_count,
                    SUM(pe.duration_ms) as total_duration_ms,
                    COUNT(DISTINCT pe.canonical_track_id) as unique_tracks,
                    COUNT(DISTINCT pe.artist_id) as unique_artists
                FROM play_event pe
                WHERE pe.user_id = :user_id
                GROUP BY EXTRACT(HOUR FROM pe.played_at)
                ORDER BY hour
            """)
        elif analysis_type == "daily_of_week":
            query = text("""
                SELECT 
                    EXTRACT(DOW FROM pe.played_at) as dow,
                    COUNT(*) as play_count,
                    SUM(pe.duration_ms) as total_duration_ms,
                    COUNT(DISTINCT pe.canonical_track_id) as unique_tracks,
                    COUNT(DISTINCT pe.artist_id) as unique_artists
                FROM play_event pe
                WHERE pe.user_id = :user_id
                GROUP BY EXTRACT(DOW FROM pe.played_at)
                ORDER BY dow
            """)
        else:
            return {"error": "Unsupported analysis_type"}

        conn = await get_connection()
        try:
            result = await conn.execute(query, {"user_id": user_id})
            rows = result.fetchall()

            if analysis_type == "hourly":
                patterns = [
                    {
                        "hour": int(row[0]),
                        "play_count": row[1],
                        "duration_ms": row[2],
                        "unique_tracks": row[3],
                        "unique_artists": row[4]
                    } for row in rows
                ]
            else:  # daily_of_week
                dow_names = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
                patterns = [
                    {
                        "day_of_week": dow_names[int(row[0])],
                        "dow_number": int(row[0]),
                        "play_count": row[1],
                        "duration_ms": row[2],
                        "unique_tracks": row[3],
                        "unique_artists": row[4]
                    } for row in rows
                ]

            return {
                "user_id": user_id,
                "analysis_type": analysis_type,
                "patterns": patterns
            }
        finally:
            await conn.close()
