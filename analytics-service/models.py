from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from uuid import UUID

class ArtistStats(BaseModel):
    artist_id: UUID
    artist_name: str
    play_count: int
    total_duration_ms: int
    total_duration_minutes: float
    unique_tracks: int
    percentage_of_total: float

class TrackStats(BaseModel):
    track_id: UUID
    track_name: str
    artist_name: str
    album_name: Optional[str]
    play_count: int
    total_duration_ms: int
    total_duration_minutes: float
    first_played: datetime
    last_played: datetime
    percentage_of_total: float

class AlbumStats(BaseModel):
    album_id: Optional[UUID]
    album_name: Optional[str]
    artist_name: str
    play_count: int
    total_duration_ms: int
    total_duration_minutes: float
    unique_tracks: int
    percentage_of_total: float

class TopArtistsResponse(BaseModel):
    user_id: UUID
    period: str
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    total_artists: int
    artists: List[ArtistStats]

class TopTracksResponse(BaseModel):
    user_id: UUID
    period: str
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    total_tracks: int
    tracks: List[TrackStats]

class TopAlbumsResponse(BaseModel):
    user_id: UUID
    period: str
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    total_albums: int
    albums: List[AlbumStats]

class TimeSeriesPoint(BaseModel):
    timestamp: datetime
    value: float
    period_label: str

class TimeSeriesData(BaseModel):
    user_id: UUID
    period: str
    granularity: str
    start_date: datetime
    end_date: datetime
    data_points: List[TimeSeriesPoint]
    total_value: float
    average_value: float

class UserListeningStats(BaseModel):
    user_id: UUID
    period: str
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    total_plays: int
    total_listening_time_ms: int
    total_listening_time_hours: float
    unique_tracks: int
    unique_artists: int
    unique_albums: int
    average_plays_per_day: float
    average_listening_time_per_day_hours: float
    most_active_day: Optional[str]
    most_active_hour: Optional[int]
    listening_streak_days: int

class NewDiscovery(BaseModel):
    track_id: UUID
    track_name: str
    artist_name: str
    album_name: Optional[str]
    first_played: datetime
    play_count_in_period: int
    total_duration_ms: int

class MusicDiscoveryResponse(BaseModel):
    user_id: UUID
    period: str
    start_date: datetime
    end_date: datetime
    new_artists: List[Dict[str, Any]]
    new_tracks: List[NewDiscovery]
    new_albums: List[Dict[str, Any]]
    discovery_rate: float  # percentage of new vs repeated content

class GenreDistribution(BaseModel):
    genre: str
    play_count: int
    percentage: float
    duration_ms: int

class ListeningPattern(BaseModel):
    period_label: str
    play_count: int
    duration_ms: int
    average_tracks_per_session: float
    most_played_artist: Optional[str]
