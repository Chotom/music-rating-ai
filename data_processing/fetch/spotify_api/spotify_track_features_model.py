from typing import List, Optional
from pydantic import BaseModel


class AudioFeature(BaseModel):
    """
    Every field in this class explained here:
    https://developer.spotify.com/documentation/web-api/reference/#/operations/get-several-audio-features
    """
    id: str
    danceability: float
    energy: float
    key: int
    loudness: float
    mode: int
    speechiness: float
    acousticness: float
    instrumentalness: float
    liveness: float
    valence: float
    tempo: float
    duration_ms: int
    time_signature: int


class TrackFeatureModel(BaseModel):
    audio_features: List[Optional[AudioFeature]]
