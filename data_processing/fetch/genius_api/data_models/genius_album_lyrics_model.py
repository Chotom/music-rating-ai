from typing import List

from pydantic import BaseModel


class TrackModel(BaseModel):
    song_id: str
    song_name: str
    song_number: str
    lyrics: str
    lyrics_state: str
    language: str


class AlbumLyricsModel(BaseModel):
    spotify_id: str
    genius_id: str
    artist_name: str
    album_name: str
    tracks: List[TrackModel]
