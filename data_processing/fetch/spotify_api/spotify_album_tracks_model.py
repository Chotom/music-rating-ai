from typing import Any, List
from pydantic import BaseModel


class Artist(BaseModel):
    href: str
    id: str
    name: str
    type: str
    uri: str


class Item(BaseModel):
    artists: List[Artist]
    disc_number: int
    duration_ms: int
    explicit: bool
    href: str
    id: str
    is_local: bool
    name: str
    track_number: int
    type: str
    uri: str


class Tracks(BaseModel):
    href: str
    items: List[Item]
    limit: int
    next: Any
    offset: int
    previous: Any
    total: int


class Album(BaseModel):
    album_type: str
    artists: List[Artist]
    id: str
    name: str
    popularity: int
    release_date: str
    total_tracks: int
    tracks: Tracks
    uri: str


class AlbumInfoModel(BaseModel):
    albums: List[Album]
