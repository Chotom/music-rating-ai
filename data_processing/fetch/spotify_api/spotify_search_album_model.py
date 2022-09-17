from __future__ import annotations
from typing import List, Dict, Optional
from pydantic import BaseModel


class Artist(BaseModel):
    href: str
    id: str
    name: str
    type: str
    uri: str


class Image(BaseModel):
    height: int
    url: str
    width: int


class Item(BaseModel):
    album_type: str
    artists: List[Artist]
    href: str
    id: str
    images: List[Image]
    name: str
    release_date: str
    release_date_precision: str
    total_tracks: int
    type: str
    uri: str

    def get_artists_name(self) -> List[str]:
        return [artist.name for artist in self.artists]


class Albums(BaseModel):
    href: str
    items: List[Item]
    limit: int
    next: Optional[str]
    offset: int
    total: int


class Model(BaseModel):
    albums: Albums
