from dataclasses import dataclass
from typing import Union


@dataclass
class Review:
    name: str
    date: float
    text: str
    stars: float
    answer: str


@dataclass
class Info:
    name: str
    rating: float
    count_rating: int
    stars: float
