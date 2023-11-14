from dataclasses import dataclass
from typing import Optional

__all__ = ["Chunk"]


@dataclass
class Chunk:
    nsamples: Optional[int] = 1024
    nvariants: Optional[int] = 1024
