from dataclasses import dataclass
from enum import Enum
from typing import List

class FunctionType(Enum):
    ADD = "ADD"
    MULT = "MULT"
    HASH = "HASH"

@dataclass
class OperationNode:
    id: str
    function: FunctionType
    sources: List[str]
    destination: List[str]

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            id=data["id"],
            function=FunctionType(data["function"]),
            sources=data["sources"],
            destination=data["destination"]
        )
