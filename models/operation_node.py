from dataclasses import dataclass, asdict
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
        
    def to_dict(self):
        # Convert enum to string value
        d = asdict(self)
        d['function'] = self.function.value
        return d
