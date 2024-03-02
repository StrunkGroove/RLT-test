from enum import Enum

from pydantic import BaseModel


class GroupType(str, Enum):
    hour = "hour"
    day = "day"
    month = "month"


class InputData(BaseModel):
    dt_from: str
    dt_upto: str
    group_type: GroupType