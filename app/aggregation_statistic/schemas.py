from enum import Enum

from pydantic import BaseModel


class GroupType(str, Enum):
    """
    Enumeration representing different types of grouping for statistical data.
    """
    hour = "hour"
    day = "day"
    month = "month"


class InputData(BaseModel):
    """
    Model representing input data for statistical data aggregation.
    """
    dt_from: str
    dt_upto: str
    group_type: GroupType