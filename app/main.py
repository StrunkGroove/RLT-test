import json 

from datetime import datetime
from enum import Enum

import pymongo

from pydantic import BaseModel


class GroupType(str, Enum):
    hour = "hour"
    day = "day"
    month = "month"

class InputData(BaseModel):
    dt_from: str
    dt_upto: str
    group_type: GroupType


data = {
   "dt_from": "2022-09-01T00:00:00",
   "dt_upto": "2022-12-31T23:59:00",
   "group_type": "month"
}


class AggregationOfStatisticalData:
    def __init__(self):
        client = pymongo.MongoClient("mongodb://mongodb:27017/")
        db = client.mydatabase
        self.collection = db.sample_collection

    def main(self, data: InputData):
        dt_from = datetime.fromisoformat(data.dt_from)
        dt_upto = datetime.fromisoformat(data.dt_upto)

        pipeline = [
            {
                "$match": {
                    "dt": {"$gte": dt_from, "$lte": dt_upto}
                }
            },
            {
                "$group": {
                    "_id": {"$dateToString": {"format": "%Y-%m-01T00:00:00", "date": "$dt"}},
                    "total_value": {"$sum": "$value"}
                }
            },
            {
                "$sort": {
                    "_id": 1
                }
            }
        ]

        result = list(self.collection.aggregate(pipeline))

        dataset = [item["total_value"] for item in result]
        labels = [item["_id"] for item in result]

        response = {"dataset": dataset, "labels": labels}

        print(json.dumps(response))


agg_data = AggregationOfStatisticalData()
agg_data.main(InputData(**data))



