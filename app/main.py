import json
from datetime import datetime, timedelta
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
# data ={
#    "dt_from": "2022-10-01T00:00:00",
#    "dt_upto": "2022-11-30T23:59:00",
#    "group_type": "day"
# }
# data = {
#    "dt_from": "2022-02-01T00:00:00",
#    "dt_upto": "2022-02-02T00:00:00",
#    "group_type": "hour"
# }


class AggregationOfStatisticalData:
    def __init__(self):
        client = pymongo.MongoClient("mongodb://mongodb:27017/")
        db = client.mydatabase
        self.collection = db.sample_collection

    def main(self, data: InputData):
        dt_from = datetime.fromisoformat(data.dt_from)
        dt_upto = datetime.fromisoformat(data.dt_upto)

        group_format = {
            GroupType.hour: "%Y-%m-%dT%H:00:00",
            GroupType.day: "%Y-%m-%d",
            GroupType.month: "%Y-%m-01",
        }

        current_dt = dt_from
        all_labels = []
        while current_dt <= dt_upto:
            all_labels.append(current_dt.strftime(group_format[data.group_type]))
            current_dt += timedelta(hours=1) if data.group_type == GroupType.hour else \
                          timedelta(days=1) if data.group_type == GroupType.day else \
                          timedelta(days=30)

        pipeline = [
            {
                "$match": {
                    "dt": {"$gte": dt_from, "$lte": dt_upto}
                }
            },
            {
                "$group": {
                    "_id": {
                        "$dateToString": {
                            "format": group_format[data.group_type],
                            "date": "$dt"
                        }
                    },
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

        data_dict = {item["_id"]: item["total_value"] for item in result}

        for label in all_labels:
            if label not in data_dict:
                data_dict[label] = 0

        sorted_data = [data_dict[label] for label in all_labels]

        response = {"dataset": sorted_data, "labels": all_labels}

        print(json.dumps(response))

agg_data = AggregationOfStatisticalData()
agg_data.main(InputData(**data))
