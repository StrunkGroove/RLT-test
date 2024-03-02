from datetime import datetime, timedelta
from .schemas import InputData, GroupType

import pymongo


class AggregationOfStatisticalData:
    def __init__(self):
        client = pymongo.MongoClient("mongodb://mongodb:27017/")
        db = client.mydatabase
        self.collection = db.sample_collection

    def main(self, data: InputData) -> str:
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
        return str(response)

