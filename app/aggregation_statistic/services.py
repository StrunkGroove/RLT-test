from datetime import datetime, timedelta

import pymongo

from dateutil.relativedelta import relativedelta

from .schemas import InputData, GroupType


class AggregationOfStatisticalData:
    """
    Class for aggregating statistical data.

    Attributes:
        collection: MongoDB collection for storing data.
    """
    def __init__(self):
        """
        Initializes the AggregationOfStatisticalData class.

        Connects to the MongoDB database and set the collection.
        """
        client = pymongo.MongoClient("mongodb://mongodb:27017/")
        db = client.mydatabase
        self.collection = db.sample_collection

    def _generate_labels(self, dt_from: datetime, dt_upto: datetime,
                         group_format: dict[GroupType, str],
                         group_type: str) -> list[str]:
        """
        Generates labels for statistical data aggregation.

        Args:
            dt_from: Start date for data aggregation.
            dt_upto: End date for data aggregation.
            group_format: Dictionary mapping group types to date formats.
            group_type: Type of grouping (hour, day, month).

        Returns:
            List of labels generated based on the specified group type.
        """
        current_dt = dt_from
        all_labels = []
        while current_dt <= dt_upto:
            all_labels.append(current_dt.strftime(group_format[group_type]))
            current_dt += timedelta(hours=1) if group_type == GroupType.hour else \
                          timedelta(days=1) if group_type == GroupType.day else \
                          relativedelta(months=1)
        return all_labels

    def _build_pipeline(self, dt_from: datetime, dt_upto: datetime,
                        group_format: dict[GroupType, str],
                        group_type: str) -> list[dict]:
        """
        Builds the MongoDB aggregation pipeline.

        Args:
            dt_from: Start date for data aggregation.
            dt_upto: End date for data aggregation.
            group_format: Dictionary mapping group types to date formats.
            group_type: Type of grouping (hour, day, month).

        Returns:
            MongoDB aggregation pipeline.
        """
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
                            "format": group_format[group_type],
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
        return pipeline

    def _aggregate_data(self, pipeline: list[dict]) -> list[dict]:
        """
        Performs data aggregation using MongoDB pipeline.

        Args:
            pipeline: MongoDB aggregation pipeline.

        Returns:
            Aggregated data.
        """
        result = list(self.collection.aggregate(pipeline))
        return result

    def _fill_missing_values(self, result: list[dict], 
                             all_labels: list[str]) -> dict[str, int]:
        """
        Fills missing values in the aggregated data.

        Args:
            result: Aggregated data.
            all_labels: List of all labels.

        Returns:
            Dictionary with labels and corresponding values.
        """
        data_dict = {item["_id"]: item["total_value"] for item in result}
        for label in all_labels:
            if label not in data_dict:
                data_dict[label] = 0
        return data_dict

    def _sort_data(self, data_dict: dict[str, int],
                   all_labels: list[str]) -> list[int]:
        """
        Sorts aggregated data.

        Args:
            data_dict: Dictionary with labels and corresponding values.
            all_labels: List of all labels.

        Returns:
            Sorted data values.
        """
        sorted_data = [data_dict[label] for label in all_labels]
        return sorted_data

    def main(self, data: InputData) -> str:
        """
        Main method for aggregating statistical data.

        Args:
            data: Input data for data aggregation.

        Returns:
            JSON string containing aggregated data.
        """
        dt_from = datetime.fromisoformat(data.dt_from)
        dt_upto = datetime.fromisoformat(data.dt_upto)

        group_format = {
            GroupType.hour: "%Y-%m-%dT%H:00:00",
            GroupType.day: "%Y-%m-%dT00:00:00",
            GroupType.month: "%Y-%m-01T00:00:00",
        }

        all_labels = self._generate_labels(dt_from, dt_upto, group_format, data.group_type)
        pipeline = self._build_pipeline(dt_from, dt_upto, group_format, data.group_type)
        result = self._aggregate_data(pipeline)
        data_dict = self._fill_missing_values(result, all_labels)
        sorted_data = self._sort_data(data_dict, all_labels)

        response = {"dataset": sorted_data, "labels": all_labels}
        return str(response)

