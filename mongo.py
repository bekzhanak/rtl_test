import os
import logging
import json

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime, timedelta


def generate_date_range(start_date, end_date, group_type):
    start = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S")
    end = datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S")
    current = start

    if group_type == "hour":
        delta = timedelta(hours=1)
    elif group_type == "day":
        delta = timedelta(days=1)
    else:
        delta = timedelta(days=32)

    while current <= end:
        yield current
        current += delta
        if group_type == "month":
            current = current.replace(
                day=1, hour=0, minute=0, second=0
            )


def aggregate(dt_from, dt_upto, group_type):
    try:
        uri = os.getenv("DB_URI")

        client = MongoClient(uri, server_api=ServerApi("1"))

        client.admin.command("ping")
        logging.log(logging.INFO, "Mongo connection successful")

        database = client["sampleDB"]
        collection = database["sample_collection"]

        labels = [
            (
                date.strftime("%Y-%m-%dT%H:%M:%S")
                if group_type == "hour"
                else date.strftime("%Y-%m-%dT00:00:00")
            )
            for date in generate_date_range(dt_from, dt_upto, group_type)
        ]

        pipeline = [
            {
                "$match": {
                    "dt": {
                        "$gte": datetime.strptime(dt_from, "%Y-%m-%dT%H:%M:%S"),
                        "$lte": datetime.strptime(dt_upto, "%Y-%m-%dT%H:%M:%S"),
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "year": {"$year": "$dt"},
                        "month": {"$month": "$dt"},
                        "day": (
                            {"$dayOfMonth": "$dt"}
                            if group_type in ["day", "hour"]
                            else None
                        ),
                        "hour": {"$hour": "$dt"} if group_type == "hour" else None,
                    },
                    "total_value": {"$sum": "$value"},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "label": {
                        "$dateToString": {
                            "format": (
                                "%Y-%m-%dT%H:%M:%S"
                                if group_type == "hour"
                                else "%Y-%m-%dT00:00:00"
                            ),
                            "date": {
                                "$dateFromParts": {
                                    "year": "$_id.year",
                                    "month": "$_id.month",
                                    "day": (
                                        "$_id.day"
                                        if group_type in ["day", "hour"]
                                        else 1
                                    ),
                                    "hour": "$_id.hour" if group_type == "hour" else 0,
                                    "minute": 0,
                                    "second": 0,
                                }
                            },
                        }
                    },
                    "total_value": 1,
                }
            },
        ]

        aggregated_data = list(collection.aggregate(pipeline))

        data_map = {item["label"]: item["total_value"] for item in aggregated_data}

        dataset = [data_map.get(label, 0) for label in labels]

        output = {"dataset": dataset, "labels": labels}

        client.close()

        return str(json.dumps(output))

    except Exception as e:
        raise Exception("The following error occurred: ", e)
