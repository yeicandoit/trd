import requests
import time
import json
from util import time_tool
import logging.config

# Add mapping: curl -H "Content-Type:application/json" -XPOST  http://127.0.0.1:9200/online/doc/_mapping -d '{"properties": {"num_device":{"type":"long"},"num_user":{"type":"long"},"@timestamp":{"type":"date"}}}'

logging.config.fileConfig('conf/log.conf')
logger = logging.getLogger('main')
URL_ELASTICSEARCH_APPLOG = "http://localhost:9200/applog-*/doc/_search"
URL_ELASTICSEARCH_ONLINE_ADD = "http://localhost:9200/online/doc"
JSON_HEADER = {"Content-Type": "application/json"}


def get_query():
    search_data = {
        "size": 0,
        "query": {
            "bool": {
                "must_not": [
                    {
                        "match_phrase": {
                            "user_id.keyword": {
                                "query": "-1"
                            }
                        }
                    }
                ],
                "must": []
            }
        },
        "aggs": {
            "count": {
                "cardinality": {
                    "field": "user_id.keyword",
                }
            }
        }
    }

    return search_data


def get_user_device_count(query_online_user={}, nday=1):
    start = time_tool.get_weehours_of_someday(-nday)
    online = {}
    for i in range(288):  # calculate every 5 minutes
        query_online_user["query"]["bool"]["must"] = [
            {
                "range": {
                    "@timestamp": {
                        "gte": (start + i * 300) * 1000,
                        "lte": (start + (i + 1) * 300) * 1000 - 1,
                        "format": "epoch_millis"
                    }
                }
            }
        ]
        r = requests.post(URL_ELASTICSEARCH_APPLOG, headers=JSON_HEADER,
                          data=json.dumps(query_online_user), timeout=(60, 120))
        timestamp = start + (i + 1) * 300
        time_array = time.localtime(timestamp)
        key = time.strftime("%Y-%m-%d_%H-%M-%S", time_array)
        online[key] = {}
        online[key]["@timestamp"] = time.strftime(
            "%Y-%m-%dT%H:%M:%S+08:00", time_array)
        if 200 == r.status_code:
            r_json = r.json()
            online[key]["num_user"] = r_json["aggregations"]["count"]["value"]
        else:
            logger.error("request applog index failed, status_code:%d, reason:%s",
                         r.status_code, r.reason)

    query_online_user["aggs"]["count"]["cardinality"]["field"] = "device_id.keyword"
    query_online_user["query"]["bool"]["must_not"] = [
        {"match_phrase": {"device_id.keyword": {"query": ""}}}]

    for i in range(288):
        query_online_user["query"]["bool"]["must"] = [
            {
                "range": {
                    "@timestamp": {
                        "gte": (start + i * 300) * 1000,
                        "lte": (start + (i + 1) * 300) * 1000 - 1,
                        "format": "epoch_millis"
                    }
                }
            }

        ]
        r = requests.post(URL_ELASTICSEARCH_APPLOG, headers=JSON_HEADER,
                          data=json.dumps(query_online_user), timeout=(60, 120))
        if 200 == r.status_code:
            r_json = r.json()
            timestamp = start + (i + 1) * 300
            time_array = time.localtime(timestamp)
            key = time.strftime("%Y-%m-%d_%H-%M-%S", time_array)
            online[key]["num_device"] = r_json["aggregations"]["count"]["value"]
        else:
            logger.error("request applog index failed, status_code:%d, reason:%s",
                         r.status_code, r.reason)

    return online


def add_user_device(online):
    for k, v in online.items():
        url = URL_ELASTICSEARCH_ONLINE_ADD + "/" + k
        online_data = {
            "@timestamp": v["@timestamp"],
            "num_device": v["num_device"],
            "num_user": v["num_user"]
        }
        r = requests.post(url, headers=JSON_HEADER,
                          data=json.dumps(online_data), timeout=(60, 120))
        if 200 != r.status_code and 201 != r.status_code:
            logger.error("request online index failed, status_code:%d, reason:%s",
                         r.status_code, r.reason)


if __name__ == '__main__':
    nday = 1
    online = get_user_device_count(get_query(), nday)
    add_user_device(online)
