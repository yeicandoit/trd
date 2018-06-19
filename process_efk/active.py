import requests
import time
import json
from datetime import datetime, timedelta
from util import time_tool
import logging.config

# Add mapping: curl -H "Content-Type:application/json" -XPOST  http://127.0.0.1:9200/active/doc/_mapping -d '{"properties": {"num_device":{"type":"long"},"num_user":{"type":"long"},"@timestamp":{"type":"date"}}}'

logging.config.fileConfig('conf/log.conf')
logger = logging.getLogger('main')
URL_ELASTICSEARCH_APPLOG = "http://localhost:9200/applog-*/doc/_search"
URL_ELASTICSEARCH_ACTIVE_ADD = "http://localhost:9200/active/doc"
JSON_HEADER = {"Content-Type": "application/json"}


def get_query(nday=1):
    start = time_tool.get_weehours_of_someday(nday)
    end = time_tool.get_weehours_of_someday(nday+1)
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
                "must": [
                    {
                        "range": {
                            "@timestamp": {
                                "gte": start * 1000,
                                "lte": end * 1000 - 1,
                                "format": "epoch_millis"
                            }
                        }
                    }
                ]
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


def get_user_device_count(query_active_user={}):
    num_user, num_device = 0, 0
    r = requests.post(URL_ELASTICSEARCH_APPLOG, headers=JSON_HEADER,
                      data=json.dumps(query_active_user), timeout=(60, 120))
    if 200 == r.status_code:
        r_json = r.json()
        num_user = r_json["aggregations"]["count"]["value"]
    else:
        logger.error("request applog index failed, status_code:%d, reason:%s",
                     r.status_code, r.reason)

    query_active_user["aggs"]["count"]["cardinality"]["field"] = "device_id.keyword"
    query_active_user["query"]["bool"]["must_not"] = [
        {"match_phrase": {"device_id.keyword": {"query": ""}}}]

    r = requests.post(URL_ELASTICSEARCH_APPLOG, headers=JSON_HEADER,
                      data=json.dumps(query_active_user), timeout=(60, 120))
    if 200 == r.status_code:
        r_json = r.json()
        num_device = r_json["aggregations"]["count"]["value"]
    else:
        logger.error("request applog index failed, status_code:%d, reason:%s",
                     r.status_code, r.reason)

    return num_user, num_device


def add_user_device(num_user, num_device, someday):
    dt = datetime.today() - timedelta(someday)
    _id = dt.strftime('%Y-%m-%d')
    url = URL_ELASTICSEARCH_ACTIVE_ADD + "/" + _id
    timestamp = dt.isoformat() + "+08:00"
    active_data = {
        "@timestamp": timestamp,
        "num_device": num_device,
        "num_user": num_user
    }
    r = requests.post(url, headers=JSON_HEADER,
                      data=json.dumps(active_data), timeout=(60, 120))
    if 200 != r.status_code and 201 != r.status_code:
        logger.error("request active index failed, status_code:%d, reason:%s",
                     r.status_code, r.reason)


if __name__ == '__main__':
    nday = 1
    num_user, num_device = get_user_device_count(get_query(-nday))
    logger.info("active_user:%d, active_device:%d", num_user, num_device)
    add_user_device(num_user, num_device, nday)
