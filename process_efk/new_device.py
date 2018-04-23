import requests
import time
import json
import datetime
import logging.config

logging.config.fileConfig('conf/log.conf')
logger = logging.getLogger('main')

URL_ELASTICSEARCH_APPLOG = "http://localhost:9200/applog-*/doc/_search"
URL_ELASTICSEARCH_DEVICE = "http://localhost:9200/device/doc/_search"
URL_ELASTICSEARCH_DEVICE_ADD = "http://localhost:9200/device/doc"
JSON_HEADER = {"Content-Type": "application/json"}


def get_device(duration=900):
    now_timestamp_millis = int(time.time()) * 1000
    time_range = {
        "range": {
            "@timestamp": {
                "gte": now_timestamp_millis - duration * 1000,
                "lte": now_timestamp_millis,
                "format": "epoch_millis"
            }
        }
    }
    search_data = {
        "size": 0,
        "query": {
            "bool": {
                "must_not": [
                    {
                        "match_phrase": {
                            "device_id.keyword": {
                                "query": ""
                            }
                        }
                    }
                ],
                "must": [time_range]
            }
        },
        "aggs": {
            "unique_device": {
                "terms": {
                    "field": "device_id.keyword",
                    "size": 1000000000
                }
            }
        }
    }
    # logger.debug(search_data)
    r = requests.post(URL_ELASTICSEARCH_APPLOG, headers=JSON_HEADER,
                      data=json.dumps(search_data), timeout=(30, 60))
    if 200 == r.status_code:
        r_json = r.json()
        # logger.debug(r.json())
        arr_device_id = [device['key']
                         for device in r_json['aggregations']['unique_device']['buckets']]
        logger.debug(arr_device_id)
        logger.debug(len(arr_device_id))
        return arr_device_id
    else:
        logger.error("request applog-%s index failed, status_code:%d, reason:%s",
                     time.strftime("%Y.%m.%d"), r.status_code, r.reason)
        return []


def get_new_device(arr_device_id=[]):
    search_new_device = {
        "_source": {
            "include": [
                "@timestamp",
                "device_id"
            ]
        },
        "query": {
            "constant_score": {
                "filter": {
                    "terms": {
                        "device_id.keyword": []
                    }
                }
            }
        }
    }
    new_device_ids = []
    for i in range(0, len(arr_device_id), 10):
        devices_to_verify = arr_device_id[i:i+10]
        search_new_device["query"]["constant_score"]["filter"]["terms"]["device_id.keyword"] = devices_to_verify
        r = requests.post(URL_ELASTICSEARCH_DEVICE, headers=JSON_HEADER,
                          data=json.dumps(search_new_device), timeout=(10, 20))
        if 200 == r.status_code:
            r_json = r.json()
            if len(r_json['hits']['hits']) != len(devices_to_verify):
                old_devices = [device['_source']['device_id']
                               for device in r_json['hits']['hits']]
                new_device_ids.extend(
                    list(set(devices_to_verify) - set(old_devices)))
        else:
            logger.error("request device index failed, status_code:%d, reason:%s",
                         r.status_code, r.reason)
    logger.debug(new_device_ids)
    logger.debug(len(new_device_ids))
    return new_device_ids


def add_new_device(new_device_ids=[]):
    device_data = {
        "device_id": "",
        "@timestamp": ""
    }
    for device_id in new_device_ids:
        device_data["device_id"] = device_id
        device_data['@timestamp'] = datetime.datetime.today(
        ).isoformat() + "+08:00"
        logger.info(device_data)
        requests.post(URL_ELASTICSEARCH_DEVICE_ADD, headers=JSON_HEADER,
                      data=json.dumps(device_data), timeout=(10, 20))


if __name__ == '__main__':
    device_ids = get_device()
    new_device_ids = get_new_device(device_ids)
    add_new_device(new_device_ids)