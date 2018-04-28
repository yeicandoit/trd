import requests
import time
import json
import datetime
import logging.config

logging.config.fileConfig('conf/log.conf')
logger = logging.getLogger('main')

URL_ELASTICSEARCH_APPLOG = "http://localhost:9200/applog-*/doc/_search"
URL_ELASTICSEARCH_USER = "http://localhost:9200/user/doc/_search"
URL_ELASTICSEARCH_USER_ADD = "http://localhost:9200/user/doc"
JSON_HEADER = {"Content-Type": "application/json"}


def get_user(duration=900):
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
                            "user_id.keyword": {
                                "query": "-1"
                            }
                        }
                    }
                ],
                "must": [time_range]
            }
        },
        "aggs": {
            "unique_user": {
                "terms": {
                    "field": "user_id.keyword",
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
        arr_user_id = [user['key']
                         for user in r_json['aggregations']['unique_user']['buckets']]
        logger.debug(arr_user_id)
        logger.debug(len(arr_user_id))
        return arr_user_id
    else:
        logger.error("request applog-%s index failed, status_code:%d, reason:%s",
                     time.strftime("%Y.%m.%d"), r.status_code, r.reason)
        return []


def get_new_user(arr_user_id=[]):
    search_new_user = {
        "_source": {
            "include": [
                "@timestamp",
                "user_id"
            ]
        },
        "query": {
            "constant_score": {
                "filter": {
                    "terms": {
                        "user_id.keyword": []
                    }
                }
            }
        }
    }
    new_user_ids = []
    for i in range(0, len(arr_user_id), 10):
        users_to_verify = arr_user_id[i:i+10]
        search_new_user["query"]["constant_score"]["filter"]["terms"]["user_id.keyword"] = users_to_verify
        r = requests.post(URL_ELASTICSEARCH_USER, headers=JSON_HEADER,
                          data=json.dumps(search_new_user), timeout=(10, 20))
        if 200 == r.status_code:
            r_json = r.json()
            if len(r_json['hits']['hits']) != len(users_to_verify):
                old_users = [user['_source']['user_id']
                               for user in r_json['hits']['hits']]
                new_user_ids.extend(
                    list(set(users_to_verify) - set(old_users)))
        else:
            logger.error("request user index failed, status_code:%d, reason:%s",
                         r.status_code, r.reason)
    logger.debug(new_user_ids)
    logger.debug(len(new_user_ids))
    return new_user_ids


def add_new_user(new_user_ids=[]):
    user_data = {
        "user_id": "",
        "@timestamp": ""
    }
    for user_id in new_user_ids:
        user_data["user_id"] = user_id
        user_data['@timestamp'] = datetime.datetime.today(
        ).isoformat() + "+08:00"
        logger.info(user_data)
        requests.post(URL_ELASTICSEARCH_USER_ADD, headers=JSON_HEADER,
                      data=json.dumps(user_data), timeout=(10, 20))


if __name__ == '__main__':
    user_ids = get_user()
    new_user_ids = get_new_user(user_ids)
    add_new_user(new_user_ids)
