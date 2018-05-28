import requests
import time
import json
import online


def get_user_device_count(query_online_user={}):
    now = int(time.time())
    start = now - now % 300 - 2*300
    end = start + 300
    online_data = {}
    query_online_user["query"]["bool"]["must"] = [
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
    r = requests.post(online.URL_ELASTICSEARCH_APPLOG, headers=online.JSON_HEADER,
                      data=json.dumps(query_online_user), timeout=(10, 20))
    time_array = time.localtime(end)
    key = time.strftime("%Y-%m-%d_%H-%M-%S", time_array)
    online_data[key] = {}
    online_data[key]["@timestamp"] = time.strftime(
        "%Y-%m-%dT%H:%M:%S+08:00", time_array)
    if 200 == r.status_code:
        r_json = r.json()
        online_data[key]["num_user"] = r_json["aggregations"]["count"]["value"]
    else:
        online.logger.error("request applog index failed, status_code:%d, reason:%s",
                            r.status_code, r.reason)

    query_online_user["aggs"]["count"]["cardinality"]["field"] = "device_id.keyword"
    query_online_user["query"]["bool"]["must_not"] = [
        {"match_phrase": {"device_id.keyword": {"query": ""}}}]

    r = requests.post(online.URL_ELASTICSEARCH_APPLOG, headers=online.JSON_HEADER,
                      data=json.dumps(query_online_user), timeout=(10, 20))
    if 200 == r.status_code:
        r_json = r.json()
        online_data[key]["num_device"] = r_json["aggregations"]["count"]["value"]
    else:
        online.logger.error("request applog index failed, status_code:%d, reason:%s",
                            r.status_code, r.reason)

    return online_data


if __name__ == '__main__':
    online_data = get_user_device_count(online.get_query())
    online.add_user_device(online_data)
