import requests
import json
import copy
import string


URL_ELASTICSEARCH_KIBANA = "http://localhost:9200/.kibana/doc/visualization:%s"
JSON_HEADER = {"Content-Type": "application/json"}
KEYS = []


pv_data = {"type": "visualization", "visualization": {"title": "app_video_detail_bottom_act_show PV", "visState": "{\"title\":\"app_video_detail_bottom_act_show\",\"type\":\"line\",\"params\":{\"type\":\"line\",\"grid\":{\"categoryLines\":false,\"style\":{\"color\":\"#eee\"}},\"categoryAxes\":[{\"id\":\"CategoryAxis-1\",\"type\":\"category\",\"position\":\"bottom\",\"show\":true,\"style\":{},\"scale\":{\"type\":\"linear\"},\"labels\":{\"show\":true,\"truncate\":100},\"title\":{}}],\"valueAxes\":[{\"id\":\"ValueAxis-1\",\"name\":\"LeftAxis-1\",\"type\":\"value\",\"position\":\"left\",\"show\":true,\"style\":{},\"scale\":{\"type\":\"linear\",\"mode\":\"normal\"},\"labels\":{\"show\":true,\"rotate\":0,\"filter\":false,\"truncate\":100},\"title\":{\"text\":\"pv\"}}],\"seriesParams\":[{\"show\":\"true\",\"type\":\"line\",\"mode\":\"normal\",\"data\":{\"label\":\"pv\",\"id\":\"1\"},\"valueAxis\":\"ValueAxis-1\",\"drawLinesBetweenPoints\":true,\"showCircles\":true}],\"addTooltip\":true,\"addLegend\":true,\"legendPosition\":\"right\",\"times\":[],\"addTimeMarker\":false},\"aggs\":[{\"id\":\"1\",\"enabled\":true,\"type\":\"count\",\"schema\":\"metric\",\"params\":{\"customLabel\":\"pv\"}},{\"id\":\"2\",\"enabled\":true,\"type\":\"date_histogram\",\"schema\":\"segment\",\"params\":{\"field\":\"@timestamp\",\"interval\":\"d\",\"customInterval\":\"2h\",\"min_doc_count\":1,\"extended_bounds\":{}}},{\"id\":\"3\",\"enabled\":true,\"type\":\"significant_terms\",\"schema\":\"group\",\"params\":{\"field\":\"channel.keyword\",\"size\":1000}}]}", "uiStateJSON": "{}", "description": "", "version": 1, "kibanaSavedObjectMeta": {
    "searchSourceJSON": "{\"index\":\"5a839130-43ba-11e8-9740-7b993571f659\",\"filter\":[{\"meta\":{\"index\":\"5a839130-43ba-11e8-9740-7b993571f659\",\"negate\":false,\"disabled\":false,\"alias\":null,\"type\":\"phrase\",\"key\":\"key.keyword\",\"value\":\"app_video_detail_bottom_act_show\",\"params\":{\"query\":\"app_video_detail_bottom_act_show\",\"type\":\"phrase\"}},\"query\":{\"match\":{\"key.keyword\":{\"query\":\"app_video_detail_bottom_act_show\",\"type\":\"phrase\"}}},\"$state\":{\"store\":\"appState\"}}],\"query\":{\"query\":\"\",\"language\":\"lucene\"}}"}}}
uv_data = {"type": "visualization", "visualization": {"title": "app_video_detail_bottom_act_show UV", "visState": "{\"title\":\"app_video_detail_bottom_act_show\",\"type\":\"line\",\"params\":{\"type\":\"line\",\"grid\":{\"categoryLines\":false,\"style\":{\"color\":\"#eee\"}},\"categoryAxes\":[{\"id\":\"CategoryAxis-1\",\"type\":\"category\",\"position\":\"bottom\",\"show\":true,\"style\":{},\"scale\":{\"type\":\"linear\"},\"labels\":{\"show\":true,\"truncate\":100},\"title\":{}}],\"valueAxes\":[{\"id\":\"ValueAxis-1\",\"name\":\"LeftAxis-1\",\"type\":\"value\",\"position\":\"left\",\"show\":true,\"style\":{},\"scale\":{\"type\":\"linear\",\"mode\":\"normal\"},\"labels\":{\"show\":true,\"rotate\":0,\"filter\":false,\"truncate\":100},\"title\":{\"text\":\"uv\"}}],\"seriesParams\":[{\"show\":\"true\",\"type\":\"line\",\"mode\":\"normal\",\"data\":{\"label\":\"uv\",\"id\":\"1\"},\"valueAxis\":\"ValueAxis-1\",\"drawLinesBetweenPoints\":true,\"showCircles\":true}],\"addTooltip\":true,\"addLegend\":true,\"legendPosition\":\"right\",\"times\":[],\"addTimeMarker\":false},\"aggs\":[{\"id\":\"1\",\"enabled\":true,\"type\":\"cardinality\",\"schema\":\"metric\",\"params\":{\"field\":\"user_id.keyword\",\"customLabel\":\"uv\"}},{\"id\":\"2\",\"enabled\":true,\"type\":\"date_histogram\",\"schema\":\"segment\",\"params\":{\"field\":\"@timestamp\",\"interval\":\"d\",\"customInterval\":\"2h\",\"min_doc_count\":1,\"extended_bounds\":{}}},{\"id\":\"3\",\"enabled\":true,\"type\":\"significant_terms\",\"schema\":\"group\",\"params\":{\"field\":\"channel.keyword\",\"size\":1000}}]}", "uiStateJSON": "{}", "description": "", "version": 1, "kibanaSavedObjectMeta": {
    "searchSourceJSON": "{\"index\":\"5a839130-43ba-11e8-9740-7b993571f659\",\"filter\":[{\"meta\":{\"index\":\"5a839130-43ba-11e8-9740-7b993571f659\",\"negate\":false,\"disabled\":false,\"alias\":null,\"type\":\"phrase\",\"key\":\"key.keyword\",\"value\":\"app_video_detail_bottom_act_show\",\"params\":{\"query\":\"app_video_detail_bottom_act_show\",\"type\":\"phrase\"}},\"query\":{\"match\":{\"key.keyword\":{\"query\":\"app_video_detail_bottom_act_show\",\"type\":\"phrase\"}}},\"$state\":{\"store\":\"appState\"}},{\"meta\":{\"index\":\"5a839130-43ba-11e8-9740-7b993571f659\",\"negate\":true,\"disabled\":false,\"alias\":null,\"type\":\"phrase\",\"key\":\"user_id.keyword\",\"value\":\"-1\",\"params\":{\"query\":\"-1\",\"type\":\"phrase\"}},\"query\":{\"match\":{\"user_id.keyword\":{\"query\":\"-1\",\"type\":\"phrase\"}}},\"$state\":{\"store\":\"appState\"}}],\"query\":{\"query\":\"\",\"language\":\"lucene\"}}"}}}


def get_keys():
    search_data = {
        "size": 0,
        "aggs": {
            "unique_key": {
                "terms": {
                    "field": "key.keyword",
                    "size": 1000000000
                }
            }
        }
    }
    r = requests.post("http://localhost:9200/applog-2018.04.21/doc/_search", headers=JSON_HEADER,
                      data=json.dumps(search_data), timeout=(30, 60))
    if 200 == r.status_code:
        r_json = r.json()
        keys = [key_obj['key']
                for key_obj in r_json['aggregations']['unique_key']['buckets']]
        # print keys
        # print len(keys)
        return keys
    else:
        print "request applog index failed, status_code:%d, reason:%s" % (
            r.status_code, r.reason)
        return []


def add_pv(data=pv_data):
    with open('data_statistics.csv', 'r') as f:
        for line in f.readlines():
            info = line.strip().split(',')
            key = info[1]
            if string.find(key, "pv") == -1:
                continue
            raw_key = info[2]
            name = info[3].decode("GBK")
            url = URL_ELASTICSEARCH_KIBANA % raw_key + "_PV"
            post_data = copy.deepcopy(data)
            post_data["visualization"]["title"] = name + \
                "(" + raw_key + ")"
            post_data["visualization"]["visState"] = post_data["visualization"]["visState"].replace(
                "app_video_detail_bottom_act_show", name + "(" + raw_key + ")")
            post_data["visualization"]["kibanaSavedObjectMeta"]["searchSourceJSON"] = post_data["visualization"]["kibanaSavedObjectMeta"]["searchSourceJSON"].replace(
                "app_video_detail_bottom_act_show", raw_key)
            # print post_data
            requests.post(url, headers=JSON_HEADER,
                          data=json.dumps(post_data), timeout=(10, 20))


def add_uv(data=uv_data):
    with open('data_statistics.csv', 'r') as f:
        for line in f.readlines():
            info = line.strip().split(',')
            key = info[1]
            if string.find(key, "uv") == -1:
                continue
            raw_key = info[2]
            name = info[3].decode("GBK")
            url = URL_ELASTICSEARCH_KIBANA % raw_key + "_UV"
            post_data = copy.deepcopy(data)
            post_data["visualization"]["title"] = name + \
                "(" + raw_key + ")"
            post_data["visualization"]["visState"] = post_data["visualization"]["visState"].replace(
                "app_video_detail_bottom_act_show", name + "(" + raw_key + ")")
            post_data["visualization"]["kibanaSavedObjectMeta"]["searchSourceJSON"] = post_data["visualization"]["kibanaSavedObjectMeta"]["searchSourceJSON"].replace(
                "app_video_detail_bottom_act_show", raw_key)
            # print post_data
            requests.post(url, headers=JSON_HEADER,
                          data=json.dumps(post_data), timeout=(10, 20))


def del_visualization(keys=[]):
    with open('data_statistics.csv', 'r') as f:
        for line in f.readlines():
            info = line.strip().split(',')
            raw_key = info[2]
            key = info[1]
            if string.find(key, "uv") != -1:
                url_uv = URL_ELASTICSEARCH_KIBANA % raw_key + "_UV"
                requests.delete(url_uv, timeout=(10, 20))
            if string.find(key, "pv") != -1:
                url_pv = URL_ELASTICSEARCH_KIBANA % raw_key + "_PV"
                requests.delete(url_pv, timeout=(10, 20))


def group_visualize(pd=pv_data, ud=uv_data):
    group_arr = []
    with open('data_statistics_group', 'r') as f:
        group_arr = [line.strip() for line in f.readlines()]
    with open('data_statistics.csv', 'r') as f:
        for line in f.readlines():
            info = line.strip().split(',')
            key = info[1]
            raw_key = info[2]
            name = info[3].decode("GBK")
            if raw_key not in group_arr:
                continue
            data = pd
            view_key = "_PV"
            if string.find(key, "pv") != -1:
                pass
            elif string.find(key, "uv") != -1:
                data = ud
                view_key = "_UV"
            else:
                continue
            url = URL_ELASTICSEARCH_KIBANA % raw_key + view_key
            post_data = copy.deepcopy(data)
            post_data["visualization"]["title"] = name + \
                "(" + raw_key + ")"
            post_data["visualization"]["visState"] = post_data["visualization"]["visState"].replace(
                "app_video_detail_bottom_act_show", name + "(" + raw_key + ")")
            post_data["visualization"]["kibanaSavedObjectMeta"]["searchSourceJSON"] = post_data["visualization"]["kibanaSavedObjectMeta"]["searchSourceJSON"].replace(
                "app_video_detail_bottom_act_show", raw_key)

            post_data["visualization"]["kibanaSavedObjectMeta"]["searchSourceJSON"] = post_data["visualization"][
                "kibanaSavedObjectMeta"]["searchSourceJSON"].replace("key.keyword", "group.keyword")
            requests.post(url, headers=JSON_HEADER,
                          data=json.dumps(post_data), timeout=(10, 20))


if __name__ == '__main__':
    add_pv()
    add_uv()
    # del_visualization()
    group_visualize()
