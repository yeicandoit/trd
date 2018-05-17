import json
import requests
import new_device


def find_device():
    search_data = {
        "source": {
            "include": ["device_id"]
        },
        "query": {
            "bool": {
                "must": [
                    {
                        "match_phrase": {
                            "device_id.keyword": {
                                "query": ""
                            }
                        }
                    }
                ]
            }
        }
    }


if __name__ == '__main__':
    # devices_hash = {
    #     "129dfda9510829d42a41cc2245ab0d": {
    #         "channel": "jfq-suoping",
    #         "@timestamp": "2018-04-30T15:19:39"
    #     }
    # }
    devices_hash = {}
    with open('device_id_channels.data', 'r') as f:
        for line in f.readlines():
            device_id, channel, first_time = line.strip().split('\t')
            # print '{"device_id":"%s", "@timestamp":"%s"}' % (device_id,
            #                                                  first_time.replace(" ", "T")+"+08:00")
            # devices.append(device_id)
            devices_hash[device_id] = {}
            devices_hash[device_id]["channel"] = channel
            devices_hash[device_id]["@timestamp"] = first_time.replace(
                " ", "T")+"+08:00"
            # print devices_hash
    print len(devices_hash.keys())

    # new_device = new_device.get_new_device(devices_hash.keys())
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
    arr_device_id = devices_hash.keys()
    new_device_ids = []
    old_devices = []
    old_devices_ids = []
    for i in range(0, len(arr_device_id), 10):
        devices_to_verify = arr_device_id[i:i+10]
        search_new_device["query"]["constant_score"]["filter"]["terms"]["device_id.keyword"] = devices_to_verify
        r = requests.post(new_device.URL_ELASTICSEARCH_DEVICE, headers=new_device.JSON_HEADER,
                          data=json.dumps(search_new_device), timeout=(10, 20))
        if 200 == r.status_code:
            r_json = r.json()
            old_devices.extend([device['_source']['device_id']
                                for device in r_json['hits']['hits']])
            old_devices_ids.extend([device['_id']
                                    for device in r_json['hits']['hits']])
            new_device_ids.extend(
                list(set(devices_to_verify) - set(old_devices)))
        else:
            print "request device index failed, status_code:%d, reason:%s" % (
                r.status_code, r.reason)
            print devices_to_verify

    device_data = {
        "device_id": "",
        "@timestamp": "",
        "channel": ""
    }
    for i in range(len(old_devices)):
        device_data["device_id"] = old_devices[i]
        device_data['@timestamp'] = devices_hash[old_devices[i]]["@timestamp"]
        device_data["channel"] = devices_hash[old_devices[i]]["channel"]
        url = new_device.URL_ELASTICSEARCH_DEVICE_ADD + \
            "/" + old_devices_ids[i]
        r = requests.post(url, headers=new_device.JSON_HEADER,
                          data=json.dumps(device_data), timeout=(10, 20))
        if 200 != r.status_code:
            print "update device failed, device_id:%s" % (old_devices[i])
    for d in new_device_ids:
        device_data["device_id"] = d
        device_data['@timestamp'] = devices_hash[d]["@timestamp"]
        device_data["channel"] = devices_hash[d]["channel"]
        url = new_device.URL_ELASTICSEARCH_DEVICE_ADD + "/" + d
        r = requests.post(url, headers=new_device.JSON_HEADER,
                          data=json.dumps(device_data), timeout=(10, 20))
        if 201 != r.status_code:
            print "add device failed:%s, status_code:%d, device_id:%s" % (
                r.reason, r.status_code, d)

    print len(old_devices)
    print len(old_devices_ids)
    print len(new_device_ids)
