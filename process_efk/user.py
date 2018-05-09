import datetime
import requests
import json

if __name__ == '__main__':
    with open('userlog.csv', 'r') as f:
        for line in f.readlines():
            user_id, registered_at, channel = line.strip().split(',')
            userlog = [{
                "user_id": int(user_id),
                "registered_at": int(registered_at),
                "channel": channel
            }]
            url = "http://127.0.0.1:9010/userlog"
            headers = {"Content-Type": "application/json"}
            r = requests.post(url, headers=headers, data=json.dumps(userlog), timeout=(10,10))
            if 200 != r.status_code:
                print userlog, r.reason
