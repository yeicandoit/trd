import datetime

if __name__ == '__main__':
    with open('device.csv', 'r') as f:
        for line in f.readlines():
            device_id, first_time = line.strip().split(',')
            timestamp = datetime.datetime.strptime(first_time,'%Y-%m-%d %H:%M:%S').isoformat()
            print '{"device_id":"%s", "@timestamp":"%s"}' % (device_id,
                                                             timestamp+"+08:00")
