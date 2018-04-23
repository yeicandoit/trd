import datetime

if __name__ == '__main__':
    with open('user.csv', 'r') as f:
        for line in f.readlines():
            user_id, first_time = line.strip().split(',')
            timestamp = datetime.datetime.strptime(first_time,'%Y-%m-%d %H:%M:%S').isoformat()
            print '{"user_id":"%s", "@timestamp":"%s"}' %(user_id, 
							  timestamp+"+08:00")

