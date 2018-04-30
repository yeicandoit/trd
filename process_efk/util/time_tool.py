import time
from datetime import datetime, timedelta


# Get timestamp of wee hours for someday
def get_weehours_of_someday(n=0):
    someday = datetime.today() + timedelta(n)
    return int(time.mktime(someday.timetuple()) - \
            someday.hour*3600 - someday.minute*60 - someday.second)


if __name__ == '__main__':
    print get_weehours_of_someday(3)
