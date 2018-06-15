import time
from datetime import datetime, timedelta


# Get timestamp of wee hours for someday
def get_weehours_of_someday(n=0):
    someday = datetime.today() + timedelta(n)
    return int(time.mktime(someday.timetuple()) - \
            someday.hour*3600 - someday.minute*60 - someday.second)

    
def get_someday_str(someday=0, mformat='%Y-%m-%d'):
    dt = datetime.today() + timedelta(someday)
    return dt.strftime(mformat)
    

def get_someday_es_format(someday=0):
    dt = datetime.today() + timedelta(someday)
    return dt.isoformat() + "+08:00"



if __name__ == '__main__':
    print get_weehours_of_someday(3)
    print get_someday_str(3)
    print get_someday_es_format()
