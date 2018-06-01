import time
import pymysql


db = None


def connectdb(host="www.taozuiredian.com", user="taozuiredian-ds", password="Wzvd/aDmwJwVhjfodRfcxt+mi2fki", database="taozuiredian"):
    global db
    if db is None:
        db = pymysql.connect(host, user, password, database)
        print "connected to DB"


def closedb():
    global db
    if db is not None:
        db.close()
        db = None
    print "DB is closed"


def querydb(sql="show tables", logger=None, message=""):
    if db is None:
        connectdb()
    cursor = db.cursor()
    try:
        timestamp = int(time.time())
        cursor.execute(sql)
        results = cursor.fetchall()
        timestamp_ = int(time.time())
        if logger is not None:
            logger.info("%s took %d second", message, timestamp_ - timestamp)
        return results
    except:
        print "Error: unable to fecth data"
        return {}


if __name__ == '__main__':
    connectdb()
    re = querydb()
    print re
    closedb()
