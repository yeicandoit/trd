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
        return []


class sql_obj:
    def __init__(self, host="www.taozuiredian.com", user="taozuiredian-ds", password="Wzvd/aDmwJwVhjfodRfcxt+mi2fki", database="taozuiredian"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.db = pymysql.connect(host, user, password, database)
        print "connected to DB"

    def connectdb(self):
        if self.db is None:
            self.db = pymysql.connect(
                self.host, self.user, self.password, self.database)

    def closedb(self):
        if self.db is not None:
            self.db.close()
            self.db = None
        print "DB is closed"

    def querydb(self, sql="show tables", logger=None, message=""):
        if self.db is None:
            self.connectdb()
        cursor = self.db.cursor()
        try:
            timestamp = int(time.time())
            cursor.execute(sql)
            results = cursor.fetchall()
            timestamp_ = int(time.time())
            if logger is not None:
                logger.info("%s took %d second", message,
                            timestamp_ - timestamp)
            return results
        except:
            print "Error: unable to fecth data"
            return []


if __name__ == '__main__':
    connectdb()
    re = querydb()
    print re
    closedb()

    so = sql_obj()
    re = so.querydb()
    print re
    so.closedb()
