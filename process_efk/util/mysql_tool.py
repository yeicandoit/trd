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


def querydb(sql="show tables"):
    if db is None:
        connectdb()
    cursor = db.cursor()
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        return results
    except:
        print "Error: unable to fecth data"
        return {}


if __name__ == '__main__':
    connectdb()
    re = querydb()
    print re
    closedb()
