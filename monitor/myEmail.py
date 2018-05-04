# coding=utf-8

import getopt
import sys
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication


def send_email(to, content='', files=None, subject="淘热点监控"):
    user = "wangqiang@optaim.com"
    pwd = "Zhiyunzhong6868"

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = to
    msg.attach(MIMEText(content, 'html', 'utf-8'))

    if None != files:
        for file in files:
            part = MIMEApplication(open(file, 'rb').read())
            part.add_header('Content-Disposition', 'attachment', filename=file)
            msg.attach(part)

    s = smtplib.SMTP("smtp.exmail.qq.com", timeout=30)  # 连接smtp邮件服务器,端口默认是25
    s.login(user, pwd)  # 登陆服务器
    s.sendmail(user, to, msg.as_string())  # 发送邮件
    s.close()


def main(argv):
    t = ""
    c = ""
    s = ""
    b = ""
    p = 1
    try:
        options, args = getopt.getopt(argv, "h:t:c:s:b:p:", [
                                      "help", "to=", "cc=", "subject=", "body=", "priority="])
    except getopt.GetoptError:
        sys.exit()

    for option, value in options:
        if option in ("-h", "--help"):
            print("help")
        if option in ("-t", "--to"):
            print("to is: {0}".format(value))
            t = value
        if option in ("-c", "--cc"):
            print("cc is: {0}".format(value))
            c = value
        if option in ("-s", "--subject"):
            print("subject is: {0}".format(value))
            s = value
        if option in ("-b", "--body"):
            print("body is: {0}".format(value))
            b = value
        if option in ("-p", "--priority"):
            print("priority is: {0}".format(value))
            p = value
    return t, c, s, b, p


if __name__ == '__main__':
    t, c, s, b, p = main(sys.argv[1:])
    send_email(t, content=b, subject=s)
