# coding=utf-8

import poplib
import sys
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication


def send_email(to, content='', files=None, subject=u"ES自动备份"):
    user = "wangqiang@optaim.com"
    pwd = "zhao_WORK68#"

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


if __name__ == '__main__':
    # get_mails('test_email')
    if len(sys.argv) > 1:
        send_email("wangqiang@optaim.com", content=sys.argv[1])
    else:
        send_email("wangqiang@optaim.com", content=u"没有传递邮件内容参数")
