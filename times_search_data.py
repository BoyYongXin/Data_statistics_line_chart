# -*- coding: utf-8 -*-
# @Author: Mr.Yang
# @Date: 2020/5/8 pm 2:37
import csv
import smtplib
import time
import traceback
from datetime import datetime
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from threading import Timer
from venv import logger

# import openpyxl
import pymongo
from pyecharts import options as opts
from pyecharts.charts import Grid
from pyecharts.charts import Line


def write_data(values):
    with open('7日数据量.csv', 'w', encoding="gb2312") as csvFile:
        writer = csv.writer(csvFile)
        for value in values:
            writer.writerow(value)
    # try:
    #     data = openpyxl.load_workbook('7日数据量.xlsx')
    # except:
    #     da = openpyxl.Workbook('7日数据量.xlsx')
    #     da.save('7日数据量.xlsx')
    #     data = openpyxl.load_workbook('7日数据量.xlsx')
    # table = data.active
    # nrows = 1  # 获得行数
    # for value in values:
    #     for raw in range(len(value)):
    #         table.cell(nrows, raw+1).value = value[raw]
    #     nrows = nrows + 1
    # data.save('7日数据量.xlsx')

def deal_time(release_time):
    if len(str(release_time)) > 10:
        release_time = str(release_time)[0:10]
    release_time = int(release_time)
    # 转换成localtime
    release_time = time.localtime(release_time)
    # 转换成新的时间格式(2016-05-05 20:28:54)
    release_time = time.strftime("%Y-%m-%d %H:%M:%S", release_time)
    return release_time


def debug(func):
    """
    抛出异常
    :param func:
    :return:
    """

    def wrapper(*args, **kw):
        try:
            return func(*args, **kw)
        except Exception as err:
            logger.error(err)
            traceback.print_exc()

    return wrapper


class MyTimer(object):

    def __init__(self, start_time, interval, callback_proc, args=None, kwargs=None):
        self.__timer = None
        self.__start_time = start_time
        self.__interval = interval
        self.__callback_pro = callback_proc
        self.__args = args if args is not None else []
        self.__kwargs = kwargs if kwargs is not None else {}

    def exec_callback(self, args=None, kwargs=None):
        self.__callback_pro(*self.__args, **self.__kwargs)
        self.__timer = Timer(self.__interval, self.exec_callback)
        self.__timer.start()

    def start(self):
        interval = self.__interval - (datetime.now().timestamp() - self.__start_time.timestamp())
        print(f"任务将于{deal_time(int(time.time()) + int(interval))}，开始执行")
        self.__timer = Timer(interval, self.exec_callback)
        self.__timer.start()

    def cancel(self):
        self.__timer.cancel()
        self.__timer = None


class AA:
    def __init__(self):
        self.mongo_conn = pymongo.MongoClient(
            '127.0.0.1:27017',
            connect=False
        )

    def get_mongo_data(self):
        db = self.mongo_conn.xuanpin
        zero_point = int(time.time()) - int(time.time() - time.timezone) % 86400 - 86400
        time_list = sorted([zero_point - i * 86400 for i in range(7)])
        time_str_list = [time.strftime("%Y-%m-%d", time.localtime(time_stemp)) for time_stemp in time_list]
        xuanpin_db_list = db.list_collection_names(session=None)
        error_list = ['wemedia_sync', 'weixin_article', 'system.profile', 'wemedia']
        line = Line(init_opts=opts.InitOpts(width='1200px', height='600px'))
        line.set_global_opts(xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=-40), ),
                             yaxis_opts=opts.AxisOpts(name="数据量（条）"), title_opts=opts.TitleOpts(title="全网内容池7日数据量", ),
                             legend_opts=opts.LegendOpts(pos_left='20%'))
        count_list = []
        for name in xuanpin_db_list:
            data_sum_list = list()
            if name in error_list:
                xuanpin_db_list.remove(name)
                continue
            collection = db[name]
            for j in time_list:
                datas = collection.find({"insert_time": {'$gte': j, "$lte": j + 86400}},
                                        {'_id': 1, 'source': 1}).count()
                data_sum_list.append(datas)
            count_list.append(data_sum_list)

            line.add_xaxis(time_str_list)
            line.add_yaxis("{}".format(name), data_sum_list, is_selected=False)
        a = list(map(sum, zip(*count_list[1:])))
        line.add_yaxis("单日总量", a, is_selected=True)

        # 生成Excel
        for tag_data in count_list:
            tag_data.insert(0, xuanpin_db_list[count_list.index(tag_data)])
        time_str_list.insert(0, '')
        count_list.insert(0, time_str_list)
        a.insert(0, 'sum')
        count_list.append(a)
        write_data(count_list)
        # seccuss_list = pd.DataFrame(count_list)
        # seccuss_list.to_excel('7日数据量.xlsx', encoding='utf-8', engine='xlsxwriter')
        grid = Grid()
        grid.add(line, grid_opts=opts.GridOpts(pos_top="30%"))
        grid.render(path='crawl_platform_email.html')  # 折线图
        print("文件已生成！")

    def send_email(self):
        mail_host = "smtp.XXX.com"
        mail_user = "XXX@XXXX.com"
        mail_pass = "password"
        mail_send = "邮箱账号"

        receivers = ['XXX@XXX.com','XXX@XXX2.com']#可多人发送
        message = MIMEMultipart()
        message["From"] = mail_send
        if len(receivers) > 1:
            message['To'] = ','.join(receivers)
        else:
            message['To'] = receivers[0]
        subject = "全网内容池数据统计"
        html_text = '<p><a href="http://127.0.0.1:7881/data_report/">点击链接可以查看7日数据变化曲线哦～</a></p>'
        message["subject"] = Header(subject, "utf-8")
        message.attach(MIMEText(html_text, 'html', 'utf-8'))

        # att1 = MIMEText(open("crawl_platform_email.html", "rb").read(),'base64', 'utf-8')
        # att1["Content-Type"] = 'application/octet-stream'
        # att1["Content-Disposition"] = "attachment; filename=crawl_platform_email.html"
        # message.attach(att1)

        att2 = MIMEText(open('7日数据量.csv', "rb").read(), 'base64', 'utf-8')
        att2["Content-Type"] = 'application/octet-stream'
        att2.add_header('Content-Disposition', 'attachment', filename='crawl_data.csv')
        message.attach(att2)
        try:
            smtpObj = smtplib.SMTP()
            smtpObj.connect(mail_host, 25)
            smtpObj.login(mail_user, mail_pass)
            smtpObj.sendmail(mail_send, receivers, message.as_string())
        except Exception as e:
            print(e)


if __name__ == "__main__":
    aa = AA()
    aa.get_mongo_data()
    # aa.send_email()
    # start1 = datetime.now().replace(hour=0, minute=15, second=0, microsecond=0)
    # start2 = datetime.now().replace(hour=10, minute=5, second=0, microsecond=0)
    # tmr1 = MyTimer(start1, 12 * 60 * 60, aa.get_mongo_data, [])
    # tmr2 = MyTimer(start2, 24 * 60 * 60, aa.send_email, [])
    # tmr1.start()
    # tmr2.start()
    # tmr.cancel()
