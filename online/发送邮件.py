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


def get_zhongwen(db_name):
    source_dict = {
        'baidu': '百度', 'bendibao': '本地宝', 'bilibili': 'B站', 'dayu': 'uc浏览器', 'dazhongdp': '大众点评',
        'fenghuang': '凤凰新闻', 'haokan': '好看视频', 'kuaishou': '快手', 'miaopai': '秒拍', 'pengpai': '澎湃新闻',
        'qiehao': 'qq浏览器', 'qutoutiao': '趣头条', 'renmin': '人民网', 'rss': '同步任务', 'shangyou': '上游新闻',
        'sina': '新浪新闻', 'sohu': '搜狐', 'tengxun': '腾讯新闻', 'toutiao': '今日头条', 'wangyi': '网易', 'weibo': '微博',
        'weixin': '微信', 'xiaoniangao': '小年糕', 'xinhua': '新华网', 'xuanpin': '选品平台任务', 'yangshi': '央视新闻',
        'youguo': '油果', 'youtube': 'Yutube', 'zhidemai': '值得买', 'zhihu': '知乎', 'twitter': 'Twitter',
        'instagram': 'Instagram', 'reddit': 'Reddit', 'facebook': 'Facebook', 'tiktok': 'Tik Tok', 'douyin': '抖音'
    }
    type_dict = {
        'video': '视频', 'news': '图文', 'duanneirong': '短内容', 'quicknews': '快讯'
    }
    data_source, data_type = db_name.split('_')
    c_source = source_dict.get(data_source, data_source)
    c_type = type_dict.get(data_type, data_type)
    return str(c_source) + str(c_type)


def write_data(files, values):
    with open(files, 'w', encoding="gb2312") as csvFile:
        writer = csv.writer(csvFile)
        for value in values:
            writer.writerow(value)


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
            'rs_xuanpin_01.mongo.int.yidian-inc.com:27017,rs_xuanpin_02.mongo.int.yidian-inc.com:27017,rs_xuanpin_03.mongo.int.yidian-inc.com:27017',
            connect=False, username='rs_xuanpin_rw', password='w#JR9tQOVud$jh43V7v#B9Bd#kD4w%'
        )

    def get_mongo_data(self):
        db = self.mongo_conn.xuanpin
        zero_point = int(time.time()) - int(time.time() - time.timezone) % 86400 - 86400
        time_list = sorted([zero_point - i * 86400 for i in range(7)])
        time_str_list = [time.strftime("%Y-%m-%d", time.localtime(time_stemp)) for time_stemp in time_list]
        xuanpin_db_list = db.list_collection_names(session=None)
        error_list = ['wemedia_sync', 'weixin_article', 'system.profile', 'wemedia']
        xuanpin_db_lists = [ll for ll in xuanpin_db_list if ll not in error_list]
        line = Line(init_opts=opts.InitOpts(width='1500px', height='800px'))
        line.set_global_opts(xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=-40), ),
                             yaxis_opts=opts.AxisOpts(name="数据量（条）"),
                             title_opts=opts.TitleOpts(title="全网内容池入库数据量", ),
                             legend_opts=opts.LegendOpts(pos_left='10%', pos_top='5%'))
        count_list = []
        count_dict = {}
        for name in xuanpin_db_lists:
            data_sum_list = list()
            if name in error_list or 'test' in name:
                continue
            collection = db[name]
            for j in time_list:
                datas = collection.find({"insert_time": {'$gte': j, "$lte": j + 86400}},
                                        {'_id': 1, 'source': 1}).count()
                data_sum_list.append(datas)
            count_list.append(data_sum_list)
            line.add_xaxis(time_str_list)
            c_name = get_zhongwen(name)
            line.add_yaxis("{}".format(c_name), data_sum_list, is_selected=False)
            count_dict[c_name]=data_sum_list
        a = list(map(sum, zip(*count_list[0:])))
        line.add_yaxis("单日总量", a, is_selected=True)
        grid = Grid()
        grid.add(line, grid_opts=opts.GridOpts(pos_top="40%"))
        grid.render(path='crawl_platform_email.html')  # 折线图
        # 生成Excel
        m=[]
        for c,d in count_dict.items():
            d.insert(0, c)
            m.append(d)
        time_str_list.insert(0, '')
        m.insert(0, time_str_list)
        a.insert(0, 'sum')
        m.append(a)
        write_data('数据单日总入库量.csv', m)
        # seccuss_list = pd.DataFrame(count_list)
        # seccuss_list.to_excel('7日数据量.xlsx', encoding='utf-8', engine='xlsxwriter')
        print("文件已生成1！")

    def get_mongo_update(self):
        db = self.mongo_conn.xuanpin
        zero_point = int(time.time()) - int(time.time() - time.timezone) % 86400 - 86400
        time_list = sorted([zero_point - i * 86400 for i in range(7)])
        time_str_list = [time.strftime("%Y-%m-%d", time.localtime(time_stemp)) for time_stemp in time_list]
        xuanpin_db_list = db.list_collection_names(session=None)
        error_list = ['wemedia_sync', 'weixin_article', 'system.profile', 'wemedia']
        xuanpin_db_lists = [ll for ll in xuanpin_db_list if ll not in error_list]
        line = Line(init_opts=opts.InitOpts(width='1200px', height='600px'))
        line.set_global_opts(xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=-40), ),
                             yaxis_opts=opts.AxisOpts(name="数据量（条）"),
                             title_opts=opts.TitleOpts(title="全网内容池7天内发布内容数据量"),
                             legend_opts=opts.LegendOpts(pos_left='10%', pos_top='5%'))
        count_list = []
        count_dict = {}
        for name in xuanpin_db_lists:
            data_sum_list = list()
            if name in error_list or 'test' in name:
                continue
            collection = db[name]
            for j in time_list:
                datas = collection.find(
                    {"publish_time": {"$gte": j - 7 * 86400}, 'update_time': {"$gte": j, '$lte': j + 86400}},
                    {"_id": 0, "update_time": 1}).count()
                data_sum_list.append(datas)
            count_list.append(data_sum_list)
            line.add_xaxis(time_str_list)
            c_name = get_zhongwen(name)
            line.add_yaxis("{}".format(c_name), data_sum_list, is_selected=False)
            count_dict[c_name] = data_sum_list
        a = list(map(sum, zip(*count_list[0:])))
        line.add_yaxis("发布时间在7日内的内容总量", a, is_selected=True)
        grid = Grid()
        grid.add(line, grid_opts=opts.GridOpts(pos_top="40%"))
        grid.render(path='crawl_platform_email_7.html')  # 折线图
        # 生成Excel
        m = []
        for c, d in count_dict.items():
            d.insert(0, c)
            m.append(d)
        time_str_list.insert(0, '')
        m.insert(0, time_str_list)
        a.insert(0, 'sum')
        m.append(a)
        write_data('7日内发布内容入库量.csv', m)
        # seccuss_list = pd.DataFrame(count_list)
        # seccuss_list.to_excel('7日数据量.xlsx', encoding='utf-8', engine='xlsxwriter')
        print("文件已生成2！")

    def send_email(self):
        mail_host = "smtp.yidian.com"
        mail_user = "huyanli@yidian-inc.com"
        mail_pass = "Hyl987654"
        mail_send = "huyanli@yidian-inc.com"
        receivers = ['yangyongxin@yidian-inc.com', 'gaomengdan@yidian-inc.com', 'chenxiaotian@yidian-inc.com',
                     'tanqinghui@yidian-inc.com',
                     'huyanli@yidian-inc.com', 'yaoyuntao@yidian-inc.com', 'chenmiao@yidian-inc.com',
                     'wangxinxin@yidian-inc.com', 'lierqiang@yidian-inc.com', 'pengdongneng@yidian-inc.com']
        # receivers = ['huyanli@yidian-inc.com']
        message = MIMEMultipart()
        message["From"] = mail_send
        if len(receivers) > 1:
            message['To'] = ','.join(receivers)
        else:
            message['To'] = receivers[0]
        subject = "全网内容池数据统计"
        html_text = '<p><a href="http://10.103.17.235:7881/data_report/">点击链接可以查看数据单日总入库量变化曲线哦～</a></p>\n'
        html_text += '<p><a href="http://10.103.17.235:7881/data_report_7/">点击链接可以查看7日内发布内容入库量变化曲线哦～</a></p>'
        message["subject"] = Header(subject, "utf-8")
        message.attach(MIMEText(html_text, 'html', 'utf-8'))

        att1 = MIMEText(open('数据单日总入库量.csv', "rb").read(), 'base64', 'utf-8')
        att1["Content-Type"] = 'application/octet-stream'
        att1["Content-Disposition"] = "attachment; filename=crawl_platform_email.html"
        att1.add_header('Content-Disposition', 'attachment', filename='crawl_data.csv')
        message.attach(att1)

        att2 = MIMEText(open('7日内发布内容入库量.csv', "rb").read(), 'base64', 'utf-8')
        att2["Content-Type"] = 'application/octet-stream'
        att2.add_header('Content-Disposition', 'attachment', filename='crawl_data_7_days.csv')
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
    # get_zhongwen('sina_news')
    # aa.get_mongo_update()
    # aa.get_mongo_data()
    # aa.send_email()
    start1 = datetime.now().replace(hour=0, minute=15, second=0, microsecond=0)
    start2 = datetime.now().replace(hour=10, minute=5, second=0, microsecond=0)
    start3 = datetime.now().replace(hour=0, minute=25, second=0, microsecond=0)
    tmr1 = MyTimer(start1, 12 * 60 * 60, aa.get_mongo_data, [])
    tmr2 = MyTimer(start2, 24 * 60 * 60, aa.send_email, [])
    tmr3 = MyTimer(start3, 12 * 60 * 60, aa.get_mongo_update, [])
    tmr1.start()
    tmr2.start()
    tmr3.start()

    # tmr.cancel()
