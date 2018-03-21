# -*- coding: utf-8 -*-
import configparser
import platform

import datetime

import pymssql
from urllib.parse import quote

from yunpian_python_sdk.model import constant as yc
from yunpian_python_sdk.ypclient import YunpianClient

from apscheduler.schedulers.blocking import BlockingScheduler

# 配置文件读取的初始化
conf = configparser.ConfigParser()
if platform.system() == 'Windows':
    conf.read("leave_care_SMS.conf", encoding="utf-8-sig")
else:
    conf.read("leave_care_SMS.conf")
# 连接数据库
conn = pymssql.connect(conf.get('server', 'ip'), conf.get('server', 'user'), conf.get('server', 'password'),
                       database=conf.get('server', 'database'))
cur = conn.cursor()
# 获取模板
templates = {}
# 短信模板
clnt = YunpianClient(conf.get('apikey', 'key'))


def _server_query(today):
    """
    查询服务器获取离职人员
    姓名， 离职日期， 手机号码， 最后审批日期
    :return:
    """
    # self._logger.debug("开始查询超过半小时未处理的离职节点")
    sql = """
    select EmployeeName, FPAEffectiveDate, HRMS_UserField_6, ApproveDate from Wf_biz_DimissionInfo 
    where IsValidStatus = 1 and ApproveDate >=  '{query_today}' and HRMS_UserField_1 = 1
    """.format(query_today=today)
    print(sql)
    cur.execute(sql)
    result = cur.fetchall()
    # print(result)
    return result  # self._logger.debug("查询完成")


def _get_templates():
    for key, value in conf.items(section='template'):
        templates[key] = value


def _SMS_send_str(name, today):
    return templates['离职员工关怀'].format(name=name, day=today)


def _SMS_send(today):
    SMS_str = {}
    for one in _server_query(today):
        name = str(one[0])
        try:
            int(name[len(name) - 1])
            name = name[:len(name) - 1]
        except ValueError:
            name = name
        # print(name)
        # print(one[2])
        SMS_str[one[2]] = _SMS_send_str(name, today)
    # print(SMS_str)
    return SMS_str
    pass


def _send(today):
    tel = []
    data_str = []
    result = _SMS_send(today).items()
    # print(result)
    for key, value in result:
        tel.append(key)
        data_str.append(quote(value))
    if len(tel) !=0:
        param = {yc.MOBILE: ','.join(tel), yc.TEXT: (','.join(data_str))}
        if conf.get(section='SMS', option='status') == 'online':
            clnt.sms().multi_send(param)  # print('发送成功')
        else:
            print(param)
    pass


# 初始化模板
_get_templates()


# 使用 apscheduler 库，实现定时
def test_job():
    # 1、查询金蝶服务器的表【Wf_biz_DimissionInfo】 ，获取当天【ApproveDate >=  '{query_today}'】
    # 离职流程完成【IsValidStatus = 1】且需要发送离职关怀短信【HRMS_UserField_1 = 1】的人员
    # 2、获取信息后，返回至 【_SMS_send()】中，将信息填充至模板，存储在以手机号码开头的字典中（去重）
    # 3、读取字典，将文本做 URL 编码处理，并按照云片的发送格式进行处理，然后在【20:00】投递
    # 适用于 python 3.x版本
    _send(datetime.date.today() + datetime.timedelta(days=0))


scheduler = BlockingScheduler()
scheduler.add_job(test_job, "cron", hour=conf.get('time', 'hour'), minute=conf.get('time', 'minute'))
scheduler.start()
