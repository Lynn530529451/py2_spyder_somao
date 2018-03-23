# -*- coding: utf-8 -*-
import ssl

import datetime

ssl._create_default_https_context = ssl._create_unverified_context  # 全局都取消验证 SSL 证书
import MySQLdb
import traceback
import time
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def get_url():
    try:
        conn = MySQLdb.connect(host='139.198.189.129', user='root', passwd='somao1129', db='bosszhipin', port=20007,charset='utf8')
        cur = conn.cursor()  # 获取一个游标

        conn_in = MySQLdb.connect(host='221.226.72.226', user='root', passwd='somao1129', db='bosszhipin', port=13306, charset='utf8')
        cur_in = conn_in.cursor()  # 获取一个游标

       #  print str(datetime.datetime.now())
        cur.execute('select job_url from boss_add')
        data1 = list(cur.fetchall())

        data1_set = set()
        for i in range(len(data1)):
            data1_set.add(data1[i])

        cur_in.execute('SELECT job_url FROM url_error')
        data2 = list(cur_in.fetchall())
        data2_set = set()
        for i in range(len(data2)):
            data2_set.add(data2[i])

        data = list(data1_set - data2_set)
       # print str(datetime.datetime.now()
        print data[0]

    finally:
        cur.close()  # 关闭游标
        conn.close()  # 释放数据库资源
        cur_in.close()  # 关闭游标
        conn_in.close()  # 释放数据库资源
        # print traceback.format_exc()


get_url()