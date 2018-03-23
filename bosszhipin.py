# -*- coding: utf-8 -*-
"""
Created on Wed Mar  7 09:39:34 2018

@author: pc
"""
import ssl
ssl._create_default_https_context = ssl._create_unverified_context  # 全局都取消验证 SSL 证书
import requests
import re
import random
from bs4 import BeautifulSoup as bs
import datetime
import MySQLdb
import traceback
import time
# import multiprocessing
import threading
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def get_url():
    conn = None
    conn_in = None
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
        return data
    except Exception:
        print traceback.format_exc()

    finally:
        if conn != None:
            cur.close()  # 关闭游标
            conn.close()  # 释放数据库资源
        if conn_in != None:
            cur_in.close()  # 关闭游标
            conn_in.close()  # 释放数据库资源

## 设置代理
def get_proxies():
    proxies = list(set(requests.get(
        "http://60.205.92.109/api.do?name=86020600B1D5E92725E68858AEBCF346&status=1&type=static").text.split('\n')))
    return proxies

## 获取网页
def get_one_parse(url):
    while True:
        try:
            headers = {
                'Host': 'www.zhipin.com',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36'
            }

            proxies = get_proxies()
            index = random.randint(1, len(proxies) - 1)
            proxy = {"http": "http://" + str(proxies[index]), "https": "http://" + str(proxies[index])}
            print 'Now Proxy is : ' + str(proxy) + ' @ ' + str(datetime.datetime.now())
            try:
                response = requests.get(url, timeout=50, proxies=proxy, headers=headers)
            except Exception, e:
                if str(e).find('10061') >= 0:    ## '10060'
                    time.sleep(1)
                    index = random.randint(1, len(proxies) - 1)
                    proxy = {"http": "http://" + str(proxies[index]), "https": "http://" + str(proxies[index])}
                    try:
                        response = requests.get(url, timeout=50, proxies=proxy, headers=headers)
                    except:
                        print '再次尝试失败'
                        return url
                else:
                    print traceback.format_exc()
                    return url
            print response.status_code
            if response.status_code == 200:
                soup = bs(response.text, 'lxml')
                return soup
            else:
                return 'error'
        except:
            print traceback.format_exc()
            return 'error'

## 解析网页，提取数据并存入
def write_data():
    conn_in = None
    try:
        # 数据库连接，注意如果是UTF-8类型的，需要制定数据库(存放数据)
        conn_in = MySQLdb.connect(host='221.226.72.226', user='root', passwd='somao1129', db='bosszhipin', port=13306,
                                  charset='utf8')
        cur_in = conn_in.cursor()  # 获取一个游标
        #    #创建job_details表 (已经有表了，不要再建)
        # cur_in.execute("drop table if exists job_details")
        # sql = """CREATE TABLE IF NOT EXISTS `job_details` (
        #     `job_url` varchar(500),
        #     `job_name` varchar(100),
        #     `employee_name` varchar(100),
        #     `employee_postion` varchar(100),
        #     `salary` varchar(100),
        #     `location` varchar(100),
        #     `exper` varchar(100),
        #     `edu` varchar(20),
        #     `job_label` varchar(100),
        #     `corp_name` varchar(300),
        #     `proceed` varchar(10),
        #     `scale` varchar(100),
        #     `crop_type` varchar(20),
        #     `crop_url` varchar(200),
        #     `job_description` text,
        #     `corp_label` varchar(1000),
        #     `address_detail` varchar(100),
        #     `pub_date` varchar(100),
        #     `timestamp` varchar(100),
        #     `datestamp` varchar(100)
        #   ) ENGINE=InnoDB DEFAULT CHARSET=utf8"""
        # cur_in.execute(sql)
        cur_again = conn_in.cursor()  # 获取一个游标
        # cur_again.execute("drop table if exists url_error")
        # sql_again = """CREATE TABLE IF NOT EXISTS `url_error` (
        #         `job_url` varchar(500) ) ENGINE=InnoDB DEFAULT CHARSET=utf8"""
        # cur_again.execute(sql_again)

        url = get_url()
        for j in url:
            job_url = j[0]
            print job_url
            #        soup = get_one_parse('http://www.zhipin.com/job_detail/1400002739.html')
            soup = get_one_parse(job_url)
            if soup == 'error' or soup == job_url:
                print '需要重新解析该网页'
                sql_url = "insert url_error VALUES ('%s')" % (job_url)
                try:
                    cur_again.execute(sql_url)
                    #          # 提交到数据库执行
                    conn_in.commit()
                except:
                    # 如果发生错误则回滚
                    print traceback.format_exc()
                    conn_in.rollback()
                continue
                ## 在解析的标签下查找需要的内容
            else:
                try:
                    test = soup.select(u"#main > div.job-banner > div > div > div.info-primary")[0]
                    job_name = test.select(u"div.name > h1")[0].text
                    salary = test.select(u"div.name > span")[0].text
                except:
                    job_name = ''
                    salary = ''
                    print 'job_name and salary cannot be found'

                try:
                    pub_date = soup.select(u'div.job-author > span')[0].text
                except:
                    pub_date = ''
                    print 'pub_date cannot be found'
                    # 三个指标都在p标签下,正则拆分
                try:
                    demand_list = test.select("p")[0].text
                    location = re.search(u'城市：(.+)经验：', demand_list).group(1)
                    exper = re.search(u'经验：(.+)学历：', demand_list).group(1)
                    edu = re.search(u'学历：(.+)', demand_list).group(1)
                except:
                    location = ''
                    exper = ''
                    edu = ''
                    print 'demand_list cannot be found'

                try:
                    job_label = test.select(u"div.job-tags > span")[0].text
                except:
                    job_label = ''
                    print 'job_label cannot be found'

                try:
                    company_list = soup.select(u"#main > div.job-banner > div > div > div.info-company")[0].select("p")[0]
                except:
                    company_list = ''
                    print 'company_list cannot be found'
                if company_list != '':
                    try:
                        proceed = re.search('<p>(.+?)<em class="vline"></em>.+人', str(company_list)).group(1)
                    except:
                        proceed = ''
                        print 'proceed cannot be found'
                    if proceed != '':
                        try:
                            scale = re.search('</em>(.+?人.*?)<em', str(company_list)).group(1)  ## <p>0-20人<em class="vline"></em><a href="/i100020/" ka="job-detail-brandindustry">互联网</a></p>

                        except:
                            scale = ''
                            print 'scale cannot be found'
                    else:
                        try:
                            scale = re.search('>(.+?人.*?)<em class="vline"></em>', str(company_list)).group(1)
                        except:
                            scale = ''
                            print 'scale cannot be found'
                    try:
                        crop_type = re.search('ka=.+">(.+?)</a>', str(company_list)).group(1)
                    except:
                        crop_type = ''
                        print 'crop_type cannot be found'
                else:
                    proceed = ''
                    scale = ''
                    crop_type = ''

                try:
                    crop_url = soup.select(u"#main > div.job-banner > div > div > div.info-company")[0].select("p")[1].text
                except:
                    crop_url = ''
                    print 'crop_url cannot be found'
                try:
                    job_description = soup.select(u"#main > div.job-box > div > div.job-detail > div.detail-content")[0].find(
                        'div', class_='text').text
                    job_description = job_description.replace('\n', '').replace(' ', '')
                except:
                    job_description = ''
                    # print traceback.format_exc()
                    print 'job_description cannot be found'

                try:
                    detail_content = soup.select(u"#main > div.job-box > div > div.job-detail > div.detail-content")[0]
                except:
                    detail_content = ''
                    print 'detail_content cannot be found'
                if detail_content != '':
                    try:
                        corp_label = detail_content.find('div', class_='job-tags').text.replace('\n', '/')
                    except:
                        corp_label = ''
                        print 'corp_label cannot be found'
                    try:
                        address_detail = detail_content.find('div', class_='location-address').text
                    except:
                        address_detail = ''
                        print 'address_detail cannot be found'
                else:
                    corp_label = ''
                    address_detail = ''
                try:
                    corp_name = soup.select(u"#main > div.job-box > div > div.job-detail > div.detail-content")[0].find('div',
                                                                                                                        class_='name').text
                except:
                    corp_name = ''
                    print 'corp_name cannot be found'

                    # 补充的指标
                try:
                    employ_info = soup.select(u'#main > div.job-box > div > div.job-detail > div.detail-op')[0]
                except:
                    employ_info = ''
                    print 'employ_info cannot be found'
                if employ_info != '':
                    try:
                        employee_name = employ_info.select('h2')[0].text
                    except:
                        employee_name = ''
                        print 'employee_name cannot be found'
                    try:
                        employee_postion = re.search(u'>(.+?)<em class="vdot">', str(employ_info)).group(1)
                    except:
                        employee_postion = ''
                        print 'employee_postion cannot be found'
                else:
                    employee_name = ''
                    employee_postion = ''

                timestamp = str(datetime.datetime.now())
                # print(timestamp)
                datestamp = str(timestamp)[0:10]

                # user插入数据
                sql = "insert job_details VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
                job_url, job_name, employee_name, employee_postion, salary, location,
                exper, edu, job_label, corp_name, proceed, scale, crop_type,
                crop_url, job_description, corp_label, address_detail, pub_date,
                timestamp, datestamp)
                try:
                    # 执行sql语句
                    cur_in.execute(sql)
                    #          # 提交到数据库执行
                    conn_in.commit()
                except:
                    # 如果发生错误则回滚
                    print traceback.format_exc()
                    conn_in.rollback()
    except Exception,e:
        print '数据获取错误'
    finally:
        if conn_in != None:
            cur_in.close()  # 关闭游标
            cur_again.close()  # 关闭游标
            conn_in.close()  # 释放数据库资源

threads = []
t1 = threading.Thread(target=write_data)
threads.append(t1)
t2 = threading.Thread(target=write_data)
threads.append(t2)
t1 = threading.Thread(target=write_data)
threads.append(t1)
t3 = threading.Thread(target=write_data)
threads.append(t3)
t4 = threading.Thread(target=write_data)
threads.append(t4)
t5 = threading.Thread(target=write_data)
threads.append(t5)
t6 = threading.Thread(target=write_data)
threads.append(t6)
t7 = threading.Thread(target=write_data)
threads.append(t7)
t8 = threading.Thread(target=write_data)
threads.append(t8)
t9 = threading.Thread(target=write_data)
threads.append(t9)
t10 = threading.Thread(target=write_data)
threads.append(t10)
t11 = threading.Thread(target=write_data)
threads.append(t11)
t12 = threading.Thread(target=write_data)
threads.append(t12)

if __name__ == '__main__':
    # url = get_url()
    # proxies = get_proxies()
    for t in threads:
        t.start()
      #  write_data()
