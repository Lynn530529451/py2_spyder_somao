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
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def get_url():
    try:
        cur_in.execute('SELECT * FROM citylist')
        data = list(cur_in.fetchall())
        return data
    except Exception:
        print traceback.format_exc()

## 设置代理
def get_proxies():
    proxies = list(set(requests.get(
        "http://60.205.92.109/api.do?name=86020600B1D5E92725E68858AEBCF346&status=1&type=static").text.split('\n')))
    return proxies

## 获取网页
def get_one_parse(dianping_url):
    while True:
        try:
            headers = {
                'Host': 'www.dianping.com',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36',
                'Cookie': 'showNav=javascript:; navCtgScroll=0; _lxsdk_cuid=16077ae40f8c8-0f865c966c2ec6-6e11107f-1fa400-16077ae40f842; _lxsdk=16077ae40f8c8-0f865c966c2ec6-6e11107f-1fa400-16077ae40f842; _hc.v=c8e4756e-e27e-7aa7-df98-073932dea8a0.1513836397; _tr.u=5QFr1hFiHFMDI8tm; cityid=5; _adwp=169583271.2189389436.1514254222.1514542565.1514545124.4; Hm_lvt_4c4fc10949f0d691f3a2cc4ca5065397=1514534363,1515565818; Hm_lvt_dbeeb675516927da776beeb1d9802bd4=1514254209,1515566804; __mta=42243738.1514253447452.1515565727917.1515741853330.51; _lx_utm=utm_source%3DBaidu%26utm_medium%3Dorganic; JSESSIONID=65AEF476529DA81E2D23C643F2545A70; aburl=1; cy=2; cye=beijing; s_ViewType=10; _lxsdk_s=16228a59782-b4c-966-281%7C%7C98'
                      }
            index = random.randint(1, len(proxies) - 1)
            proxy = {"http": "http://" + str(proxies[index]), "https": "http://" + str(proxies[index])}
            print 'Now Proxy is : ' + str(proxy) + ' @ ' + str(datetime.datetime.now())
            try:
                response = requests.get(dianping_url, timeout=50, proxies=proxy, headers=headers)
            except Exception, e:
                if str(e).find('10061') >= 0:
                    time.sleep(1)
                    index = random.randint(1, len(proxies) - 1)
                    proxy = {"http": "http://" + str(proxies[index]), "https": "http://" + str(proxies[index])}
                    try:
                        response = requests.get(dianping_url, timeout=50, proxies=proxy, headers=headers)
                    except:
                        print '再次尝试失败'
                        return dianping_url
                else:
                    print traceback.format_exc()
                    return dianping_url
            print response.status_code
            if response.status_code == 200:
                soup = bs(response.text, 'lxml')
                return soup
            elif response.status_code == 404:
                return '页面不存在'
            else:
                return 'error'
        except:
            print traceback.format_exc()
            return 'error'

## 解析网页，提取数据并存入
def write_cinema_list():
    try:
        cur_in.execute("drop table if exists dianping_cinema_list")
        sql = """CREATE TABLE IF NOT EXISTS `dianping_cinema_list` (
                        id                     int(25) NOT NULL auto_increment primary key
                        , url                  varchar(200)
                        , cityId               varchar(50)
                        , cityEn               varchar(50)
                        , City                 varchar(50)
                        , regionId             varchar(50)
                        , Region               varchar(200)
                        , keywordId            varchar(50)
                        , keyword              varchar(50)
                        , categoryId           varchar(50)
                        , mainCategoryPrimary  varchar(200)
                        , timestamp            varchar(50)
                        , datestamp             varchar(50)
           ) ENGINE=InnoDB DEFAULT CHARSET=utf8"""
        cur_in.execute(sql)
    except Exception:
        print traceback.format_exc()

    try:
        cur_in.execute("drop table if exists dianping_cinema_error")
        sql_again = """CREATE TABLE IF NOT EXISTS `dianping_cinema_error` (
                 `city_url` varchar(200)) ENGINE=InnoDB DEFAULT CHARSET=utf8"""     # ,`region_url` varchar(200) ,`shopinfo_url` varchar(200)
        cur_in.execute(sql_again)
    except Exception:
        print traceback.format_exc()
    id = 0
    # region_cinema_url =[]
    for j in data:
        city_url = 'http://www.dianping.com/' + str(j[2]) + '/ch30/g136'
        print city_url
        #        soup = get_one_parse('http://www.zhipin.com/job_detail/1400002739.html')
        soup = get_one_parse(city_url)
        if soup == 'error' or soup == city_url:
            print '需要重新解析该网页'
            sql_url = "insert dianping_cinema_error VALUES ('%s')" % (city_url)
            try:
                cur_in.execute(sql_url)
                # 提交到数据库执行
                conn_in.commit()
            except:
                # 如果发生错误则回滚
                print traceback.format_exc()
                conn_in.rollback()
            continue
            ## 在解析的标签下查找需要的内容
        elif soup == '页面不存在':
            print '页面不存在'
            continue
        else:
            try:
                test = soup.select(u"#region-nav > a")
                for i in range(len(test)):
                    print test[i]
                    try:
                        url = re.search('href="(.+?)">', str(test[i])).group(1)
                        # region_cinema_url.append(url)
                        regionId = 'r' + re.search('a data-cat-id="(.+?)"', str(test[i])).group(1)
                        Region = test[i].text
                        print Region
                    except:
                        continue
                    id += 1
                    print id
                    cityId = j[1]
                    cityEn = j[2]
                    City = j[3]
                    keywordId = 'ch30'
                    keyword = 'cinema'
                    categoryId = 'g136'
                    mainCategoryPrimary = '电影院'
                    timestamp = str(datetime.datetime.now())
                    datestamp = str(timestamp)[0:10]

                    # user插入数据
                    sql = "insert dianping_cinema_list VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
                        id, url, cityId, cityEn, City, regionId, Region, keywordId, keyword, categoryId,
                        mainCategoryPrimary, timestamp, datestamp)
                    try:
                        # 执行sql语句
                        cur_in.execute(sql)
                        conn_in.commit()
                    except:
                        # 如果发生错误则回滚
                        print traceback.format_exc()
                        conn_in.rollback()

            except:
                print 'region information cannot found'

                # test = soup.select(u"body > div.section.Fix.J-shop-search > div.bread.J_bread > span")



if __name__ == '__main__':
    try:
        conn_in = None
        conn_in = MySQLdb.connect(host='***', user='root', passwd='***', db='dianping20171225',
                                  port=13306, charset='utf8')
        cur_in = conn_in.cursor()  # 获取一个游标

        data = get_url()
        proxies = get_proxies()
        write_cinema_list()
        # time.sleep(50)
        #write_cinema_id()
    except Exception:
        print traceback.format_exc()
        print '数据库连接错误'
    finally:
        if conn_in != None:
            cur_in.close()  # 关闭游标
            conn_in.close()  # 释放数据库资源

