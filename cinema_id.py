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

# def get_url():
#     try:
#         cur_in.execute('SELECT * FROM citylist')
#         data = list(cur_in.fetchall())
#         return data
#     except Exception:
#         print traceback.format_exc()

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
def write_cinema_id():
    try:
        cur_in.execute("drop table if exists dianping_cinema_id")
        sql = """CREATE TABLE IF NOT EXISTS `dianping_cinema_id` (
                                id                     int(55) NOT NULL auto_increment primary key
                                , fullName             varchar(200)
                                , shopUrl              varchar(200)
                                , cityId               varchar(50)
                                , cityEn               varchar(50)
                                , City                 varchar(50)
                                , regionId             varchar(50)
                                , Region               varchar(200)
                                , keywordId            varchar(50)
                                , keyword              varchar(50)
                                , categoryId           varchar(50)
                                , mainCategoryPrimary  varchar(200)
                                ,list_id               varchar(50)
                                , timestamp            varchar(50)
                                , datestamp            varchar(50)
                                )engine = innodb default charset = utf8"""
        cur_in.execute(sql)
    except Exception:
        print '建储存表错误' + traceback.format_exc()

    try:
        cur_in.execute("drop table if exists cinema_region_error")
        sql_again = """CREATE TABLE IF NOT EXISTS `cinema_region_error` (
                 `region_error` varchar(200)) ENGINE=InnoDB DEFAULT CHARSET=utf8"""     # ,`region_url` varchar(200) ,`shopinfo_url` varchar(200)
        cur_in.execute(sql_again)
    except Exception:
        print '建失误链接表错误' + traceback.format_exc()

    id = 0
    # list_id = 0
    cur_in.execute('SELECT * FROM dianping_cinema_list')
    url_list = list(cur_in.fetchall())
    for j in url_list:
        region_url = j[1]
        print region_url

        #        soup = get_one_parse('http://www.zhipin.com/job_detail/1400002739.html')
        soup0 = get_one_parse(region_url)
        if soup0 == 'error' or soup0 == region_url:
            print '需要重新解析该网页'
            sql_url = "INSERT  cinema_region_error  VALUES ('%s')" % (region_url)
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
        elif soup0 == '页面不存在':
            print '页面不存在'
            continue
        else:
            try:
                page_last = soup0.select(u"body > div.section.Fix.J-shop-search > div.content-wrap > div.shop-wrap > div.page > a")[-2].text
                print 'page_last:' + page_last
            except:
                page_last = '1'
                print 'page_last:' + page_last

            for page in range(1, int(page_last)+1):
                region_url = region_url + 'p{}'.format(page)
                print region_url
                #        soup = get_one_parse('http://www.zhipin.com/job_detail/1400002739.html')
                soup = get_one_parse(region_url)
                if soup == 'error' or soup == region_url:
                    print '需要重新解析该页码的网页'
                    sql_url = "INSERT  cinema_region_error  VALUES ('%s')" % (region_url)
                    try:
                        cur_in.execute(sql_url)
                        # 提交到数据库执行
                        conn_in.commit()
                    except:
                        # 如果发生错误则回滚
                        print '写失误链接表错误' + traceback.format_exc()
                        conn_in.rollback()
                    continue
                    ## 在解析的标签下查找需要的内容
                elif soup == '页面不存在':
                    print '页面不存在'
                    continue
                else:
                    try:
                        test = soup.select(u"#shop-all-list > ul > li")
                        print '正则查找成功'
                    except:
                        print '正则查找失败'
                        continue
                    for i in range(0, len(test)):
                        try:
                            fullName = re.search('<img alt="(.+?)"', str(test[i])).group(1)
                            shopUrl = re.search('href="(.+?)"', str(test[i])).group(1)
                            print 'shopinfo found'
                        except:
                            continue
                        id += 1
                        print id
                        list_id = j[0]
                        cityId = j[2]
                        cityEn = j[3]
                        print cityEn
                        City = j[4]
                        regionId = j[5]
                        Region = j[6]
                        keywordId = j[7]
                        keyword = j[8]
                        categoryId = j[9]
                        mainCategoryPrimary = j[10]

                        timestamp = str(datetime.datetime.now())
                        datestamp = str(timestamp)[0:10]
                        # 插入数据
                        sql = "insert dianping_cinema_id VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
                            id, fullName, shopUrl, cityId, cityEn, City, regionId, Region, keywordId, keyword,
                            categoryId, mainCategoryPrimary, list_id, timestamp, datestamp)
                        try:
                            cur_in.execute(sql)
                            conn_in.commit()
                            print '写入成功'
                        except:
                            # print '数据出错'
                            print '写入表错误' + traceback.format_exc()
                            conn_in.rollback()
                    # except:
                    #     print 'shop information cannot found'

if __name__ == '__main__':
    try:
        conn_in = None
        conn_in = MySQLdb.connect(host='221.226.72.226', user='root', passwd='***', db='dianping20171225',
                                  port=13306, charset='utf8')
        cur_in = conn_in.cursor()  # 获取一个游标

        # data = get_url()
        proxies = get_proxies()
        # time.sleep(50)
        write_cinema_id()
    except Exception:
        print traceback.format_exc()
        print '数据库连接错误'
    finally:
        if conn_in != None:
            cur_in.close()  # 关闭游标
            conn_in.close()  # 释放数据库资源

