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
import selenium
from selenium import webdriver
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def get_url():
    try:
        cur_in.execute('SELECT * FROM dianping_cinema_id')
        url_list = list(cur_in.fetchall())
        return url_list
    except Exception:
        print traceback.format_exc()

## 设置代理
def get_proxies():
    proxies = list(set(requests.get(
        "http://60.205.92.109/api.do?name=86020600B1D5E92725E68858AEBCF346&status=1&type=static").text.split('\n')))
    return proxies

## 获取网页
def get_one_parse(shop_url):
    while True:
        try:
            headers = {
                'Host': 'www.dianping.com',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36',
                'Cookie': 'Cookie:_lxsdk_cuid=16077ae40f8c8-0f865c966c2ec6-6e11107f-1fa400-16077ae40f842; _lxsdk=16077ae40f8c8-0f865c966c2ec6-6e11107f-1fa400-16077ae40f842; _hc.v=c8e4756e-e27e-7aa7-df98-073932dea8a0.1513836397; _tr.u=5QFr1hFiHFMDI8tm; cityid=5; _adwp=169583271.2189389436.1514254222.1514542565.1514545124.4; Hm_lvt_4c4fc10949f0d691f3a2cc4ca5065397=1514534363,1515565818; aburl=1; __mta=42243738.1514253447452.1515741853330.1521442911424.52; Hm_lvt_dbeeb675516927da776beeb1d9802bd4=1521442916; s_ViewType=10; _lx_utm=utm_source%3DBaidu%26utm_medium%3Dorganic; dper=d3f60c69e4ca8b80d424a96d7fb30f01aa4348b49413f493d04887eb546929f5; ll=7fd06e815b796be3df069dec7836c3df; ua=dpuser_6549038798; ctu=e5581da1b0424b56b9899658ecdd2c71239095a10204a76c2c22338b45282069; uamo=19951983841; cy=2; cye=beijing; _lxsdk_s=16242576d22-8f8-85c-27d%7C%7C71'
                      }
            index = random.randint(1, len(proxies) - 1)
            proxy = {"http": "http://" + str(proxies[index]), "https": "http://" + str(proxies[index])}
            print 'Now Proxy is : ' + str(proxy) + ' @ ' + str(datetime.datetime.now())
            try:
                response = requests.get(shop_url, timeout=50, proxies=proxy, headers=headers)
            except Exception, e:
                if str(e).find('10061') >= 0 or str(e).find('403') >= 0:
                    time.sleep(1)
                    index = random.randint(1, len(proxies) - 1)
                    proxy = {"http": "http://" + str(proxies[index]), "https": "http://" + str(proxies[index])}
                    try:
                        driver = selenium.webdriver.Chrome()
                        driver.get(shop_url)
                        time.sleep(10)
                        cookie = [item["name"] + "=" + item["value"] for item in driver.get_cookies()]
                        # print cookie
                        cookiestr = ';'.join(item for item in cookie)
                        headers['Cookie'] = cookiestr
                        driver.quit()
                        print 'Cookie 获取成功'
                    except:
                        print 'Cookie 获取失败'
                    try:
                        response = requests.get(shop_url, timeout=50, proxies=proxy, headers=headers)
                    except:
                        print '再次尝试失败'
                        return shop_url
                else:
                    print traceback.format_exc()
                    return shop_url
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
def write_cinema_shopInfo():
    try:
        cur_in.execute("drop table if exists dianping_cinema_shopInfo")
        sql = """CREATE TABLE IF NOT EXISTS `dianping_cinema_shopInfo` (
                 shopId                   int(55) NOT NULL auto_increment primary key 
                ,fullName 				   varchar(500)
                ,shopName 			       varchar(500)
                , otherName				  varchar(500)
                , shopUrl                 varchar(200)
                , City                    varchar(200)
                , categoryName 			  varchar(200)
                , mainCategoryPrimary     varchar(200)
                , mainCategoryName 		  varchar(200)
                , region				  varchar(200)
                , businessDistrict        varchar(200)
                , Score                   varchar(50)
                , Price                   varchar(50)
                , Score1             varchar(50)
                , Score2             varchar(50)
                , Score3            varchar(50)
                , Address                 varchar(500)
                , Tel                     varchar(200)
                , Time                    varchar(200)
                , description			  mediumtext
                ,userId 				varchar(50), 
                shopCityId 				varchar(200), 
                publicTransit 			varchar(500), 
                cityId 					varchar(200), 
                cityCnName 				varchar(200), 
                cityName 				varchar(200), 
                cityEnName 				varchar(200), 
                isOverseasCity 			varchar(200), 
                shopGlat				 varchar(50), 
                shopGlng				 varchar(50), 
                cityGlat				 varchar(50), 
                cityGlng				 varchar(50), 
                Power 					 varchar(50), 
                shopPower 				varchar(50), 
                voteTotal 				varchar(50), 
                shopType 				varchar(50), 
                mainRegionId 			varchar(50), 
                categoryURLName			varchar(50), 
                shopGroupId 			varchar(50), 
                loadUserDomain			varchar(50), 
                manaScore 				varchar(50), 
                mainCategoryId 			varchar(50), 
                defaultPic 				varchar(500),
                timestamp 				varchar(50),
                datestamp 				varchar(50))engine = innodb default charset = utf8"""
        cur_in.execute(sql)
    except Exception:
        print '建储存表错误' + traceback.format_exc()

    try:
        cur_in.execute("drop table if exists cinema_shop_error")
        sql_again = """CREATE TABLE IF NOT EXISTS `cinema_shop_error` (
                 `shop_error` varchar(200)) ENGINE=InnoDB DEFAULT CHARSET=utf8"""     # ,`shop_url` varchar(200) ,`shopinfo_url` varchar(200)
        cur_in.execute(sql_again)
    except Exception:
        print '建失误链接表错误' + traceback.format_exc()

    shopId = 0
    # list_id = 0
    # cur_in.execute('SELECT * FROM dianping_cinema_id')
    # url_list = list(cur_in.fetchall())
    for j in url_list:
        shop_url = j[2]
        print shop_url
        #        soup = get_one_parse('http://www.zhipin.com/job_detail/1400002739.html')
        soup0 = get_one_parse(shop_url)   # 'http://www.dianping.com/shop/5476911'

        if soup0 == 'error' or soup0 == shop_url:
            print '需要重新解析该网页'
            sql_url = "INSERT  cinema_shop_error  VALUES ('%s')" % (shop_url)
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
            # 其他信息 来自其中的字典(取出之后就是字符串了)
            try:
                other_info = re.search('<script> window.shop_config={(.+?)} </script>', str(soup0)).group(1)
            except:
                print 'other_info 没有找到'
                sql_url = "INSERT  cinema_shop_error  VALUES ('%s')" % (shop_url)
                try:
                    cur_in.execute(sql_url)
                    # 提交到数据库执行
                    conn_in.commit()
                except:
                    # 如果发生错误则回滚
                    print traceback.format_exc()
                    conn_in.rollback()
                continue
                # Address = ''
                # userId = ''
                # shopName = ''
                # otherName = ''
                # shopCityId = ''
                # categoryName = ''
                # mainCategoryName = ''
                # publicTransit = ''
                # cityId = ''
                # cityCnName = ''
                # cityName = ''
                # cityEnName = ''
                # isOverseasCity = ''
                # shopGlat = ''
                # shopGlng = ''
                # cityGlat = ''
                # cityGlng = ''
                # Power = ''
                # shopPower = ''
                # Score = ''
                # voteTotal = ''
                # shopType = ''
                # mainRegionId = ''
                # categoryURLName = ''
                # shopGroupId = ''
                # loadUserDomain = ''
                # manaScore = ''
                # defaultPic = ''

            try:
                Address = re.search('address: "(.+?)",', str(other_info)).group(1)
            except:
                Address = ''
            try:
                userId = re.search('userId:(.+?),', str(other_info)).group(1)
            except:
                userId = ''
            try:
                shopName = re.search('shopName: "(.+?)",', str(other_info)).group(1)
            except:
                shopName = ''
            try:
                otherName = re.search('otherName: "(.*?)",', str(other_info)).group(1)
            except:
                otherName = ''
            try:
                shopCityId = re.search('shopCityId:(.+?),', str(other_info)).group(1)
            except:
                shopCityId = ''
            try:
                categoryName = re.search('userId:(.+?),', str(other_info)).group(1)
            except:
                categoryName = ''
            try:
                mainCategoryName = re.search('userId:(.+?),', str(other_info)).group(1)
            except:
                mainCategoryName = ''
            try:  # publicTransit: "中影集团大门与小西天牌楼中间",
                publicTransit = re.search('publicTransit: "(.*?)", cityId', str(other_info)).group(1)
            except:
                publicTransit = ''
            try:
                cityId = re.search('cityId: "(.+?)",', str(other_info)).group(1)  # 此表cityId 实际为 regionId
                cityCnName = re.search('cityCnName: "(.+?)",', str(other_info)).group(1)
                cityName = re.search('cityName: "(.+?)",', str(other_info)).group(1)
                cityEnName = re.search('cityEnName: "(.+?)",', str(other_info)).group(1)
            except:
                cityId = ''
                cityCnName = ''
                cityName = ''
                cityEnName = ''
            try:
                isOverseasCity = re.search('isOverseasCity: (.+?),', str(other_info)).group(1)
            except:
                isOverseasCity = ''
            try:
                shopGlat = re.search('shopGlat: "(.+?)",', str(other_info)).group(1)
                shopGlng = re.search('shopGlng:"(.+?)",', str(other_info)).group(1)
                cityGlat = re.search('cityGlat:"(.+?)",', str(other_info)).group(1)
                cityGlng = re.search('cityGlng:"(.+?)",', str(other_info)).group(1)
            except:
                shopGlat = ''
                shopGlng = ''
                cityGlat = ''
                cityGlng = ''

            try:
                Power = re.search('power:(.+?),', str(other_info)).group(1)
                shopPower = re.search('shopPower:(.+?),', str(other_info)).group(1)
                Score = str(int(shopPower) / 10.0)
            except:
                Power = ''
                shopPower = ''
                Score = ''
            try:
                voteTotal = re.search('voteTotal:(.+?),', str(other_info)).group(1)
                shopType = re.search('shopType:(.+?),', str(other_info)).group(1)
                mainRegionId = re.search('mainRegionId:(.+?),', str(other_info)).group(1)
                categoryURLName = re.search('categoryURLName:"(.*?)", shopGroupId', str(other_info)).group(1)
                shopGroupId = re.search('shopGroupId:(.+?),', str(other_info)).group(1)
            except:
                voteTotal = ''
                shopType = ''
                mainRegionId = ''
                categoryURLName = ''
                shopGroupId = ''
            try:
                loadUserDomain = re.search('loadUserDomain:"(.+?)", map', str(other_info)).group(1)
                print loadUserDomain
            except:
                loadUserDomain = ''
            try:
                manaScore = re.search('manaScore:"(.*?)" },', str(other_info)).group(1)
                print manaScore
            except:
                manaScore = ''
            try:
                defaultPic = re.search('defaultPic:"(.+?)"', str(other_info)).group(1)
                print defaultPic
            except:
                defaultPic = ''

            shopUrl = shop_url
            try:
                st0 = soup0.select(u"#body > div > div.breadcrumb")[0].text   # re.findall(u'商户已关闭', html) 还未考虑
                businessDistrict = st0.split('>')[-2]
            except:
                businessDistrict = ''

            try:
                score_info = soup0.select(u"#basic-info > div.brief-info")
                Price = re.search('<span class="item" id="avgPriceTitle">人均:(.+?)元</span>', str(score_info[0])).group(1)
                Score1 = re.search('视效:(.+?)</span>', str(score_info[0])).group(1)
                Score2 = re.search('环境:(.+?)</span>', str(score_info[0])).group(1)
                Score3 = re.search('服务:(.+?)</span>', str(score_info[0])).group(1)
            except:
                Price = ''
                Score1 = ''
                Score2 = ''
                Score3 = ''
            try:
                Tel = soup0.select(u"#basic-info > p > span.item")[0].text
            except:
                Tel = ''

            try:
                yingyeInfo = soup0.select(u"#basic-info > div.other.J-other > p")
            except:
                Time = ''
                description = ''
            try:
                Time = re.search('<span class="item"> (.+?)</span>', str(yingyeInfo[0])).group(1)
            except:
                Time = ''
            try:
                Description = yingyeInfo[-1].text.replace(u'商户简介：','')
            except:
                Description = ''

            shopId += 1
            fullName = j[1]
            cityEn = j[4]
            City = j[5]
            mainCategoryPrimary = j[11]
            mainCategoryId = j[10]
            Region = j[7]
            timestamp = str(datetime.datetime.now())
            datestamp = str(timestamp)[0:10]
            # 插入数据
            sql = "insert dianping_cinema_shopInfo VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s'," \
                  "                                    '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', " \
                  "                                   '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s','%s', '%s', " \
                  "                                '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s'," \
                  "                                  '%s', '%s', '%s', '%s')" % \
                                   (shopId, fullName, shopName, otherName, shopUrl, City, categoryName, mainCategoryPrimary, mainCategoryName, \
                                    Region, businessDistrict, Score, Price, Score1, Score2, Score3, Address, Tel, Time, Description,\
                                    userId, shopCityId, publicTransit, cityId, cityCnName, cityName, cityEnName, isOverseasCity, \
                                    shopGlat, shopGlng, cityGlat, cityGlng, Power, shopPower, voteTotal, shopType, mainRegionId, categoryURLName, \
                                    shopGroupId,  loadUserDomain, manaScore, mainCategoryId, defaultPic, timestamp, datestamp)
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
        conn_in = MySQLdb.connect(host='221.226.72.226', user='root', passwd='somao1129', db='dianping20171225',
                                  port=13306, charset='utf8')
        cur_in = conn_in.cursor()  # 获取一个游标

        url_list = get_url()
        proxies = get_proxies()
        # time.sleep(50)
        write_cinema_shopInfo()
    except Exception:
        print traceback.format_exc()
        print '数据库连接错误'
    finally:
        if conn_in != None:
            cur_in.close()  # 关闭游标
            conn_in.close()  # 释放数据库资源

