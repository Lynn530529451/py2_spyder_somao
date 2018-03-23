# -*- coding=utf-8 -*-

from multiprocessing import Process, Queue
from lxml import etree
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

class BossSpider(Process):
    def __init__(self, url, q):
        # 重写父类的__init__方法
        super(BossSpider, self).__init__()
        self.url = url
        self.q = q
        # self.headers = {
        #     'Host': 'www.zhipin.com',
        #     'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36'
        #   }

    def get_url(self):
        conn = None
        conn_in = None
        try:
            conn = MySQLdb.connect(host='****', user='root', passwd='***', db='bosszhipin', port=20007,
                                   charset='utf8')
            cur = conn.cursor()  # 获取一个游标

            conn_in = MySQLdb.connect(host='***', user='root', passwd='***', db='bosszhipin',
                                      port=13306, charset='utf8')
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

    def get_proxies(self):
        proxies = list(set(requests.get(
            "http://60.205.92.109/api.do?name=86020600B1D5E92725E68858AEBCF346&status=1&type=static").text.split('\n')))
        return proxies

    def get_one_parse(self,url):
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
                    if str(e).find('10061') >= 0:  ## '10060'
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

    def parse_page(self):
        '''
        解析网站源码，并采用ｘｐａｔｈ提取　电影名称和平分放到队列中
        :return:
        '''
        response = self.send_request(self.url)
        html = etree.HTML(response)
        # 　获取到一页的电影数据
        node_list = html.xpath("//div[@class='info']")
        for move in node_list:
            # 电影名称
            title = move.xpath('.//a/span/text()')[0]
            # 评分
            score = move.xpath('.//div[@class="bd"]//span[@class="rating_num"]/text()')[0]

            # 将每一部电影的名称跟评分加入到队列
            self.q.put(score + "\t" + title)


def main():
    # 创建一个队列用来保存进程获取到的数据
    q = Queue()
    base_url = 'https://movie.douban.com/top250?start='
    # 构造所有ｕｒｌ
    url_list = [base_url + str(num) for num in range(0, 225 + 1, 25)]

    # 保存进程
    Process_list = []
    # 创建并启动进程
    for url in url_list:
        p = BossSpider(url, q)
        p.start()
        Process_list.append(p)

    # 让主进程等待子进程执行完成
    for i in Process_list:
        i.join()

    while not q.empty():
        print q.get()


if __name__ == "__main__":
    start = time.time()
    main()
    print '[info]耗时：%s' % (time.time() - start)
