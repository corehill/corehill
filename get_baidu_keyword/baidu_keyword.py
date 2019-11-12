import multiprocessing
import threading
import time
from multiprocessing import Pool
import pymysql
import requests
from pyquery import PyQuery as pq
import re
import redis



re_data = redis.Redis(host='localhost', port=6379)
db = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='123456', db='baidu')
cursor = db.cursor()


class baidu_keyword(object):
    def __init__(self):
        self.baidu=1
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
        }

    def parse_url(self, url):
        try:

            response = requests.get(url=url, headers=self.headers)

            return response.text
        except Exception as e:
            print(e)


    def handle_page(self,text):
        url_pool = []
        doc = pq(text)
        word_origin = doc('#rs')

        # print(word_origin)
        pattern = re.compile('<a href="(.*?)">(.*?)</a>', re.S)
        results = re.findall(pattern, str(word_origin))
        # print(results)
        time.sleep(2)
        for result in results:
            word = result[1]
            print(word)

            query = """insert into all_keyword_2 (word,baidu) values (%s,%s) on duplicate key update word=word"""
            query_2 = """update all_keyword_2 set baidu = "1" where word= %s"""
            try:
                cursor.execute(query, (word,self.baidu))
                cursor.execute(query_2, (word))
                db.ping(reconnect=True)
                db.commit()

                print(word, '插入数据库')
                print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
            except Exception as e:
                print(e)
                db.rollback()
            # print(result[0])
            urls = 'http://www.baidu.com' + result[0]
            url_pool.append(urls)
            # print(url_pool)
        return url_pool


    def parse_content(self, url_pool):
        for i in range(len(url_pool)):
            re_data.lpush('baidu_url_1', url_pool[i])
            # print(4)


    def parse_url_content(self):
        threads = []
        for i in range(10):
            url = re_data.rpop('baidu_url_1')

            # time.sleep(3)
            t = threading.Thread(self.main(url))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()




    def main(self, url):
        text = self.parse_url(url)
        # print(text)
        # t2 = time.time()
        # print(t2)
        # if t2-t1>200:
        #     b.main(url)
        if text:
            url_pool = self.handle_page(text)
            self.parse_content(url_pool)

            self.parse_url_content()


def main(start_page,end_page):
    base_url = re_data.rpop('baidu_url_1')
    # base_url = 'http://www.baidu.com/s?wd=凤凰马经天下彩首页'
    b = baidu_keyword()
    try:
        # pool.map(b.main(base_url), [i * 10 for i in range(10)])
        b.main(base_url)
        # t1 = time.time()
        # print(t1)
    except:
        b.main(base_url)


if __name__ == '__main__':

    # pool = Pool()


    # for i in range(10):  # 启动10个多进程
    #     p = multiprocessing.Process()
    #     p.start()
    while 1:
        proc_record = []
        for i in range(10):
            p = multiprocessing.Process(target = main, args = (1, 2))
            p.start()
            proc_record.append(p)
        for p in proc_record:
            p.join()

    # base_url =  'http://www.baidu.com/s?wd=买马图纸'
    # b = baidu_keyword()
    # try:
    #     # pool.map(b.main(base_url), [i * 10 for i in range(10)])
    #     b.main(base_url)
    #     # t1 = time.time()
    #     # print(t1)
    # except:
    #     b.main(base_url)
    # p.join()