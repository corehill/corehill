import pymysql
import requests
from pyquery import PyQuery as pq
import re
import json
import threading
from multiprocessing import Pool, Process
import time
from requests.exceptions import ProxyError
from urllib3.exceptions import MaxRetryError
import pandas as pd


class forbid(object):
    def __init__(self):
        self.base_url = 'http://m.baidu.com/s?wd={}'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36',
        }

        self.point_words = ['qq.com', 'cctv.com', '新闻', 'sohu.com', 'taobao.com', 'sina.com', 'ifeng.com', '163.com',
                            'people', 'cntv', 'ccp', '新华',
                            'gov.cn', 'sogou', 'china', 'zhihu', 'baidu', 'cjn.cn', 'xinhuanet.com', 'cnr.cn',
                            'southcn.com',
                            'chinanews.com', 'new', 'xinhua']

        self.word_contain = ['随时现金', '斗牛财经', '都市赛车', '疯狂赛车', '山脊赛车', '现金超人', '我要出彩', '迅雷下载', '富途牛牛', '信用牛牛', '牛牛信用',
                             '金沙秀',
                             '七彩影', '牛牛影', '中午版', '音牛牛', '包牛牛', '宋小宝', '七彩云', '龙虎门', '风信子', '邀请码', '黑平台', '修改器', '万金花',
                             '小金花', '可靠不', '发牌机', '洗牌机', '不可能', '可不可', '微信群', '三公像', '搭建', '拼多多', '小赛车', '七彩云', '多彩宝',
                             '王中王', '图下载', '棋牌室', '真人秀', '提不出', '看斗牛', '皇冠和', '不出款', '信用卡', '彩贝壳', '皇冠三', '黑不黑', '电竞馆',
                             '刷流水', '不出来', '靠谱不', '正规么', '现金侠', '规范么', '了没有', '谱么', '靠么', '实么', '是否', '外卦', '外挂', '漏洞',
                             '自己', '陷阱', '代码', '对刷', '刷反水', '刷佣金', '论坛', '套利', '租赁', '破解', '真的', '欢迎', '推广', '挂机', '中特',
                             '制作', '假的', '外挂', '原理', '策略', '删除', '手游', '社保', '玩法', '活动', '图纸', '外汇', '查询', '规律', '图库',
                             '市值',
                             '出租', '视频', '兼职', '排名', '好不', '资料', '无法', '彩视', '输了', '快递', '作弊', '出款难', '失败', '多高', '设置',
                             '新闻', '不上', '改', '机器', '高铁', '火车', '回放', '开发', '源码', '投诉', '回血', '建议', '口诀', '分享', '帐户',
                             '评测',
                             '连线', '助手', '步骤', '激活', '试题', '彩笔', '报纸', '酒店', '马会', '特码', '六合', '道人', '报马', '报码', '输死',
                             '输了',
                             '控制', '素材', '彩色', '出售', '总结', '真假', '真伪', '微博', '卡通', '背景', '设备', '盗号', '赌徒', '理论', '作业',
                             '央视',
                             '节目', '干扰', '干嘛', '不了', '内测', '资讯', '不给', '技巧', '旅游', '设计', '程序', '揭秘', '建设', '交通', '限制',
                             '音乐',
                             '配音', '发音', '交通', '招人', '招聘', '二手', '旅行', '相机', '香港', '不能', '证券', '粉丝', '商场', '金融', '头像',
                             '故事',
                             '心得', '交流', '不去', '传销', '是不', '搭建', '公交', '公众', '巴士', '电召', '银行', '批发', '维护', '学生', '搭建',
                             '自建',
                             '儿童', '唯彩', '消防', '交友', '选择', '支付', '制度', '天气', '小品', '刘能', '电视', '编程', '订制', '驱动', '定制',
                             '彩虹',
                             '睛彩', '幽默', '导游', '农行', '缴费', '壁纸', '打击', '涂料', '黑钱', '下架', '打印', '铃声', '小说', '嘛', '套路',
                             '跑路',
                             '违规', '违法', '说明', '黑客', '评论', '铸源', '种子', '青年', '小学', '中学', '高中', '学院', '实名', '监管', '点评',
                             '评价',
                             '购物', '流程', '跑了', '脚本', '算命', '军旗', '地产', '研发', '用品', '详解', '商城', '概率', '趣阁', '背后', '占卜',
                             '诀窍',
                             '签证', '举报', '外卖', '借款', '照片', '员工', '厂家', '文章', '生产', '题目', '转让', '漫画', '动漫', '基金', '考试',
                             '驾驶',
                             '心得', '攻击', '交所', '围棋', '象棋', '交易', '趣味', '爆笑', '介绍', '出售', '绝招', '搜索', '博会', '球衣', '女王的',
                             '影院', '影视', '影音', '观看', '续集', '赛程', '架设', '影城', '影视', '制作', '手表', '日报', '广告', '没了', '丰田',
                             '手册',
                             '倾家', '报价', '置业', '上架', '批发', '控制', '期权', '关门', '音频', '航空', '汽车', '气象', '派对', '恋爱', '扮演',
                             '原则',
                             '生日', '关闭', '方法', '大学', '茶馆', '配置', '日报', '晚报', '录像', '约球', '电商', '护理', '0月', '1月', '2月',
                             '3月',
                             '4月', '5月', '6月', '7月', '8月', '9月', '融365', 'u现金', '唱', '扫', '骗', '坑', '帖', '害', '刷', '假',
                             '图',
                             '租', '词', '货', '难', '几', '校', '借', '贷', '药', '教', '训', '剧', '练', '语', '税', '课', '素', '影',
                             '媒',
                             '判', '书', '殖', '物', '寄', '停', '饰', '幼', '递', '举', '曝', '禁', '闭', '侵', '说', '监', '阅', '恶',
                             '搞',
                             '稿', '谜', '诗', '忌', '案', '幼', '酒', '贫', '感', '病', '鞋', '歌', '擎', '4399', '7k7k', 'office',
                             'ppt', 'pdf', 'vpn', '..', '?', 'txt', '_', '<', '-', '( ', '(', '【', '[', '/', '\\', ',',
                             '，',
                             '。']

    def get_words_mysql(self, num):
        db = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='123456', db='baidu')
        # cursor = db.cursor()
        sql = f'select keyword, id from ac_keyword_add_copy limit {num * 10},10'
        content = pd.read_sql(sql, db).to_dict()
        return content

    def index_words(self, url):
        try:
            response = requests.get(url=url, headers=self.headers)
            if response.status_code == 200:
                return response.text
            else:
                print(response.status_code)
                return self.index_words(url)
        except:
            return self.index_words(url)

    def extend_word(self, word):
        if len(word) >= 6:
            word = word[0:-2]
            self.word_add(word)
            self.parse_main(word)
            return self.extend_word(word)
        else:
            self.word_add(word)
            self.parse_main(word)

    def parse_main(self, word):
        values = []
        db = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='123456', db='baidu')
        cursor = db.cursor()
        url = self.base_url.format(word)
        html = self.index_words(url)
        if html:
            doc = pq(html)
            word_origin_2 = doc('#relativewords > div.rw-list-container.rw-list-container2')
            # pattern = re.compile('<a href="(.*?)">(.*?)</a>', re.S)
            pattern = re.compile('<span(.*?)>(.*?)</span>', re.S)
            results = re.findall(pattern, str(word_origin_2))
            for word in results:
                if [s for s in self.word_contain if s not in word[1]]:
                    value = tuple([str(word[1])])
                    # print(value)
                    values.append(value)
            i = '%s'
            j = ',%s'
            for x in range(1, len(values)):
                i += j
            print(i)
            print(len(values))
            query = f"""insert into ac_keyword_add_copy (keyword) values {i}"""
            try:
                cursor.execute(query, values)
                db.ping(reconnect=True)
                db.commit()
                print(values, '插入数据库2')
            except Exception as e:
                print(e)
                db.rollback()

    def word_add(self, word):
        values = []
        i = '%s'
        j = ',%s'
        db = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='123456', db='baidu')
        cursor = db.cursor()
        url = f'http://m.baidu.com/sugrec?pre=1&p=3&ie=utf-8&json=1&prod=wise&from=wise_web&wd={word}'
        info_json = requests.get(url).json()
        if 'g' in info_json:
            add_words = info_json['g']
            for word in add_words:
                key = word['q']
                if [s for s in self.word_contain if s not in key]:
                    # print(key)
                    value = tuple([str(key)])
                    values.append(value)
            for x in range(1, len(values)):
                i += j

            query = f"""insert into ac_keyword_add_copy (keyword) values {i}"""
            try:
                cursor.execute(query, values)
                db.ping(reconnect=True)
                db.commit()
                print(values, '插入数据库1')
            except Exception as e:
                print(e)
                db.rollback()

    def handel_word(self, key_word):
        self.extend_word(key_word)
        self.parse_main(key_word)
        # self.word_add(key_word)

    def run(self, num):
        pool = []
        content = self.get_words_mysql(num)
        # print(content)
        for i in range(10):
            key_word = content['keyword'][i]
            # id = content['id'][i]
            #     t = threading.Thread(target=self.handel_word, args=(key_word, id))
            #     t.start()
            #     pool.append(t)
            # for t in pool:
            #     t.join()

            p = Process(target=self.handel_word, args=(key_word,))
            p.start()
            pool.append(p)
        for p in pool:
            p.join()

    def main(self, num):
        self.run(num)
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))


if __name__ == '__main__':
    f = forbid()
    # f.main(1)
    num = 19949
    while 1:
        try:
            # global num
            time.sleep(1)
            f.main(num)
            num += 1
            # with open('num.txt', 'w') as f:
            #     f.write(str(num))
            print('第%d轮 ：' % num)
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        except AttributeError:
            pass
