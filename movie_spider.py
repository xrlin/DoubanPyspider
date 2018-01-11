#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2017-01-09 08:40:46
# Project: DoubanSpider

from pyspider.libs.base_handler import *
from datetime import datetime
import random
import string
import pymongo
import bson
import requests
import json
from functools import wraps

def random_agent():
    user_agent_list = [\
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 "
        "(KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
        "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 "
        "(KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 "
        "(KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 "
        "(KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 "
        "(KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 "
        "(KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",
        "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 "
        "(KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 "
        "(KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 "
        "(KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 "
        "(KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"
       ]
    return random.choice(user_agent_list)
    
def random_bid():
    #return {'bid': 'MC2U0Nf8TdZ'}
    return {'bid': ''.join(random.sample(string.ascii_letters + string.digits, 11))}

def gen_headers():
    return { 
        'User-Agent': random_agent(),
        "Host": "movie.douban.com",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, sdch, br",
        "Accept-Language": "zh-CN, zh; q=0.8, en; q=0.6",
    }

def str_to_float(num_str):
    try:
        return float(num_str)
    except:
        return None
    
def str_to_datetime(datetime_str):
    for mnt in ('%Y-%m-%d', '%Y', '%Y-%m'):
      try:
        return datetime.strptime(datetime_str.strip(), mnt)
      except ValueError:
        pass
    raise ValueError('no valid date format found')

    
def get_proxy():
    r = requests.get('http://127.0.0.1:8000/?types=0&country=国内')
    ip_ports = json.loads(r.text)
    selected_ip_port = random.choice(ip_ports)
    ip = selected_ip_port[0]
    port = selected_ip_port[1]
    return '{ip}:{port}'.format(ip=ip, port=port)

def delete_proxy(proxy):
    requests.get('http://127.0.0.1:8000/delete?ip={0}'.format(proxy.split(':')[0]))

def deal_with_error(func):
    @wraps(func)
    def wrapper(obj, resp):
        if resp.status_code == 599:
            print('Proxy invalid.')
            delete_proxy(obj.task['fetch']['proxy'])
            resp.raise_for_status()
        return func(obj, resp)
    return wrapper
    
class Handler(BaseHandler):
    crawl_config = {
        #'headers': {
        #    'User-Agent': 'baiduspider',
        #    "Cookie": "bid=%s" % "".join(random.sample(string.ascii_letters + string.digits, 11))
        #}
        #'proxy': '127.0.0.1:3128'
    }

    def crawl(self, url, **params):
        super().crawl(url, **params, proxy=get_proxy())
    
    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('https://movie.douban.com/tag/?view=cloud', callback=self.index_page, headers=gen_headers(), cookies=random_bid())

    @catch_status_code_error
    @config(age=10 * 24 * 60 * 60)
    @deal_with_error
    def index_page(self, response):
        for each in response.doc('#content > div > div.article > div.indent.tag_cloud > table > tbody > tr > td > a').items():
            self.crawl(each.attr.href, callback=self.movie_list_page, headers=gen_headers(), cookies=random_bid())

    @catch_status_code_error
    @config(priority=2)
    @deal_with_error
    def movie_list_page(self, response):
        next_page = response.doc('#content > div > div.article > div.paginator > span.next > a')
        for movie_link in response.doc('#content div.pl2 > a').items():
            self.crawl(movie_link.attr.href, callback=self.movie_detail_page, headers=gen_headers(), cookies=random_bid())
        if next_page and next_page.attr.href:
            self.crawl(next_page.attr.href, callback=self.movie_list_page,  headers=gen_headers(),cookies=random_bid())
            
    @catch_status_code_error    
    @config(priority=10)
    @deal_with_error
    def movie_detail_page(self, response):
        self.get_reviews(response.url+'reviews')
        return {
            'name': response.doc('#content > h1 > span:nth-child(1)').text(),
            'rate': str_to_float(response.doc('#interest_sectl > div.rating_wrap.clearbox > div.rating_self.clearfix > strong').text()),
            'screenwriter': [ name.strip() for name in response.doc('#info > span:nth-child(3) > span.attrs').text().split('/')],
            'directors': [ a.text() for a in response.doc('#info > span:nth-child(1) > span.attrs > a').items()],
            'performers': [ a.text() for a in response.doc('#info > span.actor > span.attrs > a').items() ],
            'genre':[a.text() for a in response.doc('#info > span[property="v:genre"]').items()],
            'state': [country.strip() for country in response.etree.xpath('//*[@id="info"]/span[text()="制片国家/地区:"]')[0].tail.split('/')],
            'language': response.etree.xpath('//*[@id="info"]/span[text()="语言:"]')[0].tail,
            'release_date': self.trim_suffix(response.etree.xpath('//*[@id="info"]/span[@property="v:initialReleaseDate"]/text()')[0]),
            'film_length': self.trim_suffix(response.doc('#info > span[property="v:runtime"]').text()),
        }
    
    @catch_status_code_error
    @config(priority=4)
    def get_reviews(self, url):
        return self.crawl(url, callback=self.review_list, headers=gen_headers(), cookies=random_bid())
     
    @catch_status_code_error
    @config(priority=5)
    @deal_with_error
    def review_list(self, response):
        for a in response.doc('div.review-item > header > h3 > a').items():
            self.crawl(a.attr.href, callback=self.review_detail, headers=gen_headers(), cookies=random_bid())
            
        next_page = response.doc('span.next > a')
        if next_page and next_page.attr.href:
            self.crawl(next_page.attr.href, callback=self.review_list, headers=gen_headers(), cookies=random_bid())
     
    @catch_status_code_error
    @config(priority=6)
    @deal_with_error
    def review_detail(self, response):
        return {
           'refer': response.doc('header.main-hd >a:nth-child(2)').attr.href,
           'title': response.doc('#content h1 > span[property="v:summary"]').text(),
           'content': response.doc('div[property="v:description"]').text(),
            'is_review': True
        }
    
    def trim_suffix(self, str):
        # 去除‘(中国大陆)之类的后缀
        return str.split('(')[0]
    
    def on_result(self, result):
        super(Handler, self).on_result(result)
        assert self.task['taskid']
        assert self.task['project']
        assert self.task['url']
        task = self.task
        if not result:
            return
        client = pymongo.MongoClient('10.0.75.1')
        db = client.douban
        movies = db.movies
        if result.get('is_review'):
            if movies.find_one({'url': result.get('refer')}):
                movie = movies.find_one({'url': result.get('refer')})
                result['url'] = task['url']
                movies.update({'url': result.get('refer')}, {'$addToSet': {'reviews': result}}, True)
            else:
                movie = {}
                movie['url'] = result.get('refer')
                result['url'] = task['url']
                movie['reviews'] = [result]
                movies.insert_one(movie)
        else:
            result['release_date'] = str_to_datetime(result['release_date'])
            movie = movies.find_one({'url': task['url']})
            if movie:
              movies.update({'url': task['url']}, {'$set': result})
            else:
              movies.insert_one(result)
