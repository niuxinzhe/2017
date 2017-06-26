# -*- coding: utf-8 -*-
"""
Created on Mon May 29 14:00:29 2017

@author: solaris
"""

import requests, re, json, time, sqlite3, logging  
import numpy
from datetime import datetime


import pandas as pd
from sqlalchemy import create_engine




def download(url, headers=None, data=None, num_retries=2):
    """
    获取sina网页源代码
    """
    time.sleep(0.8)
    try:
        response = requests.get(url=url, data=data, headers=headers,timeout=20)
        response.status_code
        html = response.content.decode('utf-8')
        ajax = json.loads(html)
        author_data = ajax['data']
    except requests.RequestException as e:
        print("网页抓取异常：", e,url)
        author_data = {}
        if hasattr(e, 'code'):
            code = e.code
            if num_retries > 0 and 500 <= code < 600:
                # retry 5XX HTTP errors
                return download(url, headers, num_retries-1, data)
        else:
            code = None
    except requests.exceptions.ConnectTimeout:
        NETWORK_STATUS = False
    except requests.exceptions.Timeout:
        REQUEST_TIMEOUT = True
    return author_data

def save_data(DateFrame, name):
        #连接并保存到数据库
        engine = create_engine('sqlite:///weiboBA.db')
        df = pd.DataFrame(DateFrame)
        df.to_sql(name, engine, index = True, if_exists='append')
        #如果不存在一个weiboQA表，新建一个weiboQA表
        #conn.execute("CREATE TABLE IF NOT EXISTS weiboQA(author varchar(80), label varchar(80),price INTEGER,total_count integer,question TEXT,onlooker_count integer, time text);")
        #conn.execute("INSERT INTO weiboQA(author, label, price, total_count, question, onlooker_count, time)values(?,?,?,?,?,?,?)",(author, label, price, total_count, question, onlooker_count, time))
        #conn.close()
      
def data_crawler(url, page, headers=None, data=None ):
    """
    抓取数据
    """

    engine = create_engine('sqlite:///weiboBA.db')
    page_info = {'pagesize': [],
                 'uid': []
                 }

    try:
        save_data(page_info, 'PageInfo')
    except Exception as e:
        print("记录插入异常:", e)
    link_data = download(url,headers, data)

    if link_data:

        list=link_data['list']
        pattern = re.compile(r'\d+')
        for i in list:
            if i['nickname']:
                #print("have data" + str(time.time())+i['nickname'])
                match = pattern.search(i['ask_url'])
                if match:
                    #获取每个答主主页页数
                    url_author="http://e.weibo.com/v1/public/h5/aj/qa/getauthor?uid="+match.group()
                    #print(url_author)
                    get_data = download(url_author,headers, data)
                    if get_data:
                        pager_info = get_data['pager_info']
                        total_page = pager_info['total_page']
                        #print(total_page)
                        n = 1
                        while n<= total_page:
                            if len(pd.read_sql("SELECT * FROM PageInfo WHERE pagesize="+str(n)+" AND uid="+match.group(), engine))==0:
                                pagesize = n
                                uid = match.group()
                                page_info = {'pagesize':[pagesize],
                                            'uid':[uid]
                                            }
                                try:
                                    save_data(page_info, 'PageInfo')
                                except Exception as e:
                                        print("记录插入异常:", e)
                                url_author_everypage = "http://e.weibo.com/v1/public/h5/aj/qa/getauthor?uid="+match.group()+"&page="+str(n)
                                get_data_everypage = download(url_author_everypage,headers, data)
                                if get_data_everypage:
                                    total_count = get_data_everypage['total_count']
                                    #print(total_count)
                                    author_info = get_data_everypage['author_info']
                                    #print(author_info)
                                    label = author_info['label']
                                    nickname = author_info['nickname']
                                    #print(nickname)
                                    price = author_info['price']
                                    if get_data_everypage['list']:
                                        list_everypage = get_data_everypage['list']
                                        for m in list_everypage:
                                            try:
                                                #print(m['intro'])
                                                #print(m['onlooker_count'])
                                                #print(m['time'])
                                                sinaQA = {'author':[nickname],
                                                'label':[label],
                                                'price':[price],
                                                'total_count':[total_count],
                                                'question':[m['intro']],
                                                'onlooker_count':[m['onlooker_count']],
                                                'time':[m['time']]
                                                }

                                                save_data(sinaQA, 'WeiboQA')

                                                #data_save = save_data(author=nickname, label=label, price=price, total_count=total_count, question=m['intro'], onlooker_count=m['onlooker_count'], time=m['time'])
                                                #print(data_save)
                                            except Exception as e:
                                                print("数据插入异常:", e)
                                    else:
                                        try:
                                            sinaQA = {'author': [nickname],
                                                      'label': [label],
                                                      'price': [price],
                                                      'total_count': [total_count],
                                                      'question': ['None'],
                                                      'onlooker_count': [0],
                                                      'time': ['None']
                                                      }
                                            save_data(sinaQA, 'WeiboQA')
                                        except Exception as e:
                                            print("数据插入异常:", e)
                            if page % 100 == 0:
                                #print(url_author_everypage)
                                #print(nickname)
                                print(page)
                                print(time.time())

                            n+=1

                            #if page % 2000 == 0:
                                #print('[ATTEMPTING] rest for 30 minutes to cheat weibo site, avoid being banned.')
                                #time.sleep(60)
                            page+=1
            else:
                print("no data"+str(time.time()))
                page += 1
    return page


if __name__ == '__main__':
    page_count = 3040
    page = 1
    while page_count <= 3128:
        
        url = "http://e.weibo.com/v1/public/h5/aj/qa/getfamousanswer?fieldtype=all&page="+str(page_count)+"&pagesize=10"
        data = { 'loginName':'15708426257', 'loginPassword':'a12345123'}
        Header = {
        'Accept':'application/json, text/plain, */*',
        'Accept-Encoding':'gzip, deflate, sdch',
        'Accept-Language':'en,zh-CN;q=0.8,zh;q=0.6,zh-TW;q=0.4,en-US;q=0.2',
        'Connection':'keep-alive',
        'Cookie':'SINAGLOBAL=4721941486693.218.1472199777412; UM_distinctid=15ac7ca1eaa1d5-0a27fcde8562ff-58133b15-100200-15ac7ca1eab2fa; SCF=AsCsluReS1sSsw0T5dNZh2XUgUwpDcIpDtKat1yzv2TYg5FHmIkxcQnRCj1kRNcTef34Mbj4COmVOT4vc_LE4tY.; SUHB=0id63ZXVt3Hl9m; UOR=,,login.sina.com.cn; ALF=1498386654; SUB=_2A250LHOODeRhGeVJ41YQ9S_MzjmIHXVX7x3GrDV8PUJbkNBeLXbFkW1UUuVdzApJFugwQQHq50_3-ETeKQ..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WFVwy_5W1_NYGmdgVwWBcfN5JpX5oz75NHD95Q0S0nXeK-peh-fWs4DqcjZUcL09sv_qJHyU5tt; _s_tentry=-; Apache=2753338218399.992.1495857654968; ULV=1495857655603:115:3:3:2753338218399.992.1495857654968:1495793294612',
        'Host':'e.weibo.com',
        'RA-Sid':'s_2191_r2x9ak474125_9',
        'RA-Ver':'3.1.4',
        'Referer':'http://e.weibo.com/v1/public/center/famousanswer?val=all',
        'User-Agent':'Mozilla/5.0 (iPhone; CPU iPhone OS 8_3 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) FxiOS/40.0 Mobile/12F69 Safari/600.1.4'
        }
        page = data_crawler(url, page, headers=Header, data=data)
        print(page_count)

        page_count +=1

