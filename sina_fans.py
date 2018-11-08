import requests, re, json, time, logging, pickle
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine


def get_data_from_sql(sql_path=u'/Users/wisdombeat/PycharmProjects/weiboBA.db副本', sql_command='SELECT * FROM WeiboQA'):
    """
    This def can get data from a sqlalchemy and when load success return the generator, else return a string.
    :param sql_path: The path of sqlalchemy.
    :param sql_command: The sql order made by your need.
    :return: a generator contains the data selected from the sqlalchemy.
    Raises: IOError: An error occurred connecting the sql and return a string when occurred.
    """
    try:
        engine = create_engine(sql_path)
        Sina_data = pd.read_sql(sql_command, engine, chunksize=2)
        return Sina_data
    except IOError:
        return 'Fail to connect sql'


def download(url, headers=None, data=None, num_retries=2):
    time.sleep(0.8)
    try:
        response = requests.get(url=url, data=data, headers=headers, timeout=20)
        response.status_code
        html = response.content.decode('utf-8')
        ajax = json.loads(html)
        author_data = ajax['data']
    except requests.RequestException as e:
        print("Cannot get page beacause:", e)
        print("Error page is:", url)
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


def save_data(sina_data, name):
    output = open(name, 'wb')
    pickle.dump(sina_data, output)
    output.close()


def data_crawler(header=None, data=None):
    sql = 'Select uid from PageInfo'
    sql_path = 'sqlite:///weiboqa.db'
    uid_list = []
    sina_fans = pd.DataFrame(columns=['author', 'uid', 'followers_count'])
    wrong_uid = []
    uid_data = pd.DataFrame(columns=['uid'])
    for uid in get_data_from_sql(sql_path, sql_command=sql):
        uid_data = pd.concat([uid_data, uid])
    for uid in uid_data['uid']:
        uid = float('%.9e'%uid)
        uid = int(uid)
        if uid not in uid_list:
            uid_list.append(uid)
    for uid in uid_list:
        url = 'https://m.weibo.cn/api/container/getIndex?containerid=100505'+str(uid)
        header['Referer'] = url
        try:
            uid_data = download(url=url, headers=header, data=data)
            if uid_data:
                userInfo = uid_data['userInfo']
                if userInfo:
                    author_name = str(userInfo['screen_name'])
                    followers_count = int(userInfo['followers_count'])
                    sina_fan = {}
                    sina_fan['author'] = author_name
                    sina_fan['followers_count'] = followers_count
                    sina_fan['uid'] = str(uid)
                    sina_fan = pd.DataFrame(sina_fan, index=[0])
                    sina_fans = pd.concat([sina_fans, sina_fan])
                    sina_fans = sina_fans.reset_index(drop=True)
                else:
                    wrong_uid.append(uid)
            else:
                wrong_uid.append(uid)
        except Exception as e:
            print('cannot download because:', e)
            wrong_uid.append(uid)
        if len(sina_fans) % 100 == 0:
            print('Have crawled %d'%(len(sina_fans)), '\n')
            print('Have run %s s'%str(time.time()), '\n')
            print('There are %d uid cannot crawl'%len(wrong_uid), '\n')
    save_data(sina_fans, '../wiki_txt/sina_fans.pkl')
    save_data(wrong_uid, '../wiki_txt/wrong_uid.pkl')


logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level = logging.INFO)
header = {
    'Accept':'application/json, text/plain, */*',
    'Accept-Encoding':'gzip, deflate, br',
    'Accept-Language':'zh-CN,zh;q=0.9',
    'Connection':'keep-alive',
    'Cookie':'_T_WM=56737a35b64d0790a5f1c0b2300e1104; SUB=_2A253Y0teDeRhGeVJ41YQ9S_MzjmIHXVUrFUWrDV6PUJbkdANLU3fkW1NT8Wm8GNMKvvdXwWTwD0_eEuENcVwxLN4; SUHB=0nluVH8pwpc9bz; SCF=Aj7H3RW-uRHQpw_uGg2Z-0wPUJKI27aQnIsh3vOKtKArWTs4BSFVTPcCuSVJTQG-B3MA_ElgQpbQRGPr5jLi6_s.; SSOLoginState=1516714766; WEIBOCN_FROM=1110006030; M_WEIBOCN_PARAMS=luicode%3D10000011%26lfid%3D1076031618051664%26fid%3D1005051618051664%26uicode%3D10000011',
    'Host':'m.weibo.cn',
    'Referer':'https://m.weibo.cn/u/1618051664',
    'User-Agent':'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0_1 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A402 Safari/604.1',
    'X-Requested-With':'XMLHttpRequest'
}
data = {
    'loginName': '15708426257',
    'loginPassword': 'a12345123'
}
data_crawler(header, data)

# url = 'https://weibo.com/ttwenda/p/show?id=2310684194076890135052'
# header = {
#     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
#     'Accept-Encoding': 'gzip, deflate, br',
#     'Accept-Language': 'zh-CN,zh;q=0.9',
#     'Cache-Control': 'max-age=0',
#     'Connection': 'keep-alive',
#     'Cookie': '_s_tentry=-; Apache=2119928308804.2043.1517581575209; SINAGLOBAL=2119928308804.2043.1517581575209; ULV=1517581575215:1:1:1:2119928308804.2043.1517581575209:; '
#               'YF-Page-G0=d30fd7265234f674761ebc75febc3a9f; SSOLoginState=1525863505; YF-V5-G0=3717816620d23c89a2402129ebf80935; '
#               'YF-Ugrow-G0=5b31332af1361e117ff29bb32e4d8439; wvr=6; UOR=,,www.baidu.com; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WFVwy_5W1_NYGmdgVwWBcfN5JpX5KMhUgL.FoeN1hBpSK27SK-2dJLoI7DNIPS.dcfb9g8X; '
#               'ALF=1557722417; SCF=AnfQ_cdCT6xKGEJ8GWp9Gskyqb31cINlOQCxThJU_RWBpcQNOv8agkZlVVxS1b3sNtc0aS0t2fZj-RboW699Hkc.; '
#               'SUB=_2A25387HiDeRhGeVJ41YQ9S_MzjmIHXVUiKQqrDV8PUNbmtBeLUjykW9NT8Wm8Dq02-CvHwOMDPa15sCA8MmYnhqU; SUHB=0S7Cr9CqF0c-n8',
#     'Host': 'weibo.com',
#     'Referer': 'https://weibo.com/ttwenda/p/show?id=2310684194076890135052',
#     'Upgrade-Insecure-Requests': '1',
#     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.9 Safari/537.36'
# }
# log_data = {
#     'loginName': '15708426257',
#     'loginPassword': 'a12345123'
# }
#
# response = requests.get(url=url, data=log_data, headers=header, timeout=20)
# response.status_code
# html = response.content.decode('utf-8')
# print(html)