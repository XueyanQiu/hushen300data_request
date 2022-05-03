import os
import time

import requests
import json
import pandas as pd


# 抓取北向资金，目前只能抓到2014年11月之后的

# 不知道为什么这个接口可能请求失败返回的空的，所以递归一下吧
def grab_history_data():
    header = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh - CN, zh;q = 0.9, en;q = 0.8, it;q = 0.7, ja; q = 0.6',
        'hexin-v': 'AzMKftoKVaq7dBl-GVtT5UbXwjxYaMcqgfwLXuXQj9KJ5F0ibThXepHMm6r2',
        'Proxy-Connection': 'keep - alive',
        'Host': 'data.10jqka.com.cn',
        'Referer': 'http://data.10jqka.com.cn/hsgt/index',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/100.0.4896.127 Safari/537.36 '
    }
    response = requests.get('http://data.10jqka.com.cn/hsgt/history/type/north/date/day', headers=header)
    if 'ok' in response.text:
        json_obj: dict = json.loads(response.text)
        return json_obj
    else:
        print('抓取失败，等待3秒')
        time.sleep(3)
        grab_history_data()


def write_csv(obj: dict):
    date_list: list = obj['data']['zhuri']['date']
    for i in range(len(date_list)):
        date_list[i] = str(date_list[i]).replace('-', '')
    total_list: list = obj['data']['zhuri']['total']
    csv_data = [list(i) for i in zip(date_list, total_list)]
    df = pd.DataFrame(data=csv_data)
    df.to_csv('northdata.csv', index=False, header=['交易日期', '北向资金'])


# 数据更新：先抓接口，全部数据追加进去northdata.csv，然后再读取，去重，排序
def fill_up_rest_data():
    new_obj = grab_history_data()

    date_list: list = new_obj['data']['zhuri']['date']
    for i in range(len(date_list)):
        date_list[i] = str(date_list[i]).replace('-', '')
    total_list: list = new_obj['data']['zhuri']['total']
    csv_data = [list(i) for i in zip(date_list, total_list)]
    df = pd.DataFrame(data=csv_data)
    df.to_csv('northdata.csv', index=False, header=False, mode='a', sep=',')

    df2 = pd.read_csv('northdata.csv')
    data = df2.sort_values(by='交易日期', ascending=True)
    data.drop_duplicates(inplace=True)
    data.to_csv('northdata.csv', index=False)


def upload_north_data():
    if not os.path.exists('northdata.csv'):
        write_csv(grab_history_data())
    fill_up_rest_data()

