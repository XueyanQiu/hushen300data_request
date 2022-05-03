import json
import os.path
import time

import pandas as pd
import requests


# 抓取沪深300数据，包括交易日期,时间戳,最高价,最低价,开盘价,收盘价,涨跌额

# 给一个特定的时间戳，抓取在此之前90个交易日（包括时间戳当日）的数据
def grab_history_data(endtime: int, count: int = 90):
    header = {
        'Accept': 'application / vnd.finance - web.v1 + json',
        'Accept - Encoding': 'gzip, deflate, br',
        'Accept - Language': 'zh - CN, zh;q = 0.9, en;q = 0.8, it;q = 0.7, ja; q = 0.6',
        'Connection': 'keep - alive',
        'Host': 'finance.pae.baidu.com',
        'Origin': 'https://gushitong.baidu.com',
        'Referer': 'https://gushitong.baidu.com/',
        'sec-ch-ua': "\"Not A;Brand\";v=\"99\",\"Chromium\";v=\"100\",\"Google Chrome\";v=\"100\"",
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same - site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/100.0.4896.127 Safari/537.36 '
    }
    params = {
        'code': 399300,
        'all': 0,
        'count': count,
        'end_time': endtime,
        'ktype': 1,
        'isIndex': True,
        'isBk': False,
        'isBlock': False,
        'stockType': 'ab',
        'market_type': 'ab',
        'group': 'quotation_index_kline'
    }
    response = requests.get('https://finance.pae.baidu.com/selfselect/getstockquotation?', params=params,
                            headers=header)
    json_obj: dict = json.loads(response.text)
    result_list: list = json_obj['Result']
    return result_list


# 排序并去重
def sort_clean_repeat_csv():
    df = pd.read_csv('kline.csv')
    data = df.sort_values(by='时间戳', ascending=True)
    data.drop_duplicates(inplace=True)
    data.to_csv('kline.csv', index=False)


# grab_history_data方法返回的数据，整理成一个只有需要的字段的list
def catch_special_column(data: list):
    res_list: list = []
    for item in data:
        res_list.append(
            {
                '交易日期': item['date'],
                '时间戳': item['time'],
                '最高价': item['kline']['high'],
                '最低价': item['kline']['low'],
                '开盘价': item['kline']['open'],
                '收盘价': item['kline']['close'],
                '涨跌额': item['kline']['increase']
            }
        )
    return res_list


# 初次运行，获取全部历史数据，并排序
def get_all_history_data():
    first_loop = True
    do_while = True
    start_time = int(time.time())
    while do_while:
        res_list = grab_history_data(start_time)
        res_list_2 = catch_special_column(res_list)
        # 2005-01-04为抓取的截止时间
        if int(res_list[0]['time']) <= 1104768000 or len(res_list) < 90:
            do_while = False
        if first_loop:
            first_loop = False
            pd.json_normalize(res_list_2).to_csv('kline.csv', index=False, mode='a', sep=',', header=True)
        else:
            pd.json_normalize(res_list_2).to_csv('kline.csv', index=False, mode='a', sep=',', header=False)
        start_time = int(res_list[0]['time']) - 86400
    sort_clean_repeat_csv()


# 以当前时间戳为endtime，抓取（endtime-表中最后一个时间戳）的天数的数据，其中可能有非交易日，因此进行一次去重
def fill_up_rest_data():
    with open('kline.csv', mode="r", encoding='utf-8') as f_csv:
        data = [[x.strip() for x in line.strip().split(',')] for line in f_csv.readlines()][-1][1]
    f_csv.close()
    rest_list = grab_history_data(int(time.time()), int((time.time() - float(data)) / 86400))
    res_list_2 = catch_special_column(rest_list)
    pd.json_normalize(res_list_2).to_csv('kline.csv', index=False, mode='a', sep=',', header=False)
    sort_clean_repeat_csv()


# 调用此方法，抓起历史数据或者进行数据更新
def request_history_data_or_update_data():
    if not os.path.exists('kline.csv'):
        get_all_history_data()
    fill_up_rest_data()
