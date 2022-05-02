import csv
import json
import os.path
import time

import pandas as pd
import requests


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


# 将一个list写入csv，这个list自己内部有序
# 交易日期,时间戳,最高价,最低价,开盘价,收盘价,涨跌额
def write_csv(data: list, path: str):
    f = open(path, 'a+', encoding='utf-8', newline='')
    csv_writer = csv.writer(f)
    for item in data:
        csv_writer.writerow([item['date'], item['time'], item['kline']['high'], item['kline']['low'],
                             item['kline']['open'], item['kline']['close'], item['kline']['increase']])
    f.close()


# 对klinedata.csv以时间顺序排序，将有序的表存放到sort.csv
def sort_csv():
    data = csv.reader(open('klinedata.csv', encoding='utf-8'), delimiter=',')
    sortedlist = sorted(data, key=lambda x: (x[0], x[1]), reverse=False)

    with open("sort.csv", "w", newline='') as f:
        fileWriter = csv.writer(f, delimiter=',')
        for row in sortedlist:
            fileWriter.writerow(row)
    f.close()


# 初次运行，获取全部历史数据，并排序
def get_all_history_data():
    if not os.path.exists('klinedata.csv'):
        do_while = True
        start_time = int(time.time())
        while do_while:
            res_list = grab_history_data(start_time)
            # 2005-01-04为抓取的截止时间
            if int(res_list[0]['time']) <= 1104768000 or len(res_list) < 90:
                do_while = False
            start_time = int(res_list[0]['time']) - 86400
            write_csv(res_list, 'klinedata.csv')
        clean_repeated('klinedata.csv')
    sort_csv()


# 以当前时间戳为endtime，抓取（endtime-表中最后一个时间戳）的天数的数据，其中可能有非交易日，因此进行一次去重
def fill_up_rest_data():
    with open('sort.csv', "r") as f_csv:
        data = [[x.strip() for x in line.strip().split(',')] for line in f_csv.readlines()][-1][1]
    f_csv.close()
    rest_list = grab_history_data(int(time.time()), int((time.time() - float(data)) / 86400))
    write_csv(rest_list, 'sort.csv')
    clean_repeated('sort.csv')


# csv文件去除重复行
def clean_repeated(path: str):
    frame = pd.read_csv(path, index_col=0)
    df = pd.DataFrame(frame)
    df.drop_duplicates(inplace=True)
    df.to_csv(path)


if __name__ == '__main__':
    if not os.path.exists('sort.csv'):
        get_all_history_data()
    fill_up_rest_data()
