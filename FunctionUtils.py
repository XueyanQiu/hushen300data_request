import pandas as pd


def merge_two_csv():
    df1 = pd.read_csv('kline.csv')
    df2 = pd.read_csv('northdata.csv')
    df_res = pd.merge(left=df1, right=df2, left_on='交易日期', right_on='交易日期', how='inner')
    df_res.to_csv('merge.csv', index=False)


# 删除2015-02-10之前的数据
def clean_no_use_date_data():
    df = pd.read_csv('merge.csv')
    df = df[df['时间戳'] > 1423497599]
    df.to_csv('merge.csv', index=False, sep=',')


# todo：量化策略代码实现
# 首次读取30行，每次多读取一行。
# x行进行排序，去1/3处和2/3处的值为阈值。
