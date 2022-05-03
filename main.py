from RequestKLineData import request_history_data_or_update_data as upload_kline_data
from RequestNorthData import upload_north_data


if __name__ == '__main__':
    upload_north_data()
    upload_kline_data()
