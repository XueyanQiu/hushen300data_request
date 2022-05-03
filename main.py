from RequestKLineData import request_history_data_or_update_data as upload_kline_data
from RequestNorthData import upload_north_data
import FunctionUtils


if __name__ == '__main__':
    upload_north_data()
    upload_kline_data()
    FunctionUtils.merge_two_csv()
    FunctionUtils.clean_no_use_date_data()

