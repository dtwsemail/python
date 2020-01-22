import baostock as bs
import pandas as pd

from common import stringUtils
from dal import StockBasicInfo
from dal import StockHistoryInfo


def download(stockCode, beginDate, endDate, fileName):
    #### 登陆系统 ####
    lg = bs.login()
    # 显示登陆返回信息
    print('login respond error_code:' + lg.error_code)
    print('login respond  error_msg:' + lg.error_msg)

    #### 获取沪深A股历史K线数据 ####
    # 详细指标参数，参见“历史行情指标参数”章节；“分钟线”参数与“日线”参数不同。
    # 分钟线指标：date,time,code,open,high,low,close,volume,amount,adjustflag
    rs = bs.query_history_k_data_plus(stockCode,
                                      "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
                                      start_date="2006-01-01", end_date=endDate,
                                      frequency="d", adjustflag="3")
    print('query_history_k_data_plus respond error_code:' + rs.error_code)
    print('query_history_k_data_plus respond  error_msg:' + rs.error_msg)

    #### 打印结果集 ####
    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(StockHistoryInfo.StockHistory.convert(rs.get_row_data()))

    dao = StockHistoryInfo.StockHistoryDao();
    dao.add_more(data_list)
    # result = pd.DataFrame(data_list, columns=rs.fields)

    #### 结果集输出到csv文件 ####
    # result.to_csv(fileName, index=False)

    #### 登出系统 ####
    bs.logout()
    return "success";


def main():
    obj = StockBasicInfo.StockBasicDao()
    data = obj.queryAll()
    print(data.count())  # 计数
    for new_obj in data:
        stockCode = new_obj.ts_code
        print('ID:{0}  {1} {2} '.format(new_obj.id, stockCode, new_obj.list_date))
        download(stringUtils.reverseCode(stockCode), "2017-07-01", "2020-01-20", stockCode + "_d_3.csv");


if __name__ == '__main__':
    main()
    # result = download("SH.600000", "2017-07-01", "2017-12-31", "history_A_stock_k_data2.csv");

    # print(result)
