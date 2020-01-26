import baostock as bs
import pandas as pd

from common import stringUtils
from spider.baostock.dal import StockBasicInfo


#季频成长能力
# 入参参数含义：
#
# code：股票代码，sh或sz.+6位数字代码，或者指数代码，如：sh.601398。sh：上海；sz：深圳。此参数不可为空；
# year：统计年份，为空时默认当前年；
# quarter：统计季度，为空时默认当前季度。不为空时只有4个取值：1，2，3，4。

# 返回参数
# 参数名称	参数描述	算法说明
# code	证券代码
# pubDate	公司发布财报的日期
# statDate	财报统计的季度的最后一天, 比如2017-03-31, 2017-06-30
# YOYEquity	净资产同比增长率	(本期净资产-上年同期净资产)/上年同期净资产的绝对值*100%
# YOYAsset	总资产同比增长率	(本期总资产-上年同期总资产)/上年同期总资产的绝对值*100%
# YOYNI	净利润同比增长率	(本期净利润-上年同期净利润)/上年同期净利润的绝对值*100%
# YOYEPSBasic	基本每股收益同比增长率	(本期基本每股收益-上年同期基本每股收益)/上年同期基本每股收益的绝对值*100%
# YOYPNI	归属母公司股东净利润同比增长率	(本期归属母公司股东净利润-上年同期归属母公司股东净利润)/上年同期归属母公司股东净利润的绝对值*100%
basePath = "/Volumes/Mac/cdt/baostock/file/stockGrowthCsv/"

def saveToCsv(data_list, fileName):
    result = pd.DataFrame(data_list)
    result.to_csv(fileName, encoding="gbk", index=False)



def download(stockCode, yearList: list, fileName):
    #### 登陆系统 ####
    lg = bs.login()
    # 显示登陆返回信息
    print('login respond error_code:' + lg.error_code)
    print('login respond  error_msg:' + lg.error_msg)

    # 查询季频估值指标盈利能力
    operation_list = []

    for year in yearList:
        for quarter in [1, 2, 3, 4]:
            rs_growth = bs.query_growth_data(code=stockCode, year=year, quarter=quarter)
            # rs_growth = bs.query_growth_data(code="sh.600000", year=2017, quarter=2)
            while (rs_growth.error_code == '0') & rs_growth.next():
                operation_list.append(rs_growth.get_row_data())

    saveToCsv(operation_list, fileName)

    #### 登出系统 ####
    bs.logout()


def getCalculateYears(listDate: str):
    beginYear = 2006;
    if listDate:
        if len(listDate) > 4:
            beginYear = int(listDate[0: 4])
            if beginYear < 2006:
                beginYear = 2006;

    return range(beginYear, 2019)


def main():
    obj = StockBasicInfo.StockBasicDao()
    data = obj.queryAll()
    print(data.count())  # 计数
    for new_obj in data:
        stockCode = new_obj.ts_code
        listDate = new_obj.list_date
        print('ID:{0}  {1} {2} '.format(new_obj.id, stockCode, new_obj.list_date))
        download(stringUtils.reverseCode(stockCode), getCalculateYears(listDate),
                 basePath + stockCode + "_g.csv");


if __name__ == '__main__':
    main()
    # result = download("SH.600000", "2017-07-01", "2017-12-31", "history_A_stock_k_data2.csv");

    # print(result)
