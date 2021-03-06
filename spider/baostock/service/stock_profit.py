import baostock as bs
import pandas as pd

from common import stringUtils
from spider.baostock.dal import StockBasicInfo

# 季频盈利能力
# 返回数据说明
# 参数名称	参数描述	算法说明
# code	证券代码
# pubDate	公司发布财报的日期
# statDate	财报统计的季度的最后一天, 比如2017-03-31, 2017-06-30
# roeAvg	净资产收益率(平均)(%)	归属母公司股东净利润/[(期初归属母公司股东的权益+期末归属母公司股东的权益)/2]*100%
# npMargin	销售净利率(%)	净利润/营业收入*100%
# gpMargin	销售毛利率(%)	毛利/营业收入*100%=(营业收入-营业成本)/营业收入*100%
# netProfit	净利润(元)
# epsTTM	每股收益	归属母公司股东的净利润TTM/最新总股本
# MBRevenue	主营营业收入(元)
# totalShare	总股本
# liqaShare	流通股本
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
    profit_list = []

    for year in yearList:
        for quarter in [1, 2, 3, 4]:
            rs_profit = bs.query_profit_data(code=stockCode, year=year, quarter=quarter)
            while (rs_profit.error_code == '0') & rs_profit.next():
                profit_list.append(rs_profit.get_row_data())

    saveToCsv(profit_list, fileName)

    #### 登出系统 ####
    bs.logout()


def getProfitYears(listDate: str):
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
        download(stringUtils.reverseCode(stockCode), getProfitYears(listDate),
                 "spider/baostock/file/stockProfiltCsv/" + stockCode + "_q.csv");


if __name__ == '__main__':
    main()
    # result = download("SH.600000", "2017-07-01", "2017-12-31", "history_A_stock_k_data2.csv");

    # print(result)
