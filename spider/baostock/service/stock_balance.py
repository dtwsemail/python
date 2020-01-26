import baostock as bs
import pandas as pd

from common import stringUtils
from spider.baostock.dal import StockBasicInfo


# 季频偿债能力
# 返回数据说明
# 参数名称	参数描述	算法说明
# code	证券代码
# pubDate	公司发布财报的日期
# statDate	财报统计的季度的最后一天, 比如2017-03-31, 2017-06-30
# currentRatio	流动比率	流动资产/流动负债
# quickRatio	速动比率	(流动资产-存货净额)/流动负债
# cashRatio	现金比率	(货币资金+交易性金融资产)/流动负债
# YOYLiability	总负债同比增长率	(本期总负债-上年同期总负债)/上年同期中负债的绝对值*100%
# liabilityToAsset	资产负债率	负债总额/资产总额
# assetToEquity	权益乘数	资产总额/股东权益总额=1/(1-资产负债率)
basePath = "/Volumes/Mac/cdt/baostock/file/stockBalanceCsv/"

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
            rs_balance = bs.query_balance_data(code=stockCode, year=year, quarter=quarter)

            # rs_growth = bs.query_growth_data(code="sh.600000", year=2017, quarter=2)
            while (rs_balance.error_code == '0') & rs_balance.next():
                operation_list.append(rs_balance.get_row_data())

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
                 basePath + stockCode + "_b.csv");


if __name__ == '__main__':
    main()
    # result = download("SH.600000", "2017-07-01", "2017-12-31", "history_A_stock_k_data2.csv");

    # print(result)
