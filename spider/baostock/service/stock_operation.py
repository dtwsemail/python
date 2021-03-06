import baostock as bs
import pandas as pd

from common import stringUtils
from spider.baostock.dal import StockBasicInfo

# 季频成长能力
# 返回数据说明
# 参数名称	参数描述	算法说明
# code	证券代码
# pubDate	公司发布财报的日期
# statDate	财报统计的季度的最后一天, 比如2017-03-31, 2017-06-30
# NRTurnRatio	应收账款周转率(次)	营业收入/[(期初应收票据及应收账款净额+期末应收票据及应收账款净额)/2]
# NRTurnDays	应收账款周转天数(天)	季报天数/应收账款周转率(一季报：90天，中报：180天，三季报：270天，年报：360天)
# INVTurnRatio	存货周转率(次)	营业成本/[(期初存货净额+期末存货净额)/2]
# INVTurnDays	存货周转天数(天)	季报天数/存货周转率(一季报：90天，中报：180天，三季报：270天，年报：360天)
# CATurnRatio	流动资产周转率(次)	营业总收入/[(期初流动资产+期末流动资产)/2]
# AssetTurnRatio	总资产周转率	营业总收入/[(期初资产总额+期末资产总额)/2]
basePath = "/Volumes/Mac/cdt/baostock/file/stockOperateCsv/"

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
            rs_operation = bs.query_operation_data(code=stockCode, year=year, quarter=quarter)
            # rs_operation = bs.query_operation_data(code="sh.600000", year=2017, quarter=2)
            while (rs_operation.error_code == '0') & rs_operation.next():
                operation_list.append(rs_operation.get_row_data())

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
                 basePath + stockCode + "_o.csv");


if __name__ == '__main__':
    main()
    # result = download("SH.600000", "2017-07-01", "2017-12-31", "history_A_stock_k_data2.csv");

    # print(result)
