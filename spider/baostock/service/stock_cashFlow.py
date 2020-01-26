import baostock as bs
import pandas as pd

from common import stringUtils
from spider.baostock.dal import StockBasicInfo


# 季频现金流量
# 返回数据说明
# 参数名称	参数描述	算法说明
# code	证券代码
# pubDate	公司发布财报的日期
# statDate	财报统计的季度的最后一天, 比如2017-03-31, 2017-06-30
# CAToAsset	流动资产除以总资产
# NCAToAsset	非流动资产除以总资产
# tangibleAssetToAsset	有形资产除以总资产
# ebitToInterest	已获利息倍数	息税前利润/利息费用
# CFOToOR	经营活动产生的现金流量净额除以营业收入
# CFOToNP	经营性现金净流量除以净利润
# CFOToGr	经营性现金净流量除以营业总收入
basePath = "/Volumes/Mac/cdt/baostock/file/stockCashFlowCsv/"

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
            rs_cash_flow = bs.query_cash_flow_data(code=stockCode, year=year, quarter=quarter)

            # rs_growth = bs.query_growth_data(code="sh.600000", year=2017, quarter=2)
            while (rs_cash_flow.error_code == '0') & rs_cash_flow.next():
                operation_list.append(rs_cash_flow.get_row_data())

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
                 basePath + stockCode + "_c.csv");


if __name__ == '__main__':
    main()
    # result = download("SH.600000", "2017-07-01", "2017-12-31", "history_A_stock_k_data2.csv");

    # print(result)
