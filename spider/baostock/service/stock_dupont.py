import baostock as bs
import pandas as pd

from common import stringUtils
from spider.baostock.dal import StockBasicInfo


# 季频杜邦指数
# 返回数据说明
# 参数名称	参数描述	算法说明
# code	证券代码
# pubDate	公司发布财报的日期
# statDate	财报统计的季度的最后一天, 比如2017-03-31, 2017-06-30
# dupontROE	净资产收益率	归属母公司股东净利润/[(期初归属母公司股东的权益+期末归属母公司股东的权益)/2]*100%
# dupontAssetStoEquity	权益乘数，反映企业财务杠杆效应强弱和财务风险	平均总资产/平均归属于母公司的股东权益
# dupontAssetTurn	总资产周转率，反映企业资产管理效率的指标	营业总收入/[(期初资产总额+期末资产总额)/2]
# dupontPnitoni	归属母公司股东的净利润/净利润，反映母公司控股子公司百分比。如果企业追加投资，扩大持股比例，则本指标会增加。
# dupontNitogr	净利润/营业总收入，反映企业销售获利率
# dupontTaxBurden	净利润/利润总额，反映企业税负水平，该比值高则税负较低。净利润/利润总额=1-所得税/利润总额
# dupontIntburden	利润总额/息税前利润，反映企业利息负担，该比值高则税负较低。利润总额/息税前利润=1-利息费用/息税前利润
# dupontEbittogr	息税前利润/营业总收入，反映企业经营利润率，是企业经营获得的可供全体投资人（股东和债权人）分配的盈利占企业全部营收收入的百分比
basePath = "/Volumes/Mac/cdt/baostock/file/stockDupontCsv/"

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
            rs_dupont = bs.query_dupont_data(code=stockCode, year=year, quarter=quarter)
            # rs_dupont = bs.query_dupont_data(code="sh.600000", year=2017, quarter=2)

            # rs_growth = bs.query_growth_data(code="sh.600000", year=2017, quarter=2)
            while (rs_dupont.error_code == '0') & rs_dupont.next():
                operation_list.append(rs_dupont.get_row_data())

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
                 basePath + stockCode + "_dupont.csv");


if __name__ == '__main__':
    main()
    # result = download("SH.600000", "2017-07-01", "2017-12-31", "history_A_stock_k_data2.csv");

    # print(result)
