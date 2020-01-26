import baostock as bs
import pandas as pd

#
# 返回数据说明
# 参数名称	参数描述
# statYear	统计年度
# statMonth	统计月份
# m0Month	货币供应量M0（月）
# m0YOY	货币供应量M0（同比）
# m0ChainRelative	货币供应量M0（环比）
# m1Month	货币供应量M1（月）
# m1YOY	货币供应量M1（同比）
# m1ChainRelative	货币供应量M1（环比）
# m2Month	货币供应量M2（月）
# m2YOY	货币供应量M2（同比）
# m2ChainRelative	货币供应量M2（环比）
basePath = "/Volumes/Mac/cdt/baostock/file/moneySupply/"


def saveToCsv(data_list, fileName):
    result = pd.DataFrame(data_list)
    result.to_csv(fileName, encoding="gbk", index=False)


def download(beginDate: str, endDate: str, fileName):
    #### 登陆系统 ####
    lg = bs.login()
    # 显示登陆返回信息
    print('login respond error_code:' + lg.error_code)
    print('login respond  error_msg:' + lg.error_msg)

    # 查询季频估值指标盈利能力
    list = []
    # yearList = range(2003, 2020)
    # for year in yearList:
    #     rs = bs.query_money_supply_data_month(str(year)+"-01", str(year)+"-12")
    rs = bs.query_money_supply_data_month(beginDate, endDate)
    # print('query_loan_rate_data respond error_code:'+rs.error_code)
    # print('query_loan_rate_data respond  error_msg:'+rs.error_msg)
    while (rs.error_code == '0') & rs.next():
        list.append(rs.get_row_data())

    saveToCsv(list, fileName)


#### 登出系统 ####
bs.logout()


def main():
    download('2003-01', "2019-12", basePath + "moneySupplyCsv.csv")


if __name__ == '__main__':
    main()
    # result = download("SH.600000", "2017-07-01", "2017-12-31", "history_A_stock_k_data2.csv");

    # print(result)
