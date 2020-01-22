from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

# 连接数据库
engine = create_engine('mysql://billion6516:82790086@118.24.77.141:13306/billion?charset=utf8')
## 编码问题
# # 获取基类
Base = declarative_base()


class StockBasic(Base):  # 继承基类
    __tablename__ = 'stock_basic'
    id = Column(Integer, primary_key=True)
    ts_code = Column(String(20))
    list_date = Column(String(20))


## 新增数据
from sqlalchemy.orm import sessionmaker

Session = sessionmaker(bind=engine)


class StockBasicDao(object):
    def __init__(self):
        self.session = Session()

    ## 查询数据
    def get_one(self):
        return self.session.query(StockBasic).filter_by(ts_code="603730.SH")  # get 是选id为2的

    def queryAll(self):
        return self.session.query(StockBasic)


def main():
    obj = StockBasicDao()
    data_more = obj.queryAll()
    print(data_more.count())  # 计数
    for new_obj in data_more:
        print('ID:{0}  {1} {2} '.format(new_obj.id, new_obj.ts_code,new_obj.list_date))


if __name__ == '__main__':
    main()
