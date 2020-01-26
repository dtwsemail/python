from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

# 连接数据库
engine = create_engine('mysql://billion6516:82790086@118.24.77.141:13306/billion?charset=utf8')
## 编码问题
# # 获取基类
Base = declarative_base()


class StockHistory(Base):  # 继承基类
    __tablename__ = 'stock_history'
    id = Column(Integer, primary_key=True)
    date = Column(String(16))
    code = Column(String(16))
    open = Column(String(16))
    high = Column(String(16))
    low = Column(String(16))
    close = Column(String(16))
    preclose = Column(String(16))
    volume = Column(String(16))
    amount = Column(String(16))
    adjustflag = Column(String(16))
    turn = Column(String(16))
    tradestatus = Column(String(16))
    pctChg = Column(String(16))
    peTTM = Column(String(16))
    pbMRQ = Column(String(16))
    psTTM = Column(String(16))
    pcfNcfTTM = Column(String(16))
    isST = Column(String(16))

    @staticmethod
    def convert(data):
        return StockHistory(
            date=data[0],
            code=data[1],
            open=data[2],
            high=data[3],
            low=data[4],
            close=data[5],
            preclose=data[6],
            volume=data[7],
            amount=data[8],
            adjustflag=data[9],
            turn=data[10],
            tradestatus=data[11],
            pctChg=data[12],
            peTTM=data[13],
            pbMRQ=data[14],
            psTTM=data[15],
            pcfNcfTTM=data[16],
            isST=data[17]
        )


## 新增数据
from sqlalchemy.orm import sessionmaker

Session = sessionmaker(bind=engine)


class StockHistoryDao(object):
    def __init__(self):
        self.session = Session()

    def add_one(self, data):
        new_obj =  StockHistory.convert(data)
        self.session.add(new_obj)
        self.session.commit()
        return new_obj

    def add_more(self,list):
        self.session.add_all(list)
        self.session.commit()


def main():
    obj = StockHistoryDao()
    obj.add_one()
    obj.add_more()


    data_more = obj.get_more()
    print(data_more.count())  # 计数
    for new_obj in data_more:
        print('ID:{0}  {1} {2} {3}'.format(new_obj.id, new_obj.sex, new_obj.name, new_obj.nickname))





if __name__ == '__main__':
    main()
