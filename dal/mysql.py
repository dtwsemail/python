import pymysql
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String ,DateTime, Boolean

pymysql.install_as_MySQLdb()
# 连接数据库
engine = create_engine('mysql://billion6516:82790086@118.24.77.141:13306/billion?charset=utf8')
## 编码问题
 # # 获取基类
Base = declarative_base()

class News(Base): # 继承基类
    __tablename__ = 'students1'
    id = Column(Integer, primary_key = True)
    nickname = Column(String(20))
    name = Column(String(20), nullable = False)
    sex = Column(String(1))
    in_time = Column(DateTime)
    is_vaild = Column(Boolean)
    idcard = Column(Integer, unique = True)

News.metadata.create_all(engine)   # 创建表格

## 新增数据
from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=engine)

class OrmTest(object):
    def __init__(self):
        self.session = Session()

    def add_one(self):
        new_obj = News(
            nickname = '123',
            name = '321',
            sex = '男',
        )
        self.session.add(new_obj)
        self.session.commit()
        return new_obj

    def add_more(self):
        new_obj = News(
            nickname = '123',
            name = '321',
            sex = '男',
        )
        new_obj2 = News(
            nickname = 'wei',
            name = 'lai',
            sex = '女',
        )
        self.session.add_all([new_obj,
                              new_obj2])
        self.session.commit()
        return new_obj

    ## 查询数据
    def get_one(self):
        return self.session.query(News).get(10)  # get 是选id为2的

    def get_more(self):
        return self.session.query(News).filter_by(is_vaild=True)


    ## 修改数据
    ## 将一条当作多条的一种情况
    def update_data(self):
        data_list = self.session.query(News).filter(News.id >= 5)
        for item in data_list:
            if item:
                item.is_vaild = 0
                self.session.add(item)  # 加入
        self.session.commit()  # 提交
    ## filter 与 filter_by 的区别


    ## 删除数据
    def delete_data(self):
        data = self.session.query(News).get(8)
        if data:
            self.session.delete(data)
            self.session.commit()
        else:
            return False

    def delete_data_more(self):
        delete_list = self.session.query(News).filter(News.id <= 5)
        for item in delete_list:
            if item:
                self.session.delete(item)
            else:
                return False
        self.session.commit()



def main():
    obj = OrmTest()
    obj.add_one()
    obj.add_more()

    data = obj.get_one()

    ## 防止查询失误
    if data:
        print('ID:{0}  {1}'.format(data.id,data.sex))
    else:
        print('Not exist')

    data_more = obj.get_more()
    print(data_more.count())  # 计数
    for new_obj in data_more:
        print('ID:{0}  {1} {2} {3}'.format(new_obj.id,new_obj.sex,new_obj.name,new_obj.nickname))

    obj.update_data()
    print('数据修改成功')

    # obj.delete_data()
    # print('数据删除成功')

    obj.delete_data_more()


if __name__ == '__main__':
    main()