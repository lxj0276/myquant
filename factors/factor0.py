# -*- coding: utf-8 -*-
"""
Created on Tue Jun 14 14:47:03 2016
多因子计算的父类最开始程序
其中
__init__：连接数据库
get_交易日期：获取交易日期
get_上市状态变更:获取上市公司的上市状态
get_info:获取A股代码、innercode、companycode是公用的函数
create_newdata:创建表


"""
import pandas as pd
import numpy as np
import pymysql
from tqdm import *
    
class init_factor:
    '''
    数据源:聚源数据库
    get_bonus:提取聚源数据库分红数据
    get_price:提取聚源数据库的行情数据  

    '''
    
    def __init__(self): #连接聚源数据库
        self._dbengine =  pymysql.connect(host='backtest.invesmart.net',
                           port=3308,
                           user='jydb',
                           password='Jydb@123465',
                           database='jydb',
                           charset='gbk')
        self._dbengine1 =  pymysql.connect(host='127.0.0.1',
                           port=3306,
                           user='root',
                           password='root',
                           database='mysql',
                           charset='gbk')

#    def __del__(self):
#        self._dbengine.close()
    
    
    def get_交易日期(self,startdate):
        sql = "select TradingDay from QT_IndexQuote  where innercode=1 and \
                TradingDay>=STR_TO_DATE("+startdate+", '%Y%m%d') "
        TradingDay = pd.read_sql(sql,con=self._dbengine)
        return TradingDay

    
    def get_上市状态变更(self):
        #获取A股上市、暂停、退市、恢复上市等信息
        sql = "select * from  LC_ListStatus  where innercode in (SELECT innercode from SecuMain\
                where SecuMarket in (83,90)  and SecuCategory=1)  "
        listedstate = pd.read_sql(sql,con=self._dbengine)
        return listedstate
    
    def get_info(self):
        #获取A股代码、简称等信息
        sql = "select InnerCode,CompanyCode,SecuCode,secuabbr,ListedDate,ListedState from SecuMain \
                where SecuMarket in (83,90)  and SecuCategory=1  "
        info = pd.read_sql(sql,con=self._dbengine)
        return info

    def create_newdata(self,databasename,tablename,data_structure):
        '''
        databasename：数据库名字
        tablename：表明
        data_structure：字段及相关类型
        创建新的数据表
         drop_sql = "drop table if exists test.OperatingRevenueTTM_week "  
         sql = "create table  if not exists test.OperatingRevenueTTM_week (\
            id int  not null primary key AUTO_INCREMENT,\
            dt datetime,   \
            SecuCode varchar(6),  \
            营业收入TTM float, \
            营业收入TTM同比 float,\
            营业收入TTM环比 float\
            )"
    
        '''
        cursor = self._dbengine1.cursor()
        drop_sql = "drop table if exists %s.%s "%(databasename,tablename)  
        create_sql = "create table  if not exists %s.%s %s"%(databasename,tablename,data_structure)
        
        try:       
            cursor.execute(drop_sql)   
            cursor.execute(create_sql) 
            self._dbengine1.commit()# 提交到数据库执行
        except Exception as e:         
            # 如果发生错误则回滚
            print(e)
            self._dbengine1.rollback()
            
    
    
    def insert_data(self,databasename,tablename,names,values):
        '''
        插入数据
        insert_sql = "insert into test.OperatingRevenueTTM_week (dt,SecuCode,营业收入TTM,营业收入TTM同比,营业收入TTM环比)\
                            values(%s,%s,%s,%s,%s)" 
        names = "(dt,SecuCode,营业收入TTM,营业收入TTM同比,营业收入TTM环比)"
        tuple(len(names.split(','))*[%s])
        values:插入的数据，np.array格式
        databasename = 'test'
        tablename = 'test0'
        '''
        cursor = self._dbengine1.cursor()
        vv = str(('%s,'*len(names.split(','))))[:-1]
        try:
            insert_sql = "insert into %s.%s %s values(%s)"%(databasename,tablename,names,vv)
            values = np.array(values)                 
            #t1 = time()
            for i in tqdm(range(0,len(values),50000)):            
                v1 = values[i:i+50000]
                v1 = tuple(map(tuple,v1))         
                cursor.executemany(insert_sql,v1) 
                self._dbengine1.commit()# 提交到数据库执行          
        except Exception as e:            
            # 如果发生错误则回滚
            self._dbengine1.rollback()
            print(e)         
        finally:
            self._dbengine.close()
            self._dbengine1.close()
    
    def get_最近的数据(self,databasename,tablename):
        sql = "select * from %s.%s where dt=(select max(dt) from %s.%s)"%(databasename,tablename,databasename,tablename)
        data = pd.read_sql(sql,con=self._dbengine1)
        return data    
    
    def update_data(self,databasename,tablename,last_data):
        '''
        更新因子表
        last_data:该因子库最近的一天/周/月的数据
        '''
        cursor = self._dbengine1.cursor()
        lastid = str(tuple(last_data['id']))
        drop_sql = "delete from %s.%s where id in %s"%(databasename,tablename,lastid)  
        update_sql = "alter table %s.%s AUTO_INCREMENT=%s"%(databasename,tablename,min(last_data['id']))#自增连续
        
        try:       
            cursor.execute(drop_sql)   
            cursor.execute(update_sql) 
            self._dbengine1.commit()# 提交到数据库执行
        except Exception as e:         
            # 如果发生错误则回滚
            print(e)
            self._dbengine1.rollback()
    
    def finance_getinfo_rank(self,data,info,up_date):
        '''
        财务数据的处理
        1. 获得innercode\secucode等字段
        2. 该财务指标去上市后的数据，上市前数据忽略
        3. TTM可能会用到上市前数据，这里可以忍受，进行忽略
        4. 对指标进行排序，以保证正确顺序，我们能够
        data:需要处理的财务数据
        info：innercode、sucucode\companycode等信息
        up_date:A股上市日期
        '''
        data = pd.merge(data,info[['InnerCode','CompanyCode','SecuCode']],on='CompanyCode')       
        data = pd.merge(data,up_date[['InnerCode','ChangeDate']],on='InnerCode',how='left')
        data = data[data['EndDate']>data['ChangeDate']]
        data = data.sort_values(['CompanyCode','EndDate','InfoPublDate'],ascending=True)
        return data
    
    def finance_cum_to_ttm(self,data,indicator):
        '''
        财务数据，当期数据计算该指标的TTM值
        需要有CompanyCode，EndDate，InfoPublDate字段
        需要：已经排序并且删除了重复数据
        data = data.sort_values(['CompanyCode','EndDate','InfoPublDate'])
        data = data.drop_duplicates(['CompanyCode','EndDate'],keep='last')
        indicator:需要计算的指标，如‘归属母公司的净利润’
        ''' 
        data['month'] = data['EndDate'].apply(lambda x:x.month)
        data['year'] = data['EndDate'].apply(lambda x:x.year)
    
        year_data = data[data['month'] ==12] #获取年度数据
        year_data['year'] = year_data['year'].apply(lambda x:x+1)
        year_data['temp'] = year_data[indicator]
        data = pd.merge(data,year_data[['year','CompanyCode','temp']],
                               on=['CompanyCode','year'],how='left')   
        data['%sTTM'%indicator] = np.where((data['CompanyCode']==data['CompanyCode'].shift(4))
                                                &(data['month']==data['month'].shift(4))&
                                                (data['year']==data['year'].shift(4)+1)&
                                                (data[indicator].shift(4)!=0)&
                                                (data['month']!=12),
                                                data[indicator]+data['temp']-data[indicator].shift(4)-1,
                                                np.where((data['month']!=12)&
                                                 (pd.isnull(data[indicator]==False)),data[indicator],np.nan))
        #data['%sTTM'%indicator] = np.where(pd.isnull(data['%sTTM'%indicator])==False,data['%sTTM'%indicator],None)
        return data
    

   
        
        
        
        
    