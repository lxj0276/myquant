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

！！！注意事项：
finance_getinfo_rank这个函数中，由于对指标进行了填充，取最新值，因此指标未来不能做横向处理，
比如中国平安20170331，非经常损益值为空，但我们取了上一期20161231或者20160930的值，
这个时候计算扣非净利润，不能用20170331的净利润-本期非经常性损益，而是在finance_getinfo_rank前就要进行计算，请注意。

"""
import pandas as pd
import numpy as np
import pymysql
import datetime
from tqdm import *
    
class init_factor:
    '''
    数据源:聚源数据库
    get_bonus:提取聚源数据库分红数据
    get_price:提取聚源数据库的行情数据  

    '''
    
    def __init__(self): #连接聚源数据库
#        self._dbengine =  pymysql.connect(host='backtest.invesmart.net',
#                           port=3308,
#                           user='jydb',
#                           password='Jydb@123465',
#                           database='jydb',
#                           charset='gbk')
#        self._dbengine1 =  pymysql.connect(host='127.0.0.1',
#                           port=3306,
#                           user='root',
#                           password='root',
#                           database='mysql',
#                           charset='gbk')
        self._dbengine =  pymysql.connect(host='192.168.1.139',
                           port=3306,
                           user='jydb',
                           password='jydb',
                           database='jydb',
                           charset='gbk')
        self._dbengine1 =  pymysql.connect(host='192.168.1.139',
                           port=3306,
                           user='jydb',
                           password='jydb',
                           charset='gbk')
        self.datapath  = "C:\\py_data\\datacenter\\quote.h5"
        

#    def __del__(self):
#        self._dbengine.close()
    def to_查错(self,data,datapath):
        '''
        把计算好的因子进行随机挑选，并导出到excel中进行检验
        data:计算好的因子
        datapath：保存到地址
        '''
        length = len(data)
        index = np.random.randint(1,length,500)
        data2 = data.iloc[index]
        data2.to_excel(datapath)
    
    def transto_None(self,data):
        '''
        处理data数据中包含nan、inf数据，转为None,以便mysql数据库能够认识
        '''
        columns = list(data.columns)
        for i in range(2,len(columns)):
            names = columns[i]
            data[names] = np.where(pd.isnull(data[names])!=True, data[names], None)
            data[names] = np.where(data[names]==np.inf, None,data[names])
        return data
     
    
    def get_行情(self,startdate,indicator=None,indicator1=None):
        '''
        获取聚源数据库非复权行情数据,从本地的H5中获取，最多获取三个字段，默认获取收盘数据
        '''
        sql = "select B.SecuCode ,B.InnerCode,B.CompanyCode,\
                A.TradingDay,A.ClosePrice as cp from QT_DailyQuote  A \
                inner join SecuMain B   on A.InnerCode = B.InnerCode   and  B.SecuCategory=1 and\
                B.SecuMarket in (83,90)  where TradingDay >=STR_TO_DATE("+startdate+",'%Y%m%d')  \
                order by TradingDay"
        if indicator is not None:
            sql = "select B.SecuCode ,B.InnerCode,B.CompanyCode,\
                A.TradingDay,A.ClosePrice as cp, "+indicator+" from QT_DailyQuote  A \
                inner join SecuMain B   on A.InnerCode = B.InnerCode   and  B.SecuCategory=1 and\
                B.SecuMarket in (83,90)  where TradingDay >=STR_TO_DATE("+startdate+",'%Y%m%d')  \
                order by TradingDay"
        if indicator1 is not None:
            sql = "select B.SecuCode ,B.InnerCode,B.CompanyCode,\
                A.TradingDay,A.ClosePrice as cp, "+indicator+","+indicator1+" from QT_DailyQuote  A \
                inner join SecuMain B   on A.InnerCode = B.InnerCode   and  B.SecuCategory=1 and\
                B.SecuMarket in (83,90)  where TradingDay >=STR_TO_DATE("+startdate+",'%Y%m%d')  \
                order by TradingDay"
               
        quote = pd.read_sql(sql,con=self._dbengine)
        return quote
        
    
    
    def get_交易日期(self,startdate):
        sql = "select TradingDay from QT_IndexQuote  where innercode=1 and \
                TradingDay>=STR_TO_DATE("+startdate+", '%Y%m%d') "
        TradingDay = pd.read_sql(sql,con=self._dbengine)
#        TradingDay = pd.read_hdf(self.datapath,"equity_quote",columns=['TradingDay'],
#                            where="TradingDay>="+startdate+" & SecuCode='000001'")
        return TradingDay

    
    def get_上市状态变更(self):
        #获取A股上市、暂停、退市、恢复上市等信息
        sql = "select * from  LC_ListStatus  where innercode in (SELECT innercode from SecuMain\
                where SecuMarket in (83,90)  and SecuCategory=1)  "
        listedstate = pd.read_sql(sql,con=self._dbengine)
        return listedstate
    
    def get_info(self):
        #获取A股代码、简称等信息
        sql = "select InnerCode,CompanyCode,SecuCode,SecuAbbr,ListedDate,ListedState from SecuMain \
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
            TradingDay datetime,   \
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
        '''
        从数据库提取最新数据
        '''
        sql = "select * from %s.%s where TradingDay=(select max(TradingDay) from %s.%s)"%(databasename,tablename,databasename,tablename)
        data = pd.read_sql(sql,con=self._dbengine1)
        return data    
    
    def update_data(self,databasename,tablename,last_data):
        '''
        更新因子表,删除最新一期数据，以防止该数据不是每周的最后一天的数据
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
    
    def finance_getinfo_rank(self,data,info,fill=False):
        '''
        财务数据的处理
        1. 获得innercode\secucode等字段
        3. TTM可能会用到上市前数据，这里可以忍受，进行忽略
        4. 对指标进行排序，以保证正确顺序，我们能够
        data:需要处理的财务数据
        info：innercode、sucucode\companycode等信息
        ListedDate:A股上市日期
        '''
        data = pd.merge(data,info[['InnerCode','CompanyCode','SecuCode','ListedDate']],on='CompanyCode')       
        #data = data[data['EndDate']>data['ListedDate']]
        data = data.sort_values(['CompanyCode','EndDate','InfoPublDate'],ascending=True)
        ##对应提取的基本面数据，若是空值，则取上一期的数组！！！
        if fill == True:
            code = data['CompanyCode']
            data = data.groupby(['CompanyCode']).fillna(method='ffill')
            data['CompanyCode'] = code
        return data
    
    def get_同比(self,data,indicator):
        '''
        计算财务指标的同比,需要有EndDate、CompanyCode字段
        '''
        data['lastdate'] = data['EndDate'].apply(lambda x:datetime.datetime(x.year-1,x.month,x.day))
        data['temp'] = data[indicator]
        data = pd.merge(data,data[['EndDate','CompanyCode','temp']],
                               left_on=['CompanyCode','lastdate'],right_on=['CompanyCode','EndDate'],how='left')        
        data['同比'] = np.where(data['temp_y']!=0,(data[indicator]-data['temp_y'])/abs(data['temp_y']),np.nan)
        return data['同比'].values
    
    def get_环比(self,data,indicator):
        '''
        计算财务指标环比，需要有EndDate、CompanyCode字段
        只能计算单季度值，或者TTM值
        data = temp_OperatingRevenue
        indicator = '营业收入TTM'
        '''
        data['month'] = data['EndDate'].apply(lambda x:x.month)
        data['predate'] = np.where(data['month']==3,data['EndDate'].apply(lambda x:datetime.datetime(x.year-1,12,31)),
                            np.where(data['month']==6,data['EndDate'].apply(lambda x:datetime.datetime(x.year,3,31)),
                                np.where(data['month']==9,data['EndDate'].apply(lambda x:datetime.datetime(x.year,6,30)),
                                         data['EndDate'].apply(lambda x:datetime.datetime(x.year,9,30)))))
        data['temp'] = data[indicator]
        data = pd.merge(data,data[['CompanyCode','EndDate','temp']],left_on=['predate','CompanyCode'],
                                right_on = ['EndDate','CompanyCode'],how='left')
        data['环比'] = np.where(data['temp_y']!=0,(data[indicator]-data['temp_y'])/abs(data['temp_y']),np.nan)
        return data['环比'].values
    
    def get_N年复合增长率(self,data,indicator,N):
        '''
        计算财务指标的同比,需要有EndDate、CompanyCode字段
        n:几年
        ntype:类型，复合还是算绝对
        n=3
        '''
        data['lastdate'] = data['EndDate'].apply(lambda x:datetime.datetime(x.year-N,x.month,x.day))
        data['temp'] = data[indicator]
        data = pd.merge(data,data[['EndDate','CompanyCode','temp']],
                               left_on=['CompanyCode','lastdate'],right_on=['CompanyCode','EndDate'],how='left')        
        data['增长率'] = np.where(data['temp_y']!=0,(data[indicator]/abs(data['temp_y']))**(1/N)-1,np.nan)
        return data['增长率'].values 
    
    def get_ttm(self,data,indicator):
        '''
        财务数据，当期数据计算该指标的TTM值
        需要有CompanyCode，EndDate，InfoPublDate字段
        需要：已经排序并且删除了重复数据
        data = data.sort_values(['CompanyCode','EndDate','InfoPublDate'])
        data = data.drop_duplicates(['CompanyCode','EndDate'],keep='last')
        indicator:需要计算的指标，如‘归属母公司的净利润’
        data = temp_OperatingRevenue
        indicator = 'TotalOperatingRevenue'
        ''' 
        data['month'] = data['EndDate'].apply(lambda x:x.month)  
        data['lastdate'] = data['EndDate'].apply(lambda x:datetime.datetime(x.year-1,x.month,x.day))
        data['yeardate'] = data['EndDate'].apply(lambda x:datetime.datetime(x.year-1,12,31))
        data['temp'] = data[indicator]
        data = pd.merge(data,data[['EndDate','CompanyCode','temp']],
                               left_on=['CompanyCode','yeardate'],right_on=['CompanyCode','EndDate'],how='left')        
        data = pd.merge(data,data[['EndDate_x','CompanyCode','temp_x']],
                               left_on=['CompanyCode','lastdate'],right_on=['CompanyCode','EndDate_x'],how='left')  
        data['TTM'] = np.where(data['month']!=12,data[indicator]+data['temp_y']-data['temp_x_y'],data[indicator])
        return data['TTM'].values
    
    def get_单季值(self,data,indicator):
        '''
        财务数据，当期数据计算该指标单季值
        需要有CompanyCode，EndDate，InfoPublDate字段
        需要：已经排序并且删除了重复数据
        data = data.sort_values(['CompanyCode','EndDate','InfoPublDate'])
        data = data.drop_duplicates(['CompanyCode','EndDate'],keep='last')
        indicator:需要计算的指标，如‘归属母公司的净利润’
        ''' 
        data['month'] = data['EndDate'].apply(lambda x:x.month)
        data['predate'] = np.where(data['month']==3,data['EndDate'].apply(lambda x:datetime.datetime(x.year-1,12,31)),
                            np.where(data['month']==6,data['EndDate'].apply(lambda x:datetime.datetime(x.year,3,31)),
                                np.where(data['month']==9,data['EndDate'].apply(lambda x:datetime.datetime(x.year,6,30)),
                                         data['EndDate'].apply(lambda x:datetime.datetime(x.year,9,30)))))
        
        data['temp'] = data[indicator]
        data = pd.merge(data,data[['CompanyCode','EndDate','temp']],left_on=['predate','CompanyCode'],
                                right_on = ['EndDate','CompanyCode'],how='left')
        data['单季值'] = np.where(((data['month']==3)|(data['month']==12)),data[indicator],
                            data[indicator]-data['temp_y'])
        return data['单季值'].values
    
    def get_财务表(self,sheetname,startdate,indicator1,indicator2=None,indicator3=None,indicator4=None,indicator5=None):
        '''
        sheetname:
        1.利润分配表_新会计准则 LC_IncomeStatementAll,最多可以同时提取5个指标
        2. 资产负债表_新会计准则 LC_BalanceSheetAll
        3. 现金流量表_新会计准则 LC_CashFlowStatementAll
        '''
                    
        sql = "select InfoPublDate,CompanyCode,EndDate,IfAdjusted, EnterpriseType as CompanyType,\
                "+indicator1+" from  "+sheetname+"    where \
                 IfMerged=1 and IfAdjusted in (1,2) and InfoPublDate>="+startdate+"" 
        if indicator2 is not None:
            sql = "select InfoPublDate,CompanyCode,EndDate,IfAdjusted,EnterpriseType as CompanyType, \
                    "+indicator1+" ,"+indicator2+" from  "+sheetname+"    where \
                     IfMerged=1 and IfAdjusted in (1,2)  and InfoPublDate>="+startdate+"" 
        if indicator3 is not None:
            sql = "select InfoPublDate,CompanyCode,EndDate,IfAdjusted,EnterpriseType as CompanyType, \
                    "+indicator1+" ,"+indicator2+","+indicator3+" from  "+sheetname+"    where \
                      IfMerged=1 and IfAdjusted in (1,2) and InfoPublDate>="+startdate+"" 
        if indicator4 is not None:
            sql = "select InfoPublDate,CompanyCode,EndDate,IfAdjusted,EnterpriseType as CompanyType, \
                    "+indicator1+" ,"+indicator2+","+indicator3+","+indicator4+" from  "+sheetname+"   where \
                     IfMerged=1 and IfAdjusted in (1,2) and InfoPublDate>="+startdate+"" 
        if indicator5 is not None:
            sql = "select InfoPublDate,CompanyCode,EndDate,IfAdjusted,EnterpriseType as CompanyType, \
                    "+indicator1+" ,"+indicator2+","+indicator3+","+indicator4+","+indicator5+" from "+sheetname+"   where \
                     IfMerged=1 and IfAdjusted in (1,2) and InfoPublDate>="+startdate+""             
        data = pd.read_sql(sql,con=self._dbengine)
        return data
    
    def get_单季财务表(self,sheetname,startdate,indicator1,indicator2=None,indicator3=None,indicator4=None,indicator5=None):
        '''
        sheetname:
        1.利润分配表_新会计准则 LC_IncomeStatementAll,最多可以同时提取5个指标
        2. 资产负债表_新会计准则 LC_BalanceSheetAll
        3. 现金流量表_新会计准则 LC_CashFlowStatementAll
        '''
                    
        sql = "select InfoPublDate,CompanyCode,EndDate,Mark, CompanyType,\
                "+indicator1+" from  "+sheetname+"    where \
                 Mark in (1,2) and InfoPublDate>="+startdate+"" 
        if indicator2 is not None:
            sql = "select InfoPublDate,CompanyCode,EndDate,Mark, CompanyType, \
                    "+indicator1+" ,"+indicator2+" from  "+sheetname+"    where \
                     Mark in (1,2) and InfoPublDate>="+startdate+"" 
        if indicator3 is not None:
            sql = "select InfoPublDate,CompanyCode,EndDate,Mark, CompanyType, \
                    "+indicator1+" ,"+indicator2+","+indicator3+" from  "+sheetname+"    where \
                     Mark in (1,2) and InfoPublDate>="+startdate+"" 
        if indicator4 is not None:
            sql = "select InfoPublDate,CompanyCode,EndDate,Mark, CompanyType, \
                    "+indicator1+" ,"+indicator2+","+indicator3+","+indicator4+" from  "+sheetname+"   where \
                     Mark in (1,2) and InfoPublDate>="+startdate+"" 
        if indicator5 is not None:
            sql = "select InfoPublDate,CompanyCode,EndDate,Mark, CompanyType, \
                    "+indicator1+" ,"+indicator2+","+indicator3+","+indicator4+","+indicator5+" from "+sheetname+"   where \
                     Mark in (1,2) and InfoPublDate>="+startdate+""             
        data = pd.read_sql(sql,con=self._dbengine)
        return data
    
    def get_股本表(self,indicator1,indicator2=None,indicator3=None,indicator4=None,indicator5=None):
        '''
        获取 公司股本结构变动 LC_ShareStru 数据 最多可以获取5个字段
        '''
        sql = "select CompanyCode,EndDate,InfoPublDate,"+indicator1+" from lc_sharestru\
                where CompanyCode in (SELECT CompanyCode from secumain where  SecuMarket in (83,90) \
                 and SecuCategory=1 )"
        if indicator2 is not None:
            sql = "select CompanyCode,EndDate,InfoPublDate,"+indicator1+","+indicator2+" from lc_sharestru\
                   where CompanyCode in (SELECT CompanyCode from secumain where  SecuMarket in (83,90) \
                   and SecuCategory=1 )" 
        if indicator3 is not None:
            sql = "select CompanyCode,EndDate,InfoPublDate,"+indicator1+","+indicator2+","+indicator3+" from lc_sharestru\
                   where CompanyCode in (SELECT CompanyCode from secumain where  SecuMarket in (83,90) \
                   and SecuCategory=1 )"
        if indicator4 is not None:
            sql = "select CompanyCode,EndDate,InfoPublDate,"+indicator1+","+indicator2+","+indicator3+","+indicator4+"\
                  from lc_sharestru\
                  where CompanyCode in (SELECT CompanyCode from secumain where  SecuMarket in (83,90) \
                  and SecuCategory=1 )"
        if indicator5 is not None:
            sql = "select CompanyCode,EndDate,InfoPublDate,"+indicator1+","+indicator2+","+indicator3+","+indicator4+"\
                   ,"+indicator5+" from lc_sharestru\
                  where CompanyCode in (SELECT CompanyCode from secumain where  SecuMarket in (83,90) \
                  and SecuCategory=1 )"
        ashares =  pd.read_sql(sql,con=self._dbengine)
        return ashares 
    
   
    

   
        
        
        
        
    