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
        self.datapath  = "C:\\py_data\\datacenter\\quote.h5"

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
    
    def get_衍生表(self,startdate,indicator1,indicator2=None,indicator3=None,indicator4=None,indicator5=None):
        '''
        公司衍生报表数据_新会计准则（新）数据 LC_FSDerivedData,最多可以同时提取5个指标
        '''
        sql = "select InfoPublDate,CompanyCode,EndDate,IfAdjusted, \
                "+indicator1+" from  LC_FSDerivedData    where \
                AccountingStandards = 1 and InfoPublDate>="+startdate+"" 
        if indicator2 is not None:
            sql = "select InfoPublDate,CompanyCode,EndDate,IfAdjusted, \
                    "+indicator1+" ,"+indicator2+" from  LC_FSDerivedData    where \
                    AccountingStandards = 1 and InfoPublDate>="+startdate+"" 
        if  indicator3 is not None:
            sql = "select InfoPublDate,CompanyCode,EndDate,IfAdjusted, \
                    "+indicator1+" ,"+indicator2+","+indicator3+" from  LC_FSDerivedData    where \
                    AccountingStandards = 1 and InfoPublDate>="+startdate+"" 
        if indicator4 is not None:
            sql = "select InfoPublDate,CompanyCode,EndDate,IfAdjusted, \
                    "+indicator1+" ,"+indicator2+","+indicator3+","+indicator4+" from  LC_FSDerivedData    where \
                    AccountingStandards = 1 and InfoPublDate>="+startdate+"" 
        if indicator5 is not None:
            sql = "select InfoPublDate,CompanyCode,EndDate,IfAdjusted, \
                    "+indicator1+" ,"+indicator2+","+indicator3+","+indicator4+","+indicator5+" from  LC_FSDerivedData    where \
                    AccountingStandards = 1 and InfoPublDate>="+startdate+""   
        
        data = pd.read_sql(sql,con=self._dbengine)
        return data
    
    def get_财务表(self,sheetname,startdate,indicator1,indicator2=None,indicator3=None,indicator4=None,indicator5=None):
        '''
        sheetname:
        1.利润分配表_新会计准则 LC_IncomeStatementAll,最多可以同时提取5个指标
        2. 资产负债表_新会计准则 LC_BalanceSheetAll
        3. 现金流量表_新会计准则 LC_CashFlowStatementAll
        '''
                    
        sql = "select InfoPublDate,CompanyCode,EndDate,IfAdjusted, EnterpriseType as CompanyType,\
                "+indicator1+" from  "+sheetname+"    where \
                AccountingStandards = 1 and IfMerged=1 and InfoPublDate>="+startdate+"" 
        if indicator2 is not None:
            sql = "select InfoPublDate,CompanyCode,EndDate,IfAdjusted,EnterpriseType as CompanyType, \
                    "+indicator1+" ,"+indicator2+" from  "+sheetname+"    where \
                    AccountingStandards = 1 and IfMerged=1 and InfoPublDate>="+startdate+"" 
        if indicator3 is not None:
            sql = "select InfoPublDate,CompanyCode,EndDate,IfAdjusted,EnterpriseType as CompanyType, \
                    "+indicator1+" ,"+indicator2+","+indicator3+" from  "+sheetname+"    where \
                    AccountingStandards = 1 and IfMerged=1 and InfoPublDate>="+startdate+"" 
        if indicator4 is not None:
            sql = "select InfoPublDate,CompanyCode,EndDate,IfAdjusted,EnterpriseType as CompanyType, \
                    "+indicator1+" ,"+indicator2+","+indicator3+","+indicator4+" from  "+sheetname+"   where \
                    AccountingStandards = 1 and IfMerged=1 and InfoPublDate>="+startdate+"" 
        if indicator5 is not None:
            sql = "select InfoPublDate,CompanyCode,EndDate,IfAdjusted,EnterpriseType as CompanyType, \
                    "+indicator1+" ,"+indicator2+","+indicator3+","+indicator4+","+indicator5+" from "+sheetname+"   where \
                    AccountingStandards = 1 and IfMerged=1 and InfoPublDate>="+startdate+""             
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
                AccountingStandards = 1 and Mark in (1,2) and InfoPublDate>="+startdate+"" 
        if indicator2 is not None:
            sql = "select InfoPublDate,CompanyCode,EndDate,Mark, CompanyType, \
                    "+indicator1+" ,"+indicator2+" from  "+sheetname+"    where \
                    AccountingStandards = 1 and Mark in (1,2) and InfoPublDate>="+startdate+"" 
        if indicator3 is not None:
            sql = "select InfoPublDate,CompanyCode,EndDate,Mark, CompanyType, \
                    "+indicator1+" ,"+indicator2+","+indicator3+" from  "+sheetname+"    where \
                    AccountingStandards = 1 and Mark in (1,2) and InfoPublDate>="+startdate+"" 
        if indicator4 is not None:
            sql = "select InfoPublDate,CompanyCode,EndDate,Mark, CompanyType, \
                    "+indicator1+" ,"+indicator2+","+indicator3+","+indicator4+" from  "+sheetname+"   where \
                    AccountingStandards = 1 and Mark in (1,2) and InfoPublDate>="+startdate+"" 
        if indicator5 is not None:
            sql = "select InfoPublDate,CompanyCode,EndDate,Mark, CompanyType, \
                    "+indicator1+" ,"+indicator2+","+indicator3+","+indicator4+","+indicator5+" from "+sheetname+"   where \
                    AccountingStandards = 1 and Mark in (1,2) and InfoPublDate>="+startdate+""             
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
    
   
    

   
        
        
        
        
    