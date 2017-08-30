# -*- coding: utf-8 -*-
"""
Created on Wed Jan 11 16:52:33 2017

@author: chenghg
"""

import numpy as np
import pandas as pd
import pymysql
import datetime

class get_quote:
    '''
    聚源数据库
    提取\更新全市场行情数据，包括复权因子、申万行业、成交量、总股本、流通股本等
    '''
    
    def __init__(self):
        self._dbengine1 =  pymysql.connect(host='backtest.invesmart.net',
                           port=3308,
                           user='jydb',
                           password='Jydb@123465',
                           database='jydb',
                           charset='gbk')
    
    def __del__(self):
        self._dbengine1.close()

    #-------------------------------获取聚源数据--------------------------------------------- 
    def get_shares(self):
        '''
        获取流通股本数据
        '''
        sql = "select CompanyCode,EndDate,Ashares,AFloats from LC_ShareStru"
        shares = pd.read_sql(sql,con=self._dbengine1)
        return shares
    
    def get_equityquote(self,startdate,enddate):
        #获取行情数据,复权价格【比例法】,并获得股票简称历次变更名称【用的是公告日数据，即公告将ST，既是还没正式改名，这边也先记录ST】
        #而股本的变动也是一样，取日期大于等于公告日和变更日的最新一条数据
        sql = "select B.SecuCode,(select SecurityAbbr from  LC_SecuChange as o2 where o2.InfoPublDate<=A.TradingDay \
                and o2.InnerCode=A.innercode ORDER BY o2.InfoPublDate desc limit 1) as SecuAbbr ,B.InnerCode,B.CompanyCode,\
                A.TradingDay,A.OpenPrice as op,A.ClosePrice as cp,A.HighPrice as hp,A.LowPrice as lp,\
                A.PrevClosePrice as precp,(A.ClosePrice * ifnull((select AdjustingFactor from QT_AdjustingFactor as O1 where  \
                O1.ExDiviDate <= A.TradingDay and O1.InnerCode = A.InnerCode order by O1.ExDiviDate desc limit 1),1))  as fq_cp,\
                A.TurnoverVolume as vol,A.TurnoverValue,A.TurnoverDeals,(select Ashares from lc_sharestru as o3 where \
                o3.InfoPublDate<=A.TradingDay and o3.EndDate<=A.TradingDay and o3.CompanyCode=B.CompanyCode order by enddate \
                desc limit 1) as Ashares,(select AFloats from lc_sharestru as o3 where \
                o3.InfoPublDate<=A.TradingDay and o3.EndDate<=A.TradingDay and o3.CompanyCode=B.CompanyCode order by enddate \
                desc limit 1) as AFloats from QT_DailyQuote  A \
                inner join SecuMain B   on A.InnerCode = B.InnerCode   and  B.SecuCategory=1 and\
                B.SecuMarket in (83,90)  where TradingDay >STR_TO_DATE("+startdate+",'%Y%m%d')   and\
                 TradingDay <=STR_TO_DATE("+enddate+",'%Y%m%d') order by TradingDay desc "
        quote = pd.read_sql(sql,con=self._dbengine1)
        return quote
    
    def get_indexquote(self,startdate,enddate):
        '''
        获取指数行情，包括综合指数和申万、中信行业指数，三大类
        '''
        
        sql = "select d.Secucode,d.SecuAbbr,c.TradingDay,c.ClosePrice as cp from QT_IndexQuote c\
                INNER JOIN (select b.Secucode,b.SecuAbbr,b.innercode,a.pubdate as ListedDate from LC_IndexBasicInfo as a \
                inner join (select * from secumain where secucategory in (4) and secumarket in (83,90) and listedstate=1)as b\
                on a.IndexCode=b.innercode where (IndexType in (10,47) or (IndexType=30 and industrystandard in (3,9,24)))) as d\
                on c.innercode=d.innercode where TradingDay >STR_TO_DATE("+startdate+",'%Y%m%d')   and\
                 TradingDay <=STR_TO_DATE("+enddate+",'%Y%m%d') order by TradingDay desc "
        indexquote = pd.read_sql(sql,con=self._dbengine1)
        return indexquote
  
    
    def get_bonus(self):
        '''
        获取分红、送股数据
        '''
        sql =  "select a.SecuCode,a.InnerCode,a.CompanyCode,b.IfDividend,b.Enddate,b.AdvanceDate,\
                b.RightRegDate,b.ExDiviDate,b.BonusShareRatio,b.TranAddShareRaio,b.CashDiviRMB \
                from secumain a inner join LC_Dividend b on a.InnerCode=b.InnerCode \
                 where a.SecuMarket in (83,90) and a.SecuCategory =1"
        bonus = pd.read_sql(sql,con=self._dbengine1)
        return bonus
    
    def new_data(self,dataname,func):
        '''
        从无到有提取数据
        dataname:数据名“quote”
        func: get_quote 或者 get_indexquoe
        '''
        quote = func('19900101','19991231') 
        quote01 = func('19991231','20091231')
        quote02 = func('20091231','20191231')
        quote = quote.append(quote01)
        quote = quote.append(quote02)  
        
        #quote = func('20091231','20191231')
        #store = pd.HDFStore("C:\\py_data\\datacenter\\quote.h5",'w')
        quote.to_hdf("C:\\py_data\\datacenter\\quote.h5",key='%s'%dataname,format='table',mode='a',data_columns=quote.columns)
        
    
    def updata_quote(self,dataname,func):
        '''
        更新行情数据
        '''
        enddate = '20990101'
        dt = pd.read_hdf("C:\\py_data\\datacenter\\quote.h5",'%'%dataname,columns=['TradingDay'],where='SecuCode="000001"')     
        startdate = dt['TradingDay'].max()
        startdate = datetime.datetime.strftime(startdate,"%Y%m%d")
        enddate = "20200101"
        new_quote = func(startdate,enddate) #提取更新数据
        quote.to_hdf("C:\\py_data\\datacenter\\quote.h5",key='%s'%dataname,format='table',mode='r+',data_columns=quote.columns,append=True)

        
    #------------------------获取wind数据--------------------------------------------------------
    
    
if __name__ == '__main__':
     get =  get_quote()
     
    
         
     #当且仅当从头开始提取数据时运行，否则运行
     get.new_data('equity_quote',get.get_equityquote) #提取股票行情
     get.new_data('index_quote',get.get_indexquote) #提取指数行情
     
     get.updata_quote('equity_quote',get.get_equityquote)#更新股票程序
     get.updata_quote('index_quote',get.get_indexquote) #更新指数行情程序
  
     
#     import time
#     t1 = time.time()
#     bb = pd.read_hdf("C:\\py_data\\datacenter\\test.h5",where='TradingDay>=20161130 & TradingDay<=20161208')
#     t2 = time.time()
#     print(t2-t1)
    
     equity_bonus = get.get_bonus()
     equity_bonus.to_hdf("C:\\py_data\\datacenter\\quote.h5",key='bonus',format='table',mode='a',data_columns=equity_bonus.columns)
      