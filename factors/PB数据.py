# -*- coding: utf-8 -*-
"""
Created on Thu Aug 10 14:51:38 2017
PB数据，月度，父类为：PE数据.py
@author: dylan
"""
from PE数据 import *

class PB_func(PE_func):
   
     def get_资产负债表(self,startdate):
        #获取利润表
        sql = "select InfoPublDate,BulletinType,CompanyCode,EndDate,IfAdjusted,EnterpriseType,\
                SEWithoutMI as '归属母公司的权益' from  LC_BalanceSheetAll    where \
                AccountingStandards = 1 and  IfMerged=1 and EndDate>=STR_TO_DATE("+startdate+", '%Y%m%d')"
        asset = pd.read_sql(sql,con=self._dbengine)
        return asset
    

if __name__ == "__main__":
    get = PB_func() #初始化
    ##--参数设置
    databasename = 'test'
    tablename = 'pb_week'
    startdate = '20030101'
    tiemtype = 'm'  #timetype 转为周或者月数据
    
    listedstate = get.get_上市状态变更() #输出被退市和暂停上市的股票
    listedstate = listedstate.sort_values(['InnerCode','ChangeDate'])
    up_date = listedstate[listedstate['ChangeType']==1] #获取上市日期
    info = get.get_info() #获取代码、简称等数据
  
    #从无到有处理因子---------------------------------------------
    TradingDay = get.get_交易日期(startdate)
    week_day = TradingDay.resample(tiemtype,on='TradingDay').last() #获取每周数据
    week_day = week_day.dropna()
    week_day.index = week_day['TradingDay']
    
    asset = get.get_资产负债表('20000101')
    ashares = get.get_股本变动()
    quote = get.get_quote(startdate)

    #只计算上市后，开始公布财报数据的因子值，TTM还是会用到上市前的财报数据，这里进行忽略
    ashares = get.finance_getinfo_rank(ashares,info,up_date)
    asset = get.finance_getinfo_rank(asset,info,up_date)


    pb_week = pd.DataFrame()
    for i in week_day.index:
        print(i)
        temp_quote = quote[quote['TradingDay']==i]
        temp_ashares = ashares[i>=ashares['InfoPublDate']]
        temp_ashares = temp_ashares.drop_duplicates(['CompanyCode'],keep='last')
        
        #净资产数据
        temp_asset = asset[(i>=asset['InfoPublDate'])]
        temp_asset = temp_asset.drop_duplicates(['CompanyCode'],keep='last')
        
        #只记录当前处于正常上市状态和恢复上市的股票------------------------------
        now_state = listedstate[i>listedstate['ChangeDate']].drop_duplicates(['InnerCode'],keep='last')
        now_state = now_state[now_state['ChangeType'].isin((1,3))] 
        temp_asset = temp_asset[temp_asset['InnerCode'].isin(now_state['InnerCode'])]
        
        #计算PB
        temp_quote = pd.merge(temp_quote,temp_asset[['SecuCode','归属母公司的权益']],on=['SecuCode'])
        temp_quote = pd.merge(temp_quote,temp_ashares[['SecuCode','TotalShares']],on=['SecuCode'])  
        temp_quote['pb'] = temp_quote['TotalShares']* temp_quote['cp']/ temp_quote['归属母公司的权益']
        temp_quote['dt'] = datetime.datetime.strftime(i,'%Y%m%d')
        temp_quote = temp_quote[['dt','SecuCode','pb']]
        pb_week = pb_week.append(temp_quote)
    pb_week['pb'] = np.where(pd.isnull(pb_week['pb'])!=True, pb_week['pb'], None)
   
    
    #-------创建表，并插入数据------------------------------------------------------------------
    data_structure = "( id int  not null primary key AUTO_INCREMENT,\
                        dt datetime,   \
                        SecuCode varchar(6),  \
                        pb float)"
    names = "(dt,SecuCode,pb)"
    
    get.create_newdata(databasename,tablename,data_structure) #创建表
    get.insert_data(databasename,tablename,names,pb_week) #插入数据
    
    
   
        
        
        
        
    