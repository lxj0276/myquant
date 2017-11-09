# -*- coding: utf-8 -*-
"""
Created on Thu Aug 10 14:51:38 2017
PB数据，月度，父类为：PE数据.py
@author: dylan
"""
from factor0 import *
 

class PB_func(init_factor):
    pass
   
    

if __name__ == "__main__":
    get = PB_func() #初始化
    ##--参数设置
    databasename = 'test'
    tablename = 'pb'
    cycle_type = 'w'  #timetype 转为周或者月数据
    startdate = '20130101'
    startdate1 = str(int(startdate)-10000) #提取财务数据时用
    
    listedstate = get.get_上市状态变更() #输出被退市和暂停上市的股票
    listedstate = listedstate.sort_values(['InnerCode','ChangeDate'])
    info = get.get_info() #获取代码、简称、上市日期等数据
  
    #从无到有处理因子---------------------------------------------
    TradingDay = get.get_交易日期(startdate)
    week_day = TradingDay.resample(cycle_type,on='TradingDay').last() #获取每周数据
    week_day = week_day.dropna()
    week_day.index = week_day['TradingDay']
    
    asset = get.get_财务表('LC_BalanceSheetAll',startdate1,'SEWithoutMI')#取净利润
    ashares = get.get_股本表('TotalShares','Ashares','AFloats','NonResiSharesJY') #取股本表
    quote = get.get_行情(startdate) #取行情数据

    #只计算上市后，开始公布财报数据的因子值，TTM还是会用到上市前的财报数据，这里进行忽略
    ashares = get.finance_getinfo_rank(ashares,info)
    asset = get.finance_getinfo_rank(asset,info)


    pb_week = pd.DataFrame()
    for i in week_day.index:
        print(i)
        temp_quote = quote[quote['TradingDay']==i]
        temp_ashares = ashares[(i>=ashares['InfoPublDate'])&(i>=ashares['EndDate'])]
        temp_ashares = temp_ashares.drop_duplicates(['CompanyCode'],keep='last')
        
        #净资产数据
        temp_asset = asset[(i>=asset['InfoPublDate'])]
        temp_asset = temp_asset.drop_duplicates(['CompanyCode'],keep='last')
        
        #只记录当前处于正常上市状态和恢复上市的股票------------------------------
        now_state = listedstate[i>listedstate['ChangeDate']].drop_duplicates(['InnerCode'],keep='last')
        now_state = now_state[now_state['ChangeType'].isin((1,3))] 
        temp_asset = temp_asset[temp_asset['InnerCode'].isin(now_state['InnerCode'])]
        
        #计算PB
        temp_quote = pd.merge(temp_quote,temp_asset[['SecuCode','SEWithoutMI']],on=['SecuCode'])
        temp_quote = pd.merge(temp_quote,temp_ashares[['SecuCode','TotalShares','Ashares','AFloats','NonResiSharesJY']],on=['SecuCode'])  
        temp_quote['总市值'] = temp_quote['TotalShares']* temp_quote['cp']
        temp_quote['pb'] =  temp_quote['总市值']/ temp_quote['SEWithoutMI']
        temp_quote['A股总市值'] = temp_quote['Ashares']* temp_quote['cp']
        temp_quote['A股流通市值'] = temp_quote['AFloats']* temp_quote['cp']
        temp_quote['A股自由流通市值'] = temp_quote['NonResiSharesJY']* temp_quote['cp']
        temp_quote['dt'] = datetime.datetime.strftime(i,'%Y%m%d')       
        pb_week = pb_week.append(temp_quote)

    pb_week['pb'] = np.where(pd.isnull(pb_week['pb'])!=True, pb_week['pb'], None)
    pb_week['总市值'] = np.where(pd.isnull(pb_week['总市值'])!=True, pb_week['总市值'], None)
    pb_week['A股总市值'] = np.where(pd.isnull(pb_week['A股总市值'])!=True, pb_week['A股总市值'], None)
    pb_week['A股流通市值'] = np.where(pd.isnull(pb_week['A股流通市值'])!=True, pb_week['A股流通市值'], None)
    pb_week['A股自由流通市值'] = np.where(pd.isnull(pb_week['A股自由流通市值'])!=True, pb_week['A股自由流通市值'], None)
    get.to_查错(pb_week,"C:/Users/dylan/Desktop/慧网工作/股票量化策略/多因子计算/因子纠错/pb&总市值&流通市值查错.xlsx")
    pb_week = pb_week[['dt','SecuCode','pb','总市值','A股总市值','A股流通市值','A股自由流通市值']]
   
    #--创建新的数据表，并插入数据
    data_structure = "( id bigint  not null primary key AUTO_INCREMENT,\
                        TradingDay datetime,   \
                        SecuCode varchar(6),  \
                        pb double,\
                        总市值 double,\
                        A股总市值 double,\
                        A股流通市值 double,\
                        A股自由流通市值 double)"
    names = "(TradingDay,SecuCode,pb,总市值,A股总市值,A股流通市值,A股自由流通市值)"
    
    get.create_newdata(databasename,tablename,data_structure) #创建表
    get.insert_data(databasename,tablename,names,pb_week) #插入数据
    
    
   
        
        
        
        
    