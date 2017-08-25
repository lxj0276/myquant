# -*- coding: utf-8 -*-
"""
Created on Tue Jun 14 14:47:03 2016
#营业收入TTM的表，包括三个字段 营业收入TTM原始值、TTM同比、TTM环比、SecuCode
index是日期格式
1. 计算每周末的每个截面数据的以上的值
2. 其中剔除退市、暂停、摘牌等A股的数据
@author: chenghg
"""
from 营业收入TTM import *
import datetime
    
class 营业收入TTM更新(营业收入TTM):
    pass

if __name__ == "__main__":
    get = 营业收入TTM更新() #初始化
    ##--参数设置
    databasename = 'test'
    tablename = 'OperatingRevenueTTM_week'
    tiemtype = 'w'  #timetype 转为周或者月数据
    
    #获取数据库最后的更新日期--------------------------------
    data = get.get_最近的数据(databasename,tablename)
    maxdt =  data['dt'].max()
    lag3year =  maxdt - datetime.timedelta(1400) #近3年数据
    maxdt = datetime.datetime.strftime(maxdt,'%Y%m%d')
    lag3year = datetime.datetime.strftime(lag3year,'%Y%m%d')
    
    listedstate = get.get_上市状态变更() #输出被退市和暂停上市的股票
    up_date = listedstate[listedstate['ChangeType']==1] #获取上市日期
    listedstate = listedstate.sort_values(['InnerCode','ChangeDate'])
    info = get.get_info() #获取代码、简称等数据
  
    #从无到有处理因子---------------------------------------------
    TradingDay = get.get_交易日期(maxdt)
    week_day = TradingDay.resample(tiemtype,on='TradingDay').last() #获取每周数据
    week_day = week_day.dropna()
    week_day.index = week_day['TradingDay']
    
    OperatingRevenueTTM = get.get_营业收入TTM(lag3year) #取最近3年的数据
    #删除空值，某些数据调整后，可能变成空值，导致不连续，需要进行调整
    OperatingRevenueTTM = OperatingRevenueTTM.dropna(subset=['OperatingRevenueTTM'],axis=0)
    OperatingRevenueTTM = pd.merge(OperatingRevenueTTM,info[['InnerCode','CompanyCode','SecuCode']],on='CompanyCode')
    OperatingRevenueTTM = OperatingRevenueTTM.sort_values(['InnerCode','EndDate','InfoPublDate'],ascending=True)
    #只计算上市后，开始公布财报数据的因子值，TTM还是会用到上市前的财报数据，这里进行忽略
    OperatingRevenueTTM = pd.merge(OperatingRevenueTTM,up_date[['InnerCode','ChangeDate']],on='InnerCode',how='left')
    OperatingRevenueTTM = OperatingRevenueTTM[OperatingRevenueTTM['EndDate']>OperatingRevenueTTM['ChangeDate']]
     
    OperatingRevenueTTM_week = pd.DataFrame()
    for i in week_day.index:
        print(i)
        pretime = datetime.datetime(i.year-2,i.month,1)   
        week_OperatingRevenueTTM = OperatingRevenueTTM[(i>=OperatingRevenueTTM['InfoPublDate'])&(OperatingRevenueTTM['InfoPublDate']>pretime)]
        week_OperatingRevenueTTM = week_OperatingRevenueTTM.drop_duplicates(['InnerCode','EndDate'],keep='last')
        week_OperatingRevenueTTM['month'] = week_OperatingRevenueTTM['EndDate'].apply(lambda x:x.month)
        week_OperatingRevenueTTM['year'] = week_OperatingRevenueTTM['EndDate'].apply(lambda x:x.year)

        week_OperatingRevenueTTM['营业收入TTM环比'] = np.where((week_OperatingRevenueTTM['InnerCode']==week_OperatingRevenueTTM['InnerCode'].shift(1))&
                                                                (week_OperatingRevenueTTM['OperatingRevenueTTM'].shift(1)!=0),
                                                              week_OperatingRevenueTTM['OperatingRevenueTTM']/week_OperatingRevenueTTM['OperatingRevenueTTM'].shift(1)-1,None)
        week_OperatingRevenueTTM['营业收入TTM同比'] = np.where((week_OperatingRevenueTTM['InnerCode']==week_OperatingRevenueTTM['InnerCode'].shift(4))
                                                                &(week_OperatingRevenueTTM['month']==week_OperatingRevenueTTM['month'].shift(4))&
                                                                (week_OperatingRevenueTTM['year']==week_OperatingRevenueTTM['year'].shift(4)+1)&
                                                                (week_OperatingRevenueTTM['OperatingRevenueTTM'].shift(4)!=0),
                                                                week_OperatingRevenueTTM['OperatingRevenueTTM']/week_OperatingRevenueTTM['OperatingRevenueTTM'].shift(4)-1,None)
        
        week_OperatingRevenueTTM = week_OperatingRevenueTTM.drop_duplicates(['InnerCode'],keep='last')
        
        #只记录当前处于正常上市状态和恢复上市的股票------------------------------
        now_state = listedstate[i>listedstate['ChangeDate']].drop_duplicates(['InnerCode'])
        now_state = now_state[now_state['ChangeType'].isin((1,3))] 
        week_OperatingRevenueTTM = week_OperatingRevenueTTM[week_OperatingRevenueTTM['InnerCode'].isin(now_state['InnerCode'])]
        #------------------------------
        week_OperatingRevenueTTM['营业收入TTM'] = week_OperatingRevenueTTM['OperatingRevenueTTM']
        week_OperatingRevenueTTM['dt'] = datetime.datetime.strftime(i,'%Y%m%d')
        week_OperatingRevenueTTM = week_OperatingRevenueTTM[['dt','SecuCode','营业收入TTM','营业收入TTM同比','营业收入TTM环比']]
        OperatingRevenueTTM_week = OperatingRevenueTTM_week.append(week_OperatingRevenueTTM)
    
    
    #----删除数据中最后一天的数据，以防止该天的数据不是每周的最后一天------------------------------
    data_structure = "( id int  not null primary key AUTO_INCREMENT,\
                        dt datetime,   \
                        SecuCode varchar(6),  \
                        营业收入TTM float, \
                        营业收入TTM同比 float,\
                        营业收入TTM环比 float)"
    names = "(dt,SecuCode,营业收入TTM,营业收入TTM同比,营业收入TTM环比)"
    
    get.update_data(databasename,tablename,data) #更新数据
    get.insert_data(databasename,tablename,names,OperatingRevenueTTM_week) #插入更新数据
  
   
        
        
        
        
    