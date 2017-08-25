# -*- coding: utf-8 -*-
"""
Created on Tue Jun 14 14:47:03 2016
#营业收入TTM的表，包括三个字段 营业收入TTM原始值、TTM同比、TTM环比、SecuCode
index是日期格式
1. 计算每周末的每个截面数据的以上的值
2. 其中剔除退市、暂停、摘牌等A股的数据
@author: chenghg
"""
from factor0 import *

class 营业收入TTM(init_factor):
   
    def get_营业收入TTM(self,startdate):
        sql = "select InfoPublDate,CompanyCode,EndDate,IfAdjusted, \
                OperatingRevenueTTM  from  LC_FSDerivedData    where \
                AccountingStandards = 1 and EndDate>="+startdate+"" 
        OperatingRevenueTTM = pd.read_sql(sql,con=self._dbengine)
        return OperatingRevenueTTM 
    

if __name__ == "__main__":
    get = 营业收入TTM() #初始化
    ##--参数设置
    databasename = 'test'
    tablename = 'OperatingRevenueTTM_week'
    startdate = '20030101'
    tiemtype = 'w'  #timetype 转为周或者月数据
    
    listedstate = get.get_上市状态变更() #输出被退市和暂停上市的股票
    listedstate = listedstate.sort_values(['InnerCode','ChangeDate'])
    up_date = listedstate[listedstate['ChangeType']==1] #获取上市日期
    info = get.get_info() #获取代码、简称等数据
  
    #从无到有处理因子---------------------------------------------
    TradingDay = get.get_交易日期(startdate)
    week_day = TradingDay.resample(tiemtype,on='TradingDay').last() #获取每周数据
    week_day = week_day.dropna()
    week_day.index = week_day['TradingDay']
    
    OperatingRevenueTTM = get.get_营业收入TTM('20000101')
    #删除空值，某些数据调整后，可能变成空值，导致不连续，需要进行调整
    OperatingRevenueTTM = OperatingRevenueTTM.dropna(subset=['OperatingRevenueTTM'],axis=0)
    OperatingRevenueTTM = pd.merge(OperatingRevenueTTM,info[['InnerCode','CompanyCode','SecuCode']],on='CompanyCode')
    OperatingRevenueTTM = OperatingRevenueTTM.sort_values(['InnerCode','EndDate','InfoPublDate'],ascending=True)
    #只计算上市后，开始公布财报数据的因子值，TTM还是会用到上市前的财报数据，这里进行忽略
    OperatingRevenueTTM = pd.merge(OperatingRevenueTTM,up_date[['InnerCode','ChangeDate']],on='InnerCode',how='left')
    OperatingRevenueTTM = OperatingRevenueTTM[OperatingRevenueTTM['EndDate']>OperatingRevenueTTM['ChangeDate']]
     
    ''' 
    aa = OperatingRevenueTTM_week.groupby(OperatingRevenueTTM_week.index).count()
    aa1 = OperatingRevenueTTM_week[OperatingRevenueTTM_week['营业收入TTM同比'].isnull()]
    aa2  = OperatingRevenueTTM[OperatingRevenueTTM['SecuCode'].isin(aa1['SecuCode'])]
    aa0 = aa2.drop_duplicates(['SecuCode'])
    aa3 = OperatingRevenueTTM[OperatingRevenueTTM['SecuCode']=='002745']
    aa4 = OperatingRevenueTTM_week[OperatingRevenueTTM_week['SecuCode']=='002745']
    aa4['date'] = aa4.index
    aa5 = OperatingRevenueTTM[OperatingRevenueTTM['SecuCode']=='002745']
    aa6 = week_OperatingRevenueTTM[week_OperatingRevenueTTM['SecuCode']=='002745']
    aa7 = listedstate[listedstate['InnerCode']==36719]
    '''
    
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
        now_state = listedstate[i>listedstate['ChangeDate']].drop_duplicates(['InnerCode'],keep='last')
        now_state = now_state[now_state['ChangeType'].isin((1,3))] 
        week_OperatingRevenueTTM = week_OperatingRevenueTTM[week_OperatingRevenueTTM['InnerCode'].isin(now_state['InnerCode'])]
        #------------------------------
        week_OperatingRevenueTTM['营业收入TTM'] = week_OperatingRevenueTTM['OperatingRevenueTTM']
        week_OperatingRevenueTTM['dt'] = datetime.datetime.strftime(i,'%Y%m%d')
        week_OperatingRevenueTTM = week_OperatingRevenueTTM[['dt','SecuCode','营业收入TTM','营业收入TTM同比','营业收入TTM环比']]
        OperatingRevenueTTM_week = OperatingRevenueTTM_week.append(week_OperatingRevenueTTM)
    
    #-------创建表，并插入数据------------------------------------------------------------------
    data_structure = "( id int  not null primary key AUTO_INCREMENT,\
                        dt datetime,   \
                        SecuCode varchar(6),  \
                        营业收入TTM float, \
                        营业收入TTM同比 float,\
                        营业收入TTM环比 float)"
    names = "(dt,SecuCode,营业收入TTM,营业收入TTM同比,营业收入TTM环比)"
    
    get.create_newdata(databasename,tablename,data_structure) #创建表
    get.insert_data(databasename,tablename,names,OperatingRevenueTTM_week) #插入数据
    
    
   
        
        
        
        
    