# -*- coding: utf-8 -*-
"""
Created on Wed Aug  9 15:42:33 2017
PE数据 周度
@author: dylan
"""

from factor0 import *
import datetime

class PE_func(init_factor):
   
    def get_quote(self,startdate):
        '''
        获取聚源数据库非复权行情数据
        '''
        #startdate = '20170101'
        #enddate = '20170115'
        sql = "select b.SecuCode,a.TradingDay,a.closeprice as cp from qt_dailyquote a\
                inner join (select * from  secumain where  SecuMarket in (83,90) and SecuCategory=1) as b\
                on a.InnerCode=b.innercode where TradingDay > STR_TO_DATE("+startdate+", '%Y%m%d') "
        quote = pd.read_sql(sql,con=self._dbengine)
        return quote

    def get_股本变动(self):
        sql = "select CompanyCode,EndDate,InfoPublDate,TotalShares from lc_sharestru\
                where CompanyCode in (SELECT CompanyCode from secumain where  SecuMarket in (83,90) \
                 and SecuCategory=1 )"
        ashares =  pd.read_sql(sql,con=self._dbengine)
        return ashares 
    
    def get_净利润TTM(self,startdate):
        sql = "select InfoPublDate,CompanyCode,EndDate,IfAdjusted, \
            NPDeductNonRecurringPL as 扣非归母净利润,NPParentCompanyOwnersTTM as 归母净利润TTM\
            from  LC_FSDerivedData    where AccountingStandards = 1 and EndDate>="+startdate+"" 
        netprofit = pd.read_sql(sql,con=self._dbengine)
        return netprofit 
    
    def get_利润表(self,startdate):
        #获取利润表
        sql = "select InfoPublDate,BulletinType,CompanyCode,EndDate,IfAdjusted,EnterpriseType,\
                NPParentCompanyOwners as '归属母公司的净利润' from  LC_IncomeStatementAll   where \
                AccountingStandards = 1 and  IfMerged=1 and EndDate>=STR_TO_DATE("+startdate+", '%Y%m%d')"
        cum_profit = pd.read_sql(sql,con=self._dbengine)
        return cum_profit

if __name__ == "__main__":
    get = PE_func() #初始化
    ##--参数设置
    databasename = 'test'
    tablename = 'pettm_week'
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
    
    netprofit = get.get_净利润TTM('20000101')
    cum_profit = get.get_利润表('20000101')
    ashares = get.get_股本变动()
    quote = get.get_quote(startdate)

    #只计算上市后，开始公布财报数据的因子值，TTM还是会用到上市前的财报数据，这里进行忽略
    ashares = get.finance_getinfo_rank(ashares,info,up_date)
    netprofit = get.finance_getinfo_rank(netprofit,info,up_date)
    cum_profit = get.finance_getinfo_rank(cum_profit,info,up_date)


    pe_week = pd.DataFrame()
    for i in week_day.index:
        print(i)
        temp_quote = quote[quote['TradingDay']==i]
        temp_ashares = ashares[i>=ashares['InfoPublDate']]
        temp_ashares = temp_ashares.drop_duplicates(['CompanyCode'],keep='last')
        
        pretime = datetime.datetime(i.year-2,i.month,1)   
        temp_profit = netprofit[(i>=netprofit['InfoPublDate'])&(netprofit['InfoPublDate']>pretime)]
        temp_profit = temp_profit.drop_duplicates(['InnerCode','EndDate'],keep='last')
        temp_profit = get.finance_cum_to_ttm(temp_profit,'扣非归母净利润')
        temp_profit = temp_profit.drop_duplicates(['CompanyCode'],keep='last')
        
        temp_cumprofit = cum_profit[(i>=cum_profit['InfoPublDate'])]
        temp_cumprofit = temp_cumprofit.drop_duplicates(['CompanyCode'],keep='last')
        
        #只记录当前处于正常上市状态和恢复上市的股票------------------------------
        now_state = listedstate[i>listedstate['ChangeDate']].drop_duplicates(['InnerCode'],keep='last')
        now_state = now_state[now_state['ChangeType'].isin((1,3))] 
        temp_profit = temp_profit[temp_profit['InnerCode'].isin(now_state['InnerCode'])]
        temp_cumprofit = temp_cumprofit[temp_cumprofit['InnerCode'].isin(now_state['InnerCode'])]
      
        temp_quote = pd.merge(temp_quote,temp_profit[['SecuCode','扣非归母净利润TTM','归母净利润TTM','month']],on=['SecuCode'])
        temp_quote = pd.merge(temp_quote,temp_ashares[['SecuCode','Ashares']],on=['SecuCode'],how='left')
        temp_quote = pd.merge(temp_quote,temp_cumprofit[['SecuCode','归属母公司的净利润']],on=['SecuCode'],how='left')
        temp_quote['pe_float'] =  np.where(temp_quote['month']==3,0.25*temp_quote['Ashares']* temp_quote['cp']/ temp_quote['归属母公司的净利润'],
                                   np.where(temp_quote['month']==6,0.5*temp_quote['Ashares']* temp_quote['cp']/ temp_quote['归属母公司的净利润'],
                                     np.where(temp_quote['month']==9,4/3*temp_quote['Ashares']* temp_quote['cp']/ temp_quote['归属母公司的净利润'],
                                       temp_quote['Ashares']* temp_quote['cp']/ temp_quote['归属母公司的净利润'])))
        #------------------------------
        temp_quote['pettm'] = temp_quote['Ashares']* temp_quote['cp']/ temp_quote['归母净利润TTM']
        temp_quote['pettm_cut'] = temp_quote['Ashares']* temp_quote['cp']/ temp_quote['扣非归母净利润TTM']
        temp_quote['dt'] = datetime.datetime.strftime(i,'%Y%m%d')
        temp_quote = temp_quote[['dt','SecuCode','pettm','pettm_cut','pe_float']]
        pe_week = pe_week.append(temp_quote)
    pe_week['pettm'] = np.where(pd.isnull(pe_week['pettm'])!=True, pe_week['pettm'], None)
    pe_week['pettm_cut'] = np.where(pd.isnull(pe_week['pettm_cut'])!=True, pe_week['pettm_cut'], None)
    pe_week['pe_float'] = np.where(pd.isnull(pe_week['pe_float'])!=True, pe_week['pe_float'], None)
    
    #-------创建表，并插入数据------------------------------------------------------------------
    data_structure = "( id int  not null primary key AUTO_INCREMENT,\
                        dt datetime,   \
                        SecuCode varchar(6),  \
                        pettm float, \
                        pettm_cut float,\
                        pe_float float)"
    names = "(dt,SecuCode,pettm,pettm_cut,pe_float)"
    
    get.create_newdata(databasename,tablename,data_structure) #创建表
    get.insert_data(databasename,tablename,names,pe_week) #插入数据
    
    
   
        
        
        
        
    