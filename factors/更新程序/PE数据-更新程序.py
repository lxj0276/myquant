# -*- coding: utf-8 -*-
"""
Created on Wed Aug  9 15:42:33 2017
PE数据 周度
@author: dylan
"""

from factor0 import *

class PE_func_update(init_factor):
    pass

if __name__ == "__main__":
    get = PE_func_update() #初始化
    ##--参数设置
    databasename = 'test'
    tablename = 'pe'
    cycle_type = 'm'  #timetype 转为周或者月数据
    
     #获取数据库最后的更新日期--------------------------------
    last_data = get.get_最近的数据(databasename,tablename)
    startdate = last_data['TradingDay'].max()
    startdate = datetime.datetime.strftime(startdate,'%Y%m%d')
    startdate1 = str(int(startdate)-10000)
    
    
    listedstate = get.get_上市状态变更() #输出被退市和暂停上市的股票
    listedstate = listedstate.sort_values(['InnerCode','ChangeDate'])
    up_date = listedstate[listedstate['ChangeType']==1] #获取上市日期
    info = get.get_info() #获取代码、简称等数据
  
    #从无到有处理因子---------------------------------------------
    TradingDay = get.get_交易日期(startdate)
    week_day = TradingDay.resample(cycle_type,on='TradingDay').last() #获取每周数据
    week_day.index = week_day['TradingDay']
    
    cum_profit = get.get_财务表('LC_IncomeStatementAll',startdate1,'NPParentCompanyOwners','OtherNetRevenue')#取净利润
    cum_profit['NPDeductNonRecurringPL'] = np.where(pd.isnull(cum_profit['OtherNetRevenue'])==False,
              cum_profit['NPParentCompanyOwners']-cum_profit['OtherNetRevenue'],cum_profit['NPParentCompanyOwners'])
    TotalShares = get.get_股本表('TotalShares') #取股本表
    quote = get.get_行情(startdate) #取行情数据

    #只计算上市后，开始公布财报数据的因子值，TTM还是会用到上市前的财报数据，这里进行忽略
    TotalShares = get.finance_getinfo_rank(TotalShares,info)
    cum_profit = get.finance_getinfo_rank(cum_profit,info)
    print('数据提取完毕...')
    ##------获取每一期的因子数据--------------------------------------------------------------------
    pe_week = pd.DataFrame()
    for i in week_day.index:
        print(i)
        temp_quote = quote[quote['TradingDay']==i]
        temp_TotalShares = TotalShares[(i>=TotalShares['InfoPublDate'])&(i>=TotalShares['EndDate'])]
        temp_TotalShares = temp_TotalShares.drop_duplicates(['CompanyCode'],keep='last')
        
        pretime = datetime.datetime(i.year-2,i.month,1)   
        temp_profit = cum_profit[(i>=cum_profit['InfoPublDate'])&(cum_profit['InfoPublDate']>pretime)]
        temp_profit = temp_profit.drop_duplicates(['InnerCode','EndDate'],keep='last')
        temp_profit['NPParentCompanyOwnersTTM'] =  get.get_ttm(temp_profit,'NPParentCompanyOwners')
        temp_profit['NPDeductNonRecurringPLTTM'] = get.get_ttm(temp_profit,'NPDeductNonRecurringPL')
        temp_profit = temp_profit.drop_duplicates(['CompanyCode'],keep='last')
        
        temp_quote = pd.merge(temp_quote,temp_profit[['SecuCode','NPParentCompanyOwners','NPDeductNonRecurringPL','NPParentCompanyOwnersTTM',\
                                                      'NPDeductNonRecurringPLTTM','month']],on=['SecuCode'])
        temp_quote = pd.merge(temp_quote,temp_TotalShares[['SecuCode','TotalShares']],on=['SecuCode'],how='left')
        temp_quote['pe_float'] =  np.where(temp_quote['month']==3,0.25*temp_quote['TotalShares']* temp_quote['cp']/ temp_quote['NPParentCompanyOwners'],
                                   np.where(temp_quote['month']==6,0.5*temp_quote['TotalShares']* temp_quote['cp']/ temp_quote['NPParentCompanyOwners'],
                                     np.where(temp_quote['month']==9,3/4*temp_quote['TotalShares']* temp_quote['cp']/ temp_quote['NPParentCompanyOwners'],
                                       temp_quote['TotalShares']* temp_quote['cp']/ temp_quote['NPParentCompanyOwners'])))
        
        temp_quote['pe_float_cut'] =  np.where(temp_quote['month']==3,0.25*temp_quote['TotalShares']* temp_quote['cp']/ temp_quote['NPDeductNonRecurringPL'],
                                   np.where(temp_quote['month']==6,0.5*temp_quote['TotalShares']* temp_quote['cp']/ temp_quote['NPDeductNonRecurringPL'],
                                     np.where(temp_quote['month']==9,3/4*temp_quote['TotalShares']* temp_quote['cp']/ temp_quote['NPDeductNonRecurringPL'],
                                       temp_quote['TotalShares']* temp_quote['cp']/ temp_quote['NPDeductNonRecurringPL'])))
        #------------------------------
        temp_quote['pettm'] = temp_quote['TotalShares']* temp_quote['cp']/ temp_quote['NPParentCompanyOwnersTTM']
        temp_quote['pettm_cut'] = temp_quote['TotalShares']* temp_quote['cp']/ temp_quote['NPDeductNonRecurringPLTTM']
        temp_quote['TradingDay'] = datetime.datetime.strftime(i,'%Y%m%d')
         #只记录当前处于正常上市状态和恢复上市的股票------------------------------
        now_state = listedstate[i>listedstate['ChangeDate']].drop_duplicates(['InnerCode'],keep='last')
        now_state = now_state[now_state['ChangeType'].isin((1,3))] 
        temp_quote = temp_quote[temp_quote['InnerCode'].isin(now_state['InnerCode'])] 
        pe_week = pe_week.append(temp_quote)
        #pe_week.to_excel("C:\\Users\\dylan\\Desktop\\慧网工作\\股票量化策略\\多因子计算\\因子纠错\\pe.xlsx")
    pe_week['pettm'] = np.where(pd.isnull(pe_week['pettm'])!=True, pe_week['pettm'], None)
    pe_week['pettm_cut'] = np.where(pd.isnull(pe_week['pettm_cut'])!=True, pe_week['pettm_cut'], None)
    pe_week['pe_float'] = np.where(pd.isnull(pe_week['pe_float'])!=True, pe_week['pe_float'], None)
    pe_week['pe_float_cut'] = np.where(pd.isnull(pe_week['pe_float_cut'])!=True, pe_week['pe_float'], None)
    pe_week = pe_week[['TradingDay','SecuCode','pettm','pettm_cut','pe_float','pe_float_cut']]       

    
    #----删除数据中最后一天的数据，以防止该天的数据不是每周的最后一天------------------------------
    names = "(TradingDay,SecuCode,pettm,pettm_cut,pe_float,pe_float_cut)"   
    get.update_data(databasename,tablename,last_data) #删除最后一期数据，并且id自增
    get.insert_data(databasename,tablename,names,pe_week) #插入更新数据
    
    
   
        
        
        
        
    