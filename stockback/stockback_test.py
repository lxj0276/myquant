# -*- coding: utf-8 -*-
"""
Created on Fri Aug 11 14:25:12 2017

@author: dylan
"""

import pandas as pd
import numpy as np
import pymysql
import datetime
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")

'''
修改：本次修改了每年超额收益的bug；
备注：可以设定最大的买入股票只数，但是每一期的买入，都是全额投入，比如第一期买入4只股票，
而实际上最大仓位是7只，那么第一期同样投入100%资金，下一期若7只满足条件，则再重新分配资金。

此回溯程序是先挑选出每一期符合条件的股票，然后再开始交易买入
每一期股票buylist: 包含SecuCode,SecuAbbr,signal_rank,三个字段,index是日期格式，若是weight_type!=None,则需要有
weight字段，其中，signal_rank为每一期股票的买入优先级,从小到大，rank越小，优先级越高

quoe:行情数据，至少包含cp,precp,vol,fq_cp[计算止损止盈用]字段，
1. 止盈、止损每日进行判断
'''
 

datapath = "C:\\py_data\\datacenter\\quote.h5"
unit = 100
feeratio = 0.003
init_cash = 100000000
volratio = 1
tradeprice = 'cp'
daily_limit = 0.1 #涨跌停为10%
min_wait_buycash = 10000
buynumber = 10
min_holddays = 1
stop_type = 'relative'
weight_type =  None
loss_rtn = -0.05
gain_rtn=None

  
def first_buytrade(buylist):
    '''
    第一天的交易
    buylist包含:TradingDay,cp,vol
    asset:资产净值
    surplus_cash:剩余资金
    tradefee:当日手续费
    stockvalue:每一只股票的市值 dataframe
    holdvol:   每一只股票持仓数量 dataframe
    wait_buycash: 每一只股票的待买金额
    weight: 买入的分配比例，根据wait_buycash做分配
    position_cash:初始买入金额，由于仓位没满，留出的部分现金
    '''
    goal_buycash = init_cash * buylist['weight']   
    #涨停股票权重设为0，即不买入
    buylist['weight'] =  np.where(buylist['cp']>=(1+daily_limit)*buylist['precp']-0.01,0,buylist['weight'])       
    real_goal_buycash = init_cash * buylist['weight']
    holdvol = pd.concat([np.floor(real_goal_buycash / buylist[tradeprice] / (1+feeratio) / unit),
                         buylist['vol'] * volratio],axis=1)
    holdvol = holdvol.min(axis=1)
    holddays = holdvol / holdvol
    real_buycash = holdvol * unit * buylist[tradeprice]      
    tradefee = real_buycash * feeratio
    wait_buycash = goal_buycash - real_buycash - tradefee
    surplus_cash = init_cash - real_buycash.sum() - tradefee.sum()
    stockvalue = real_buycash
    tradefee = tradefee.sum()
    asset = stockvalue.sum() + surplus_cash
    tradedetail = pd.DataFrame(goal_buycash.values,columns=['目标金额'],index=goal_buycash.index)
    tradedetail['成交数量'] =  holdvol
    tradedetail['成交金额'] =  real_buycash
    tradedetail['成交价格'] =  buylist[tradeprice] 
    tradedetail['剩余目标金额'] = wait_buycash
    return asset,surplus_cash,tradefee,stockvalue,holdvol,wait_buycash,holddays,tradedetail

def run_trade(buylist,surplus_cash,holdvol,wait_buycash,sell_holdvol,holddays):
    '''
    非第一天及换仓第一天的交易
    min_wait_buycah: 最新的剩余资金，比若10000元，既当目标剩余资金小于10000元是，不再继续买入
    surplus_cash:剩余资金
    holdvol:T-1日持仓数量明细
    wait_buycash:每只股票剩余待买目标资金
    '''     
    tradefee = 0
    sell_tradefee = 0
    sell_stockvalue = pd.Series(0)
    sell_tradedetail = pd.DataFrame()
    tradedetail = pd.DataFrame()
    add_holdvol = pd.Series()
    holddays = holddays + 1
    day = datetime.datetime.strftime(buylist['TradingDay'][-1],"%Y%m%d")
    buylist = buylist[buylist.index.isin(holdvol.index)] #只关注持仓中的行情
    
    #先卖出股票,腾出资金        
    if len(sell_holdvol) > 0:
        sell_quote = pd.read_hdf(datapath,'equity_quote',columns=['SecuCode','cp','precp','vol'],
                                 where='SecuCode in %s & TradingDay=%s'%(str(list(sell_holdvol.index)),day))                          
        sell_quote.index = sell_quote['SecuCode']
        surplus_cash,sell_tradefee,sell_stockvalue,sell_holdvol,day_sell_holdvol,sell_cash =  sell_trade(sell_holdvol,sell_quote,surplus_cash)
        if len(day_sell_holdvol) > 0:
            sell_tradedetail = pd.DataFrame(day_sell_holdvol.values,columns=['卖出成交数量'],index=day_sell_holdvol.index)
            sell_tradedetail['卖出成交金额'] = sell_cash
         

    #接下来继续交易没有完成的交易
    stockvalue = holdvol * buylist[tradeprice] * unit   
    buy_weight = wait_buycash / wait_buycash.sum() #按照未成目标金额，分配剩余资金的买入权重
    
    #如上一个交易日，由于涨停或者成交量限制，没完成交易的，继续交易    
    if wait_buycash.max() >= min_wait_buycash:
        goal_buycash = min(surplus_cash,wait_buycash.sum()) * buy_weight
        goal_buycash = goal_buycash.apply(lambda x: 0 if x<=min_wait_buycash else x)
        real_goal_buycash = adjust_goal_buycash(goal_buycash,buylist) #涨停股票的今日买入金额为0
        add_holdvol =  pd.concat([np.floor(real_goal_buycash / buylist[tradeprice] / (1+feeratio) / unit),
                                           buylist['vol'] * volratio],axis=1)
        add_holdvol = add_holdvol.min(axis=1)
        holdvol = holdvol + add_holdvol  
        real_buycash = add_holdvol * unit * buylist[tradeprice]
        tradefee = real_buycash * feeratio
        wait_buycash = wait_buycash - real_buycash - tradefee          
        stockvalue = holdvol * buylist[tradeprice] * unit
        tradefee = tradefee.sum() 
        surplus_cash = surplus_cash - real_buycash.sum() - tradefee
        
        if add_holdvol.sum()>0 or len(sell_tradedetail)>0:
            tradedetail = pd.DataFrame(wait_buycash.values,columns=['目标金额'],index=wait_buycash.index)
            tradedetail['成交数量'] =  pd.DataFrame(add_holdvol)
            tradedetail['成交金额'] =  real_buycash
            tradedetail['成交价格'] =  buylist[tradeprice] 
            tradedetail['剩余目标金额'] = wait_buycash
            tradedetail = tradedetail.append(sell_tradedetail)

    
    tradefee = sell_tradefee + tradefee
    asset = stockvalue.sum() + surplus_cash
    asset = stockvalue.sum() + sell_stockvalue.sum() + surplus_cash
        
    return asset,surplus_cash,tradefee,stockvalue,holdvol,wait_buycash,sell_holdvol,holddays,tradedetail

  

def adjust_first_trade(buylist,asset,surplus_cash,stockvalue,keep_holdvol,sell_holdvol,holddays):
    '''
    换仓的第一天交易：
    1. 先进行权重重新分配，原有持仓中权重大的，卖出一部分股票，权重小的则按照
    2. 先卖出，然后所得的资金在用于买入
    '''
    sell_tradefee = 0
    tradefee = 0
    sell_stockvalue = pd.Series(0)
    sell_tradedetail = pd.DataFrame()
    tradedetail = pd.DataFrame()

    holddays = pd.merge(pd.DataFrame(holddays),pd.DataFrame(buylist['cp']/buylist['cp']),left_index=True,right_index=True,how='outer')
    holddays = holddays.fillna(0)
    holddays = holddays.sum(axis=1)
    holddays = holddays[holddays.index.isin(buylist.index)]
    sell_holddays = holddays[~holddays.index.isin(buylist.index)]
    
    #原有持仓，没有发出卖出信号，但权重需要重新调整
    wait_buycash = pd.DataFrame(asset * buylist['weight'])
    goal_stockvalue =  pd.DataFrame(asset * buylist['weight'])

    if len(keep_holdvol) > 0:#持仓reblance部分
        keep_stockvalue =  stockvalue[stockvalue.index.isin(keep_holdvol.index)]
        keep_stockvalue = pd.DataFrame(keep_stockvalue,index=keep_stockvalue.index,columns=['keep_stockvalue'])           
        goal_buycash = pd.merge(wait_buycash,keep_stockvalue,left_index=True,right_index=True,how='left')
        goal_buycash['keep_stockvalue'] = goal_buycash['keep_stockvalue'].fillna(0)
        goal_buycash['diff_cash'] = goal_buycash['weight'] - goal_buycash['keep_stockvalue']
        goal_buycash['holdvol_weight'] = goal_buycash['diff_cash'] / goal_buycash['keep_stockvalue']
        wait_buycash = goal_buycash
        #重新调整权重，需要卖出部分股票的程序 + 卖出股票的卖出交易
        sell_keep_holdvol_weight =   pd.DataFrame(goal_buycash[goal_buycash['holdvol_weight']<0].holdvol_weight )
        sell_keep_holdvol = pd.merge(pd.DataFrame(keep_holdvol),sell_keep_holdvol_weight,left_index=True,right_index=True,how='left')          
        sell_keep_holdvol = np.floor(sell_keep_holdvol[0] * np.abs(sell_keep_holdvol['holdvol_weight'])).dropna() 
        keep_holdvol = pd.merge(pd.DataFrame(keep_holdvol),pd.DataFrame(sell_keep_holdvol),left_index=True,right_index=True,how='left')
        keep_holdvol = keep_holdvol.fillna(0)
        keep_holdvol[0] = keep_holdvol['0_x'] - keep_holdvol['0_y']
        keep_holdvol = keep_holdvol[0]
        sell_keep_holdvol =  sell_keep_holdvol[sell_keep_holdvol>0]             
        if len(sell_holdvol) > 0:
            sell_holdvol = sell_holdvol.append(sell_keep_holdvol)
            
        else:
            sell_holdvol =   sell_keep_holdvol  
    else:
        wait_buycash['diff_cash'] = wait_buycash.values
    if len(sell_holdvol) > 0:
        day = datetime.datetime.strftime(buylist['TradingDay'][-1],"%Y%m%d")
        sell_quote = pd.read_hdf(datapath,'equity_quote',columns=['SecuCode','cp','precp','vol'],
                                 where='TradingDay=%s'%day)
        sell_quote = sell_quote[sell_quote['SecuCode'].isin(sell_holdvol.index)]
        sell_quote.index=sell_quote['SecuCode']
        surplus_cash,sell_tradefee,sell_stockvalue,sell_holdvol,day_sell_holdvol,sell_cash =  sell_trade(sell_holdvol,sell_quote,surplus_cash)
        if len(day_sell_holdvol) > 0:
            sell_tradedetail = pd.DataFrame(day_sell_holdvol.values,columns=['卖出成交数量'],index=day_sell_holdvol.index)
            sell_tradedetail['卖出成交金额'] = sell_cash
    #新发出信号买入股票的程序
    stockvalue = keep_holdvol * buylist[tradeprice] * unit
    new_goal_buycash = wait_buycash['diff_cash']
    new_goal_buycash[new_goal_buycash<0] = 0    
    if  new_goal_buycash.max() >= min_wait_buycash:             
        #涨停股票权重设为0，即不买入
        buylist['weight'] =  np.where(buylist['cp']>=(1+daily_limit)*buylist['precp']-0.01,0,buylist['weight'])
        weight = new_goal_buycash  /   new_goal_buycash.sum()
        day_weight = pd.merge(pd.DataFrame(weight),buylist[['weight','SecuCode']],left_index=True,right_index=True,how='left')#涨停不买入
        day_weight[day_weight['weight']==0] = 0 #涨停的金额买入金额为0
        real_goal_buycash = surplus_cash * day_weight['diff_cash']
        add_holdvol =  pd.concat([np.floor(real_goal_buycash / buylist[tradeprice] / (1+feeratio) / unit),
                                     buylist['vol'] * volratio],axis=1)
        add_holdvol = add_holdvol.min(axis=1)
        holdvol = pd.merge(pd.DataFrame(keep_holdvol),pd.DataFrame(add_holdvol),left_index=True,right_index=True,how='outer')
        holdvol = holdvol.fillna(0)
        holdvol = holdvol.sum(axis=1)
                
        real_buycash = add_holdvol * unit * buylist[tradeprice]
        tradefee = real_buycash * feeratio
        wait_buycash = new_goal_buycash - real_buycash - tradefee 
        tradefee = tradefee.sum()
        surplus_cash = surplus_cash - real_buycash.sum() - tradefee
        stockvalue = holdvol * buylist[tradeprice] * unit
        
        if add_holdvol.sum()>0 or len(sell_tradedetail)>0:
            tradedetail = pd.DataFrame(goal_stockvalue.values,columns=['目标金额'],index=goal_stockvalue.index)
            tradedetail['成交数量'] =  pd.DataFrame(add_holdvol)
            tradedetail['成交金额'] =  real_buycash
            tradedetail['成交价格'] =  buylist[tradeprice] 
            tradedetail['剩余目标金额'] = wait_buycash
            tradedetail = tradedetail.append(sell_tradedetail)
    else:
        holdvol = keep_holdvol
        wait_buycash = pd.Series(wait_buycash['diff_cash'].values,index=wait_buycash['diff_cash'].index)
        
    
    asset = stockvalue.sum() + sell_stockvalue.sum() + surplus_cash
    tradefee = tradefee + sell_tradefee         
    return asset,tradefee,surplus_cash,holdvol,sell_holdvol,wait_buycash,stockvalue,sell_stockvalue,holddays,sell_holddays,tradedetail
          
    
def sell_trade(sell_holdvol,sell_quote,surplus_cash):
    '''
    卖出交易
    '''
    day_sell_holdvol =  pd.concat([sell_holdvol,sell_quote[['vol','cp','precp']]],axis=1) 
    day_sell_holdvol['vol'] = day_sell_holdvol['vol'] * volratio
    day_sell_holdvol[0] = np.where(day_sell_holdvol['cp']<=(1-daily_limit)*day_sell_holdvol['precp'] + 0.01
                                    ,0,day_sell_holdvol[0])  #跌停判断,跌停不卖出
    day_sell_holdvol = day_sell_holdvol.drop(labels=['cp','precp'],axis=1)
    day_sell_holdvol = day_sell_holdvol.min(axis=1)
    surplus_holdvol = sell_holdvol - day_sell_holdvol
    sell_cash = day_sell_holdvol * sell_quote[tradeprice] * unit
    sell_tradefee = (sell_cash * feeratio).sum()
    surplus_cash = surplus_cash + sell_cash.sum() - sell_tradefee
    sell_stockvalue = surplus_holdvol * sell_quote[tradeprice] * unit
    surplus_holdvol = surplus_holdvol[surplus_holdvol>0]
    sell_stockvalue = sell_stockvalue[sell_stockvalue>0]
    return surplus_cash,sell_tradefee,sell_stockvalue,surplus_holdvol,day_sell_holdvol,sell_cash

def bonus_deal(day_bonus,holdvol,sell_holdvol,surplus_cash,asset): 
    '''
    分红行权处理
    '''   
    if len(holdvol) > 0:                 
        new_holdvol = pd.merge(pd.DataFrame(holdvol),day_bonus[['SecuCode','BonusShareRatio','TranAddShareRaio','CashDiviRMB']],\
                            left_index=True,right_on=['SecuCode'],how='left')
        new_holdvol =  new_holdvol.fillna(0)
        new_holdvol.index = holdvol.index
        holdvol = new_holdvol[0] *(1+new_holdvol['BonusShareRatio']/10+new_holdvol['TranAddShareRaio']/10)
        bonus_cash = (new_holdvol[0] * new_holdvol['CashDiviRMB']*unit/10).sum()
        surplus_cash = surplus_cash + bonus_cash
        asset = asset + bonus_cash
    if len(sell_holdvol) > 0:
        new_sell_holdvol = pd.merge(pd.DataFrame(sell_holdvol),day_bonus[['SecuCode','BonusShareRatio','TranAddShareRaio','CashDiviRMB']],\
                            left_index=True,right_on=['SecuCode'],how='left')
        new_sell_holdvol =  new_sell_holdvol.fillna(0)
        new_sell_holdvol.index = sell_holdvol.index
        sell_holdvol = new_sell_holdvol[0] *(1+new_sell_holdvol['BonusShareRatio']/10+new_sell_holdvol['TranAddShareRaio']/10)
        bonus_cash =  (new_sell_holdvol[0] * new_sell_holdvol['CashDiviRMB']*unit/10).sum()
        surplus_cash = surplus_cash + bonus_cash
        asset = asset + bonus_cash
    return holdvol,sell_holdvol,surplus_cash,asset
 
def adjust_goal_buycash(goal_buycash,buylist):
    '''
    调整买入目标金额，当今天涨停时，则今日买入金额为0
    '''
    goal_buycash = pd.DataFrame(goal_buycash.values,index=goal_buycash.index,columns=['goal_buycash'])
    goal_buycash = pd.merge(goal_buycash,buylist,left_index=True,right_index=True,how='left')
    goal_buycash['goal_buycash'] = np.where(goal_buycash['cp']>=(1+daily_limit)*goal_buycash['precp']-0.01,0,goal_buycash['goal_buycash'])
    return goal_buycash['goal_buycash']   


def daily_loss_gain(fq_cp,trade_fq_cp,day_benchmark_quote,trade_benchmark_cp,sell_holdvol,holdvol,stockvalue,wait_buycash,holddays):
    '''
    止盈止损的调整
    发生止盈、止损需要卖出的情况，则进行相应调整
    '''
    sell_stock = pd.Series()
    if stop_type == 'relative': #相对收益止盈、止损
        if loss_rtn is not None: #止损
            dail_sy = fq_cp / trade_fq_cp - 1
            alpha_sy = dail_sy -  (day_benchmark_quote['precp'][0] / trade_benchmark_cp-1)
            sell_stock = alpha_sy[alpha_sy<=loss_rtn] 
            #day_buylist = day_buylist[~day_buylist.index.isin(loss_stock.index)]
            sell_holdvol = sell_holdvol.append(holdvol[sell_stock.index])
            holdvol = holdvol[~holdvol.index.isin(sell_stock.index)]
            stockvalue =  stockvalue[~stockvalue.index.isin(sell_stock.index)]
            wait_buycash = wait_buycash[~wait_buycash.index.isin(sell_stock.index)]
            holddays = holddays[~holddays.index.isin(sell_stock.index)]
            trade_benchmark_cp = trade_benchmark_cp[holdvol.index]
            trade_fq_cp = trade_fq_cp[holdvol.index]
            #sell_holdvol = sell_holdvol.sum()
     
        if gain_rtn is not None: #止盈
            dail_sy = fq_cp / trade_fq_cp - 1
            alpha_sy = dail_sy -  (day_benchmark_quote['precp'][0] / trade_benchmark_cp-1)
            sell_stock = alpha_sy[alpha_sy>=gain_rtn] 
            #day_buylist = day_buylist[~day_buylist.index.isin(gain_stock.index)]                        
            sell_holdvol = sell_holdvol.append(holdvol[sell_stock.index])
            holdvol = holdvol[~holdvol.index.isin(sell_stock.index)]
            stockvalue =  stockvalue[~stockvalue.index.isin(sell_stock.index)]
            wait_buycash = wait_buycash[~wait_buycash.index.isin(sell_stock.index)]
            holddays = holddays[~holddays.index.isin(sell_stock.index)]
            trade_benchmark_cp = trade_benchmark_cp[holdvol.index]
            trade_fq_cp = trade_fq_cp[holdvol.index]
          
    elif stop_type == 'abs': #绝对止盈、止损
        if loss_rtn is not None: #止损
            dail_sy = fq_cp / trade_fq_cp - 1
            sell_stock = dail_sy[dail_sy<=loss_rtn] 
            #day_buylist = day_buylist[~day_buylist.index.isin(loss_stock.index)]
            sell_holdvol = sell_holdvol.append(holdvol[sell_stock.index])
            holdvol = holdvol[~holdvol.index.isin(sell_stock.index)]
            stockvalue =  stockvalue[~stockvalue.index.isin(sell_stock.index)]
            wait_buycash = wait_buycash[~wait_buycash.index.isin(sell_stock.index)]
            holddays = holddays[~holddays.index.isin(sell_stock.index)]
            trade_benchmark_cp = trade_benchmark_cp[holdvol.index]
            trade_fq_cp = trade_fq_cp[holdvol.index]
         
        if gain_rtn is not None: #止盈
            dail_sy = fq_cp / trade_fq_cp - 1
            sell_stock = dail_sy[dail_sy>=gain_rtn] 
            #day_buylist = day_buylist[~day_buylist.index.isin(gain_stock.index)]
            sell_holdvol = sell_holdvol.append(holdvol[sell_stock.index])
            holdvol = holdvol[~holdvol.index.isin(sell_stock.index)]
            stockvalue =  stockvalue[~stockvalue.index.isin(sell_stock.index)]
            wait_buycash = wait_buycash[~wait_buycash.index.isin(sell_stock.index)]
            holddays = holddays[~holddays.index.isin(sell_stock.index)]
            trade_benchmark_cp = trade_benchmark_cp[holdvol.index]
            trade_fq_cp = trade_fq_cp[holdvol.index]
        
    #------------------------------------------------------------------------------------- 
    return sell_stock,trade_fq_cp,trade_benchmark_cp,sell_holdvol,holdvol,stockvalue,wait_buycash,holddays                  
    
        
def settle(buylist,benchmark_code,enddate):   
    '''
    buy_list:每次的选股清单，datetime格式的index,innercode,secucode,secuabbr,signal_rank
    quote：行情,需要至少包括TradingDay,secucode,cp,precp,vol字段
    bonus: 分红数据,至少需要包括ExDiviDate:行权日期, BonusShareRatio:送股比例(10送X),
            TranAddShareRaio:转增股比例(10转增X),CashDiviRMB:派现(含税/人民币元) 字段
    每日清算程序
    benchmark:基准行情，index为日期格式，index_cp,收盘价行情
    '''
    timeindex = pd.DataFrame(buylist.index.drop_duplicates().values)
    timeindex = timeindex[timeindex[0]<=enddate]
    timeindex['date'] = timeindex[0].apply(lambda x:datetime.datetime.strftime(x,"%Y%m%d"))
    starttime = timeindex['date'].min() 
    #获取分红数据
    bonus =  pd.read_hdf(datapath,'bonus', columns=['ExDiviDate','SecuCode','BonusShareRatio','TranAddShareRaio','CashDiviRMB'],
                         where='ExDiviDate>=%s & ExDiviDate<=%s'%(starttime,enddate))         
    #获取基准行情数据
    benchmark =  pd.read_hdf(datapath,'index_quote',columns=['TradingDay','SecuCode','cp','precp'],
                                 where='SecuCode in %s & TradingDay>%s & TradingDay<=%s'%(str(tuple((benchmark_code,'000'))),starttime,enddate))           
    benchmark.index = benchmark['TradingDay']
    

    asset0 = []
    holdvol0 = pd.DataFrame()
    stockvalue0 = pd.DataFrame()
    sell_holdvol0 = pd.DataFrame()
    wait_buycash0 = pd.DataFrame()
    holddays0 = pd.DataFrame()
    sell_holddays0 = pd.DataFrame()
    tradedetail0 = pd.DataFrame()
    buytime = pd.DataFrame()
    selltime = pd.DataFrame()
    day1 = []
    holddays = pd.Series()
    
    for i in tqdm(range(len(timeindex))):
        nowtime =  timeindex.iloc[i]['date'] 
        if i == len(timeindex) - 1:
            nexttime = enddate
        else:
            nexttime =  timeindex.iloc[i+1]['date']
  
        day_buy = buylist[buylist.index==nowtime]
        day_buy.index = day_buy['SecuCode']
        #当买入的天数不足最小持有天数数，则本次调仓不卖出，把不卖出的股票添加到买入列表，并且signal_rank设置成最小，既最优先买入
        cannot_sellstock = holddays[holddays<=min_holddays]
        buystock = day_buy.index
        if len(cannot_sellstock) > 0:
            buystock =  cannot_sellstock.index.append(day_buy.index)
        
        #获取本次调仓时间到下次调仓时间的数据
        session_quote = pd.read_hdf(datapath,'equity_quote',columns=['TradingDay','SecuCode','cp','precp','fq_cp','vol'],
                                 where='SecuCode in %s & TradingDay>%s & TradingDay<=%s'%(str(list(buystock)),nowtime,nexttime))  
        
        session_quote = pd.merge(session_quote,day_buy[['SecuCode','signal_rank']],on=['SecuCode'],how='left')
        session_quote['signal_rank'] = session_quote['signal_rank'].fillna(0)
        session_quote.index  =session_quote['TradingDay']
        
        session_timeindex = session_quote[['TradingDay']].drop_duplicates()
        session_timeindex = session_timeindex.sort_index()
        
        if i == 0: #第一次交易，仅有买入
            for day in session_timeindex.index:
                day_buylist = session_quote[session_quote['TradingDay']==day]
                day_buylist.index = day_buylist['SecuCode'] 
                day_benchmark_quote = benchmark[benchmark['TradingDay']==day] 
                                            
                #第一天的处理
                if day == session_timeindex.index.min():  
                    day_buylist['signal_rank'] = day_buylist['signal_rank'].rank(method='first')    
                    day_buylist = day_buylist[day_buylist['signal_rank']<=buynumber]
                    if weight_type == None:
                        day_buylist['weight'] = 1.0 / len(day_buylist)
                    sell_stockvalue =  pd.Series(0)      
                    trade_fq_cp = day_buylist['fq_cp'] #记录复权成交价 
                    fq_cp = trade_fq_cp
                    trade_benchmark_cp = (1+trade_fq_cp - trade_fq_cp) *day_benchmark_quote['cp'][0] #记录基准价格
                    asset,surplus_cash,tradefee,stockvalue,holdvol,wait_buycash,holddays,tradedetail = first_buytrade(day_buylist)                          
                else:                       
                    sell_holdvol = pd.Series()                        
                    #---daily止盈止损-------------------------------------------------------------------       
                    if stop_type is not None:
                        sell_stock,trade_fq_cp,trade_benchmark_cp,sell_holdvol,holdvol,stockvalue,wait_buycash,holddays = \
                            daily_loss_gain(fq_cp,trade_fq_cp,day_benchmark_quote,trade_benchmark_cp 
                                     ,sell_holdvol,holdvol,stockvalue,wait_buycash,holddays) 
                        
                    day_bonus = bonus[bonus['ExDiviDate']==day]
                    day_bonus.index = day_bonus['SecuCode']
                    day_bonus = day_bonus[(day_bonus.index.isin(holdvol.index))|(day_bonus.index.isin(sell_holdvol.index))]
                    #分红处理
                    if len(day_bonus) > 0:                        
                        holdvol,sell_holdvol,surplus_cash,asset = bonus_deal(day_bonus,holdvol,sell_holdvol,surplus_cash,asset)
                    
                    asset,surplus_cash,tradefee,stockvalue,holdvol,wait_buycash,sell_holdvol,holddays,tradedetail = \
                    run_trade(day_buylist,surplus_cash,holdvol,wait_buycash,sell_holdvol,holddays)
                    fq_cp = day_buylist.ix[holdvol.index]['cp'] #记录现有持仓前实盘价
                    
                    
                asset0.append([asset,tradefee,surplus_cash,stockvalue.sum(),sell_stockvalue.sum()])
                holdvol0 = holdvol0.append(pd.DataFrame(holdvol))
                stockvalue0 = stockvalue0.append(pd.DataFrame(stockvalue))
                holddays0 = holddays0.append(pd.DataFrame(holddays))
                buytime = buytime.append(pd.DataFrame(len(holdvol)*[day]))
                wait_buycash0 = wait_buycash0.append(pd.DataFrame(wait_buycash))
                tradedetail['日期'] =  np.array(len(tradedetail) * [day])
                tradedetail0 = tradedetail0.append(tradedetail)
                day1.append(day)
               
                    
        else:                 
            for day in session_timeindex.index:
                #print(day)
                day_buylist = session_quote[session_quote['TradingDay']==day]
                day_buylist.index = day_buylist['SecuCode'] 
                day_benchmark_quote = benchmark[benchmark['TradingDay']==day] 
                day_bonus = bonus[bonus['ExDiviDate']==day]
                day_bonus.index = day_bonus['SecuCode']
                #分红处理  
                try:
                    day_bonus = day_bonus[(day_bonus.index.isin(holdvol.index))|(day_bonus.index.isin(sell_holdvol.index))]
                except:
                    day_bonus = day_bonus[(day_bonus.index.isin(holdvol.index))]
                                            
                if len(day_bonus) > 0:                        
                    holdvol,sell_holdvol,surplus_cash,asset = bonus_deal(day_bonus,holdvol,sell_holdvol,surplus_cash,asset)                          
                
                #换仓日的第一天交易
                if day == session_timeindex['TradingDay'].min():
                    #发生止盈止损的股票，即使本次有新信号产生，也不买入
                    
                    if stop_type is not None:
                        sell_stock,trade_fq_cp,trade_benchmark_cp,sell_holdvol,holdvol,stockvalue,wait_buycash,holddays = \
                                daily_loss_gain(fq_cp,trade_fq_cp,day_benchmark_quote,trade_benchmark_cp
                                                 ,sell_holdvol,holdvol,stockvalue,wait_buycash,holddays) 
                       
                    day_buylist = day_buylist[~day_buylist.index.isin(sell_stock.index)]  
                     #持仓中在本次股票池中，则signal_rank=0,相当于不卖出  
                    day_buylist['signal_rank'] = np.where(day_buylist.index.isin(holdvol.index)==True,0,day_buylist['signal_rank'])
                    day_buylist['signal_rank'] = day_buylist['signal_rank'].rank(method='first')    
                    day_buylist = day_buylist[day_buylist['signal_rank']<=buynumber]
                    if weight_type == None:
                        day_buylist['weight'] = 1.0 / day_buylist.count()[0]
                    #进行止盈止损操作,，仍在买入目标池中股票，计算当前的收益
                    keep_holdvol = holdvol[holdvol.index.isin(day_buylist.index)]
                    old_sell_holdvol = pd.DataFrame()
                    try:
                        if len(sell_holdvol)>0: #待卖出的股票中，有需要买入的，则取消卖出                          
                            keep_sell_holdvol = sell_holdvol[sell_holdvol.index.isin(day_buylist.index)]
                            old_sell_holdvol =pd.DataFrame(sell_holdvol[~sell_holdvol.index.isin(day_buylist.index)])  
                            if len(keep_sell_holdvol) > 0:#未卖出部分转移至保持股票列表
                                keep_holdvol = pd.merge(pd.DataFrame(keep_holdvol),pd.DataFrame(keep_sell_holdvol),left_index=True,right_index=True,how='outer')
                                keep_holdvol = keep_holdvol.fillna(0)
                                keep_holdvol = keep_holdvol.sum(axis=1)
                                keep_sell_stockvalue = sell_stockvalue[sell_stockvalue.index.isin(day_buylist.index)]
                                stockvalue = pd.merge(pd.DataFrame(stockvalue),pd.DataFrame(keep_sell_stockvalue),left_index=True,right_index=True,how='outer')
                                stockvalue = stockvalue.fillna(0)
                                stockvalue = stockvalue.sum(axis=1)
                    except:
                        pass
                    
                    finally:                           
                        if len(sell_holdvol) > 0:
                            new_sell_holdvol = holdvol[~holdvol.index.isin(day_buylist.index)] 
                            sell_holdvol = pd.merge(old_sell_holdvol,pd.DataFrame(new_sell_holdvol),left_index=True,right_index=True,how='outer')
                            sell_holdvol = sell_holdvol.fillna(0)
                            sell_holdvol = sell_holdvol.sum(axis=1)
                        else:
                            sell_holdvol = holdvol[~holdvol.index.isin(day_buylist.index)] 
                        
                        #time1 = time.time()
                        asset,tradefee,surplus_cash,holdvol,sell_holdvol,wait_buycash,stockvalue,sell_stockvalue,holddays,sell_holddays,tradedetail = \
                        adjust_first_trade(day_buylist,asset,surplus_cash,stockvalue,keep_holdvol,sell_holdvol,holddays)
                        #time2 = time.time()
                        #print('day=%s,所耗时间=%s'%(day,time2-time1))
                        
                        new_trade_stock = holdvol[~holdvol.index.isin(trade_fq_cp.index)].index
                        new_trade_stock =  day_buylist[day_buylist.index.isin(new_trade_stock)]['cp']
                        trade_fq_cp = trade_fq_cp.append(new_trade_stock) #新买入股票，记录买入成本价
                        trade_benchmark_cp = trade_benchmark_cp.append((1+new_trade_stock-new_trade_stock)*day_benchmark_quote['cp'][0])
                        fq_cp = day_buylist.ix[holdvol.index]['cp']
                        sell_holddays0 = sell_holddays0.append(pd.DataFrame(sell_holddays))
                   
                else:
           
                    if stop_type is not None:
                        sell_stock,trade_fq_cp,trade_benchmark_cp,sell_holdvol,holdvol,stockvalue,wait_buycash,holddays = \
                            daily_loss_gain(fq_cp,trade_fq_cp,day_benchmark_quote,trade_benchmark_cp
                                             ,sell_holdvol,holdvol,stockvalue,wait_buycash,holddays) 
                    asset,surplus_cash,tradefee,stockvalue,holdvol,wait_buycash,sell_holdvol,holddays,tradedetail = \
                    run_trade(day_buylist,surplus_cash,holdvol,wait_buycash,sell_holdvol,holddays)
                    fq_cp = day_buylist.ix[holdvol.index]['cp']
                
                
                asset0.append([asset,tradefee,surplus_cash,stockvalue.sum(),sell_stockvalue.sum()])
                holdvol0 = holdvol0.append(pd.DataFrame(holdvol))
                stockvalue0 = stockvalue0.append(pd.DataFrame(stockvalue))
                sell_holdvol0 = sell_holdvol0.append(pd.DataFrame(sell_holdvol))
                holddays0 = holddays0.append(pd.DataFrame(holddays))
                buytime = buytime.append(pd.DataFrame(len(holdvol)*[day]))
                selltime = selltime.append(pd.DataFrame(len(sell_holdvol)*[day]))
                wait_buycash0 = wait_buycash0.append(pd.DataFrame(wait_buycash))
                tradedetail['日期'] =  np.array(len(tradedetail) * [day])
                tradedetail0 = tradedetail0.append(tradedetail)
                day1.append(day)
               
    
    #每日的持仓明细 
    index2 = buytime.drop_duplicates()[0]
    asset = pd.DataFrame(np.array(asset0),index=day1,columns=['净值','手续费','现金','持有证券资产','待卖证券资产'])
    
    holdvol0['SecuCode'] = holdvol0.index
    holdvol0['市值'] =  np.array(stockvalue0 )
    holdvol0['天数'] =  np.array(holddays0 )
    holdvol0['目标金额'] = np.array( wait_buycash0 )
    holdvol0['date']  =  np.array(buytime[0])
   
     #插入代码和简称
    name_info = buylist[['SecuCode','SecuAbbr']].drop_duplicates()
    name_info.index = range(len(name_info))
    name_info = name_info.drop_duplicates()

    holdvol = pd.merge(name_info,holdvol0,on='SecuCode',how='right')
    holdvol = pd.DataFrame(np.array(holdvol),columns=['证券代码','证券简称','持仓数量','市值','天数','目标金额','日期'])
      
    if len(selltime) > 0:
       sell_holdvol0['SecuCode'] =  sell_holdvol0.index
       sell_holdvol0['date'] = np.array(selltime[0])
       sell_holdvol = pd.merge(name_info,sell_holdvol0,on='SecuCode',how='right')
       sell_holdvol = pd.DataFrame(np.array(sell_holdvol),columns=['证券代码','证券简称','持仓数量','日期'])

    else:
        sell_holdvol = pd.DataFrame(columns=['证券代码','证券简称','持仓数量','日期'])

    tradedetail0 = pd.merge(name_info,tradedetail0,right_index=True,left_on='SecuCode',how='right')      
    
    
    with pd.ExcelWriter("C:\\py_data\\textdata\\text.xlsx") as writer:
        asset.to_excel(writer,u'asset')           
        sell_holdvol.to_excel(writer,'待卖出股票情况')           
        sell_holddays0.to_excel(writer,'股票持仓天数统计')
        tradedetail0.to_excel(writer,"每日成交明细")
        holdvol.to_excel(writer,"每日持仓明细")
    
    return asset,holdvol,sell_holdvol,tradedetail0,sell_holddays0 

    

           
        

 
            
 
