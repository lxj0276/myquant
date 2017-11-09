# -*- coding: utf-8 -*-
"""
Created on Mon Nov  6 17:11:21 2017

@author: dylan
"""

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

holddays = pd.merge(pd.DataFrame(holddays),pd.DataFrame(day_buylist['cp']/day_buylist['cp']),left_index=True,right_index=True,how='outer')
holddays = holddays.fillna(0)
holddays = holddays.sum(axis=1)
holddays = holddays[holddays.index.isin(day_buylist.index)]
sell_holddays = holddays[~holddays.index.isin(day_buylist.index)]

#原有持仓，没有发出卖出信号，但权重需要重新调整
wait_buycash = pd.DataFrame(asset * day_buylist['weight'])
goal_stockvalue =  pd.DataFrame(asset * day_buylist['weight'])

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
    day = datetime.datetime.strftime(day_buylist['TradingDay'][-1],"%Y%m%d")
    sell_quote = pd.read_hdf(datapath,'equity_quote',columns=['SecuCode','cp','precp','vol'],
                             where='TradingDay=%s'%day)
    sell_quote = sell_quote[sell_quote['SecuCode'].isin(sell_holdvol.index)]
    sell_quote.index=sell_quote['SecuCode']
    surplus_cash,sell_tradefee,sell_stockvalue,sell_holdvol,day_sell_holdvol,sell_cash =  sell_trade(sell_holdvol,sell_quote,surplus_cash)
    if len(day_sell_holdvol) > 0:
        sell_tradedetail = pd.DataFrame(day_sell_holdvol.values,columns=['卖出成交数量'],index=day_sell_holdvol.index)
        sell_tradedetail['卖出成交金额'] = sell_cash
#新发出信号买入股票的程序
stockvalue = keep_holdvol * day_buylist[tradeprice] * unit
new_goal_buycash = wait_buycash['diff_cash']
new_goal_buycash[new_goal_buycash<0] = 0    
if  new_goal_buycash.max() >= min_wait_buycash:             
    #涨停股票权重设为0，即不买入
    day_buylist['weight'] =  np.where(day_buylist['cp']>=(1+daily_limit)*day_buylist['precp']-0.01,0,day_buylist['weight'])
    weight = new_goal_buycash  /   new_goal_buycash.sum()
    day_weight = pd.merge(pd.DataFrame(weight),day_buylist[['weight','SecuCode']],left_index=True,right_index=True,how='left')#涨停不买入
    day_weight[day_weight['weight']==0] = 0 #涨停的金额买入金额为0
    real_goal_buycash = surplus_cash * day_weight['diff_cash']
    add_holdvol =  pd.concat([np.floor(real_goal_buycash / day_buylist[tradeprice] / (1+feeratio) / unit),
                                 day_buylist['vol'] * volratio],axis=1)
    add_holdvol = add_holdvol.min(axis=1)
    holdvol = pd.merge(pd.DataFrame(keep_holdvol),pd.DataFrame(add_holdvol),left_index=True,right_index=True,how='outer')
    holdvol = holdvol.fillna(0)
    holdvol = holdvol.sum(axis=1)
            
    real_buycash = add_holdvol * unit * day_buylist[tradeprice]
    tradefee = real_buycash * feeratio
    wait_buycash = new_goal_buycash - real_buycash - tradefee 
    tradefee = tradefee.sum()
    surplus_cash = surplus_cash - real_buycash.sum() - tradefee
    stockvalue = holdvol * day_buylist[tradeprice] * unit
    
    if add_holdvol.sum()>0 or len(sell_tradedetail)>0:
        tradedetail = pd.DataFrame(goal_stockvalue.values,columns=['目标金额'],index=goal_stockvalue.index)
        tradedetail['成交数量'] =  pd.DataFrame(add_holdvol)
        tradedetail['成交金额'] =  real_buycash
        tradedetail['成交价格'] =  day_buylist[tradeprice] 
        tradedetail['剩余目标金额'] = wait_buycash
        tradedetail = tradedetail.append(sell_tradedetail)
else:
    holdvol = keep_holdvol
    wait_buycash = pd.Series(wait_buycash['diff_cash'].values,index=wait_buycash['diff_cash'].index)
    

asset = stockvalue.sum() + sell_stockvalue.sum() + surplus_cash
tradefee = tradefee + sell_tradefee      