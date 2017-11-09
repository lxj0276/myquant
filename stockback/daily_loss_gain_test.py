# -*- coding: utf-8 -*-
"""
Created on Wed Nov  8 11:19:45 2017

@author: dylan
"""

sell_stock = pd.Series()
if stop_type == 'relative': #相对收益止盈、止损
    if loss_rtn is not None: #止损
        dail_sy = fq_cp / trade_fq_cp - 1
        alpha_sy = dail_sy -  (day_benchmark_quote['precp'][0] / trade_benchmark_cp-1)
        sell_stock = alpha_sy[alpha_sy<=loss_rtn] 
        #day_buylist = day_buylist[~day_buylist.index.isin(loss_stock.index)]
        sell_holdvol = sell_holdvol.append(holdvol[sell_stock.index])
        sell_holdvol = sell_holdvol.groupby(sell_holdvol.index).sum()        
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
        sell_holdvol = sell_holdvol.groupby(sell_holdvol.index).sum()
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
        sell_holdvol = sell_holdvol.groupby(sell_holdvol.index).sum()
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
        sell_holdvol = sell_holdvol.groupby(sell_holdvol.index).sum()
        holdvol = holdvol[~holdvol.index.isin(sell_stock.index)]
        stockvalue =  stockvalue[~stockvalue.index.isin(sell_stock.index)]
        wait_buycash = wait_buycash[~wait_buycash.index.isin(sell_stock.index)]
        holddays = holddays[~holddays.index.isin(sell_stock.index)]
        trade_benchmark_cp = trade_benchmark_cp[holdvol.index]
        trade_fq_cp = trade_fq_cp[holdvol.index]