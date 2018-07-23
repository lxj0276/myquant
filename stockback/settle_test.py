   
'''
buy_list:每次的选股清单，datetime格式的index,innercode,secucode,secuabbr,signal_rank
quote：行情,需要至少包括TradingDay,secucode,cp,precp,vol字段
bonus: 分红数据,至少需要包括ExDiviDate:行权日期, BonusShareRatio:送股比例(10送X),
TranAddShareRaio:转增股比例(10转增X),CashDiviRMB:派现(含税/人民币元) 字段
每日清算程序
benchmark:基准行情，index为日期格式，index_cp,收盘价行情
'''
'''
enddate = '20170515'
benchmark_code = '399905'

timeindex = pd.DataFrame(buylist4.index.drop_duplicates().values)
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

'''



#for i in tqdm(range(len(timeindex))):
for i in tqdm(range(27)):
    #i  = 27
    nowtime =  timeindex.iloc[i]['date'] 
    if i == len(timeindex) - 1:
        nexttime = enddate
    else:
        nexttime =  timeindex.iloc[i+1]['date']
  
    day_buy = buylist4[buylist4.index==nowtime]
    day_buy.index = day_buy['SecuCode']
    #当买入的天数不足最小持有天数数，则本次调仓不卖出，把不卖出的股票添加到买入列表，并且signal_rank设置成最小，既最优先买入
    cannot_sellstock = holddays[holddays<min_holddays]
    buystock = day_buy.index
    if len(cannot_sellstock) > 0:
        buystock =  cannot_sellstock.index.append(day_buy.index)
    
    #获取本次调仓时间到下次调仓时间的数据
    session_quote = pd.read_hdf(datapath,'equity_quote',columns=['TradingDay','SecuCode','cp','precp','fq_cp','vol'],
                             where='TradingDay>%s & TradingDay<=%s'%(nowtime,nexttime))  
    buy_quote = session_quote[session_quote['SecuCode'].isin(buystock)]
    
   
    buy_quote = pd.merge(buy_quote,day_buy.drop(['TradingDay','SecuAbbr'],axis=1),on=['SecuCode'],how='left')
    buy_quote['signal_rank'] = buy_quote['signal_rank'].fillna(0)
    buy_quote.index  =buy_quote['TradingDay']
    
    session_timeindex = buy_quote[['TradingDay']].drop_duplicates()
    session_timeindex = session_timeindex.sort_index()
    
    if i == 0: #第一次交易，仅有买入
        for day in session_timeindex.index:
            day_buylist = buy_quote[buy_quote['TradingDay']==day]
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
                sell_holdvol = pd.Series()   
            else:
                if len(session_timeindex)>2:#防止两个调仓日期仅差一天，出现BUG的情况
                    sell_quote = session_quote[session_quote['TradingDay']==day]
                                         
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
                    run_trade(day_buylist,surplus_cash,holdvol,wait_buycash,sell_holdvol,sell_quote,holddays)
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
            #day = session_timeindex.min()[0]
            day_buylist = buy_quote[buy_quote['TradingDay']==day]
            day_buylist.index = day_buylist['SecuCode'] 
            day_benchmark_quote = benchmark[benchmark['TradingDay']==day] 
            day_bonus = bonus[bonus['ExDiviDate']==day]
            day_bonus.index = day_bonus['SecuCode']
            sell_quote = session_quote[session_quote['TradingDay']==day]
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
                sell_stock = pd.Series()
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
                    asset,tradefee,surplus_cash,holdvol,sell_holdvol,wait_buycash,stockvalue,sell_stockvalue,holddays,sell_holddays,tradedetail = \
                    adjust_first_trade(day_buylist,asset,surplus_cash,stockvalue,keep_holdvol,sell_holdvol,sell_quote,holddays)
   
                    
                    new_trade_stock = holdvol[~holdvol.index.isin(trade_fq_cp.index)].index
                    new_trade_stock =  day_buylist[day_buylist.index.isin(new_trade_stock)]['cp']
                    trade_fq_cp = trade_fq_cp.append(new_trade_stock) #新买入股票，记录买入成本价
                    trade_benchmark_cp = trade_benchmark_cp.append((1+new_trade_stock-new_trade_stock)*day_benchmark_quote['cp'][0])
                    fq_cp = day_buylist.ix[holdvol.index]['cp']
                    sell_holddays0 = sell_holddays0.append(pd.DataFrame(sell_holddays))
               
            else:
                if len(session_timeindex)>2:#防止调仓日期仅差一个交易而传输的bug
                    if stop_type is not None:
                        sell_stock,trade_fq_cp,trade_benchmark_cp,sell_holdvol,holdvol,stockvalue,wait_buycash,holddays = \
                            daily_loss_gain(fq_cp,trade_fq_cp,day_benchmark_quote,trade_benchmark_cp
                                             ,sell_holdvol,holdvol,stockvalue,wait_buycash,holddays) 
                    asset,surplus_cash,tradefee,stockvalue,holdvol,wait_buycash,sell_holdvol,holddays,tradedetail = \
                    run_trade(day_buylist,surplus_cash,holdvol,wait_buycash,sell_holdvol,sell_quote,holddays)
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
#index2 = buytime.drop_duplicates()[0]
#asset = pd.DataFrame(np.array(asset0),index=day1,columns=['净值','手续费','现金','持有证券资产','待卖证券资产'])

#holdvol0['SecuCode'] = holdvol0.index
#holdvol0['市值'] =  np.array(stockvalue0 )
#holdvol0['天数'] =  np.array(holddays0 )
#holdvol0['目标金额'] = np.array( wait_buycash0 )
#holdvol0['date']  =  np.array(buytime[0])
#   
# #插入代码和简称
#name_info = buylist.drop_duplicates(subset=['SecuCode'])[['SecuCode','SecuAbbr']]
#name_info.index = range(len(name_info))
#name_info = name_info.drop_duplicates()
#
#holdvol = pd.merge(name_info,holdvol0,on='SecuCode',how='right')
#holdvol = pd.DataFrame(np.array(holdvol),columns=['证券代码','证券简称','持仓数量','市值','天数','目标金额','日期'])
#  
#if len(selltime) > 0:
#   sell_holdvol0['SecuCode'] =  sell_holdvol0.index
#   sell_holdvol0['date'] = np.array(selltime[0])
#   sell_holdvol = pd.merge(name_info,sell_holdvol0,on='SecuCode',how='right')
#   sell_holdvol = pd.DataFrame(np.array(sell_holdvol),columns=['证券代码','证券简称','持仓数量','日期'])
#
#else:
#    sell_holdvol = pd.DataFrame(columns=['证券代码','证券简称','持仓数量','日期'])
#
#tradedetail0 = pd.merge(name_info,tradedetail0,right_index=True,left_on='SecuCode',how='right')      
    
                  
    