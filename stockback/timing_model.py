# -*- coding: utf-8 -*-
"""
Created on 20180620
择时用的公用程序

@author: chenghg
"""
import numpy as np
import pandas as pd

class timing():
    def __init__(self): #连接聚源数据库
        self.datapath = "C:\\py_data\\datacenter\\quote.h5"
        
    def performance_func(self,rtn,freq):
        '''
        rtn是log收益率
        rtn = daily_rtn
        freq=250
        rtn.info
        '''
        m_nav = np.exp(rtn.cumsum())   
#        m_nav.plot(figsize=(30,10))  
        m_nav_max = m_nav.cummax()
        m_nav_min= (m_nav /  m_nav_max - 1)  
        std = 100 * (np.exp(rtn)-1).std() * np.sqrt(freq)
        sy = 100 * (pow(np.exp(rtn.sum()),freq/rtn.count()) - 1 )
        sharpratio = (sy - 3) / std
        maxdrawdown = 100 * m_nav_min.min()
    
        performance = pd.DataFrame(np.array([sy,std,maxdrawdown,sharpratio]).T,index=sy.index,
                                 columns=['年化收益率','波动率','最大回撤','夏普比例'])
        year_sy = 100 * (np.exp(rtn.resample('a').sum()) - 1)
        by_year = m_nav_min.groupby(lambda x:x.year)
        year_maxdrawdown = by_year.min()
        cumsum_year_sy = m_nav.resample('a').last() -1 
        cumsum_year_sy.index = year_maxdrawdown.index
        year_sy.index = year_maxdrawdown.index
        return m_nav,performance,year_sy,year_maxdrawdown,cumsum_year_sy
    
    def performance_detail(self,rtn,freq,pos,data):
        '''
        计算各种指标如胜利率，平均盈利等
        rtn = daily_rtn
        freq = 250 
        data=cp:dataframe，index为日期，columns为收盘行情
        pos=signal ： 买卖信号
        '''
        holddays_ratio = 100*(pos[pos!=0].count()/ pos.count()).mean()
        trade_times = (pos[pos!=pos.shift(1)].sum()).sum() / 2
        avg_holddays = pos.count().mean()/ trade_times 
        windays_ratio = 100*(rtn[rtn>0].count()/rtn[rtn!=0].count()).values[0]
        win_loss_ratio = (rtn[rtn>0].mean() / abs(rtn[rtn<0].mean())).values[0]
        #---计算每次的表现------------------------------------------------------
        win_times =  pos[(pos!=pos.shift(1))|(pos!=pos.shift(-1))]
        win_times = pd.DataFrame(win_times.stack())
        temp_cp =  pd.DataFrame(data.stack())
        win_times = pd.merge(win_times,temp_cp,left_index=True,right_index=True,how='left')
        win_times['date'] = pd.DataFrame(win_times.index)[0].apply(lambda x:x[0]).values
        win_times['code'] = pd.DataFrame(win_times.index)[0].apply(lambda x:x[1]).values
        win_times = win_times.sort_values(['code','date'])
        win_times['nextdate'] = win_times['date'].shift(-1)
        win_times['0_z'] = np.where(win_times['code']==win_times['code'].shift(-1),
                                     win_times['0_y'].shift(-1),np.nan)
        win_times['temp'] = np.where(win_times['code']==win_times['code'].shift(-1),
                                     win_times['0_x'].shift(-1),np.nan)
        win_times = win_times[(win_times['0_x']==1)&(win_times['temp']==1)]
        win_times['rtn'] = win_times['0_x']*(win_times['0_z']/ win_times['0_y'] -1)-0.003
        #开始计算
        max_sy = 100*win_times['rtn'].max()
        min_sy = 100*win_times['rtn'].min()
        wins =  win_times[win_times>0]['rtn'].count()
        loss =  win_times[win_times<0]['rtn'].count()
        wins_ratio = 100*wins / (wins+loss) 
        avg_wins = 100*win_times[win_times>0]['rtn'].mean()
        avg_loss = 100*win_times[win_times<0]['rtn'].mean()
        win_loss_ratio2 = avg_wins / abs(avg_loss)
        
        m_nav = np.exp(rtn.cumsum())   
        m_nav_max = m_nav.cummax()
        m_nav_min= (m_nav /  m_nav_max - 1)  
        std = 100 * (np.exp(rtn)-1).std() * np.sqrt(freq)
        sy = (100 * (pow(np.exp(rtn.sum()),freq/rtn.count()) - 1 )).values[0]
        sharpratio = ((sy - 3) / std).values[0]
        maxdrawdown = (100 * m_nav_min.min()).values[0]
        
        performance = pd.DataFrame(np.array([[sy,std,maxdrawdown,sharpratio,holddays_ratio,
                                             trade_times,avg_holddays,windays_ratio,
                                             win_loss_ratio,wins,loss,max_sy,min_sy,
                                             wins_ratio,avg_wins,avg_loss,win_loss_ratio2]]),
                                 index = std.index,
                                 columns=['年化收益率%','波动率%','最大回撤%','夏普比例',
                                          '持仓天数占比%','交易次数','平均持仓天数',
                                          '获利天数占比%(日)','平均盈亏比(日)','盈利次数',
                                          '亏损次数','单次最大盈利%','单次最大亏损%',
                                          '胜利率%(次)','平均盈利%(次)','平均亏损%(次)','盈亏比(次)'])
        
        year_sy = 100 * (np.exp(rtn.resample('a').sum()) - 1)
        by_year = m_nav_min.groupby(lambda x:x.year)
        year_maxdrawdown = by_year.min()
        cumsum_year_sy = m_nav.resample('a').last() -1 
        cumsum_year_sy.index = year_maxdrawdown.index
        year_sy.index = year_maxdrawdown.index
        
        year_performance = pd.DataFrame(pd.concat([year_sy,year_maxdrawdown,cumsum_year_sy],
                                          axis=1).values,index=year_sy.index,
                                            columns=['年收益','年最大回撤','累计收益'])   

        return m_nav,performance,year_performance,win_times
        
    
    def backtest(self,data,signal,fee=True):
        '''
        该择时回溯框架，注意点：
        1. 信号不是开仓信号，而是第二天的买入信号；signal输入时，记得已经shift(1)过
        2. 采取收盘价开仓方式；
        3. fee为True,代表计算手续费，手续费按照单边千分之1.5计算；False不计算手续费
        4. 该框架，可以计算多个股票的效果，多个股票由仓位，按照每日收益率计算组合的平均收益；
        5. 计算每日收益时，已经考虑了每只股票的复权效应。
        
        data = cp
        signal = pos
        回溯计算
        index:DataFrame,index为日期格式，columns为指复权价格；
        signal:DataFrame,index为日期格式，columns为每日的择时信号，-1、1、0，且已经lag后
        '''
        rtn0 = data/data.shift(1) - 1
        rtn = np.log(data/data.shift(1))
        signal0 = signal.shift(1)
        sy = rtn * signal0
        sumsy = np.exp(sy.cumsum())
        adjust = signal.fillna(0)
        adjust = pd.DataFrame(np.where((adjust!=0)&(adjust.shift(1)!=adjust),sumsy,
                            np.where(signal0!=0,np.nan,1)),index=data.index,columns=data.columns)
        adjust = adjust.fillna(method='ffill')
       
        rtn3 = sumsy / adjust
        rtn4 =  rtn3/rtn3.shift(1) -1 
        rtn4 = pd.DataFrame(np.where((signal!=0)&(signal.shift(1)!=signal),rtn0*signal0,rtn4),
                           index=data.index,columns=data.columns )
        rtn4 = pd.DataFrame(np.where(signal0==0,0,rtn4),index=data.index,columns=data.columns)
        #计算手续费
        if fee==True:
            rtn5 = pd.DataFrame(np.where(abs(signal-signal.shift(1))>1,rtn4-0.0015*2,
                             np.where(abs(signal-signal.shift(1))==1,rtn4-0.0015,rtn4)),
                             index=data.index,columns=data.columns)
            
        else:
            rtn5 = rtn4
#        aa2 = pd.concat([signal,signal0,adjust,rtn0,sy,sumsy,rtn4,rtn5],axis=1)
#        aa2.columns = [['signal','signal0','adjust','rtn0','sy','sumsy','rtn4','rtn5']]
        if len(rtn5.columns) >1:
            rtn5 = pd.DataFrame(np.where((abs(signal0)==1)|(abs(signal==1)),rtn5,np.nan),index=rtn5.index,columns=rtn5.columns)
            rtn5 = rtn5.mean(axis=1)
           
        rtn5 = rtn5.fillna(0)
        daily_rtn = pd.DataFrame(np.log(1+rtn5)) 
        
        m_nav,performance,year_performance,signal_times = self.performance_detail(daily_rtn,250,signal,data)
        return  m_nav,performance,year_performance,signal_times


