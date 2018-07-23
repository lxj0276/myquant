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
        pass
        
    def performance_func(self,rtn,freq):
        '''
        rtn是log收益率
        '''
        m_nav = np.exp(rtn.cumsum())   
        m_nav.plot(figsize=(30,10))  
        m_nav_max = m_nav.cummax()
        m_nav_min= (m_nav /  m_nav_max - 1)  
        std = 100 * (np.exp(rtn)-1).std() * np.sqrt(freq)
        sy = 100 * (pow(np.exp(rtn.sum()),freq/rtn.count()) - 1 )
        sharpratio = (sy - 3) / std
        maxdrawdown = 100 * m_nav_min.min()
    
        performance = pd.DataFrame(np.array([sy,std,maxdrawdown,sharpratio]).T,index = sy.index,
                                 columns=['年化收益率','波动率','最大回撤','夏普比例'])
        year_sy = 100 * (np.exp(rtn.resample('a',how='sum')) - 1)
        by_year = m_nav_min.groupby(lambda x:x.year)
        year_maxdrawdown = by_year.min()
        cumsum_year_sy = m_nav.resample('a',how='last') -1 
        
        return m_nav,performance,year_sy,year_maxdrawdown,cumsum_year_sy
    
    def backtest(self,index,signal,fee=True):
        '''
        该择时回溯框架，注意点：
        1. 信号不是开仓信号，而是第二天的买入信号；signal输入时，记得已经shift(1)过
        2. 采取收盘价开仓方式；
        3. fee为True,代表计算手续费，手续费按照单边千分之1.5计算；False不计算手续费
        4. 该框架，可以计算多个股票的效果，多个股票由仓位，按照每日收益率计算组合的平均收益；
        5. 计算每日收益时，已经考虑了每只股票的复权效应。
        
        data = index[['上证50']]
        signal = index[['pos']].shift(1)
        signal['上证50'] = signal['pos']
        signal = signal[['上证50']]
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
        aa2 = pd.concat([signal,signal0,adjust,rtn0,sy,sumsy,rtn4,rtn5],axis=1)
        aa2.columns = [['signal','signal0','adjust','rtn0','sy','sumsy','rtn4','rtn5']]
       
        if len(rtn5.columns) >1:
            rtn5 = rtn5.mean(axis=1)
        daily_rtn = np.log(1+rtn5)
        
        m_nav,performance,year_sy,year_maxdrawdown,cumsum_year_sy = performance_func(daily_rtn,250)
        return  m_nav,performance,year_sy,year_maxdrawdown,cumsum_year_sy


