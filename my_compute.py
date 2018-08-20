# -*- coding: utf-8 -*-
"""
Created on Wed Mar 30 15:56:12 2016

@author: chenghg
"""
import pandas as pd
import numpy as np

  
def rank_to_group(data,groupnum):
    '''
    本函数，计算排名百分比，并进行分组，要求datafram(M * N),index=datetime64,columns=相同类[如股票代码]
    ascending: 默认由高到低排名，数值越大，排名百分比月考前，否则=True,数值越大，排名越高 
    axis: 默认按照列值进行排序
    groupnum: 均等分组数 int 如5组                
    '''
    group = np.linspace(0,1,groupnum+1)
    rank = data.rank(1,ascending=False)
    count_rank = rank.max(1)
    rank = rank.div(count_rank,0)   
    for i in range(groupnum):
        rank[(rank>group[i]) & (rank<=group[i+1])] = int((i+1) * groupnum)  
    rank  = rank / groupnum
    return rank
#tii1 = rank_to_group(msharp,10)                     

#分组计算,rank3：原排名，data:数据 lagvalue:滞后阶数
def future_rank(ranks,data,other_data,lagvalue):
    '''
    本函数，计算某指标进入特定排名，未来N个阶数的排名情况,要求ohter_data.shape == data.shape
    ranks:原排名 int 如10 - 20名
    lagvalue:滞后阶数，如未来3个月
    data：初始数据，如滚动12个月夏普比例排名数据，datafram(M * N),Index=datetime64,columns=相同类[如股票代码]
    other_data: 新数据，如滚动3个月夏普比例排名数据，M* N,Index=time,columns=股票代码
                当data = other_data，lagvalue==3,计算未来3个与，12个月滚动夏普排名
                当data != other_data,lagvalue=3,则是计算未来3个月，3个月滚动夏普排名
    '''
    rankdata = data[data == ranks]        
    datashift = other_data.shift(-lagvalue)    
    future_rank =  rankdata - ranks + datashift
    #future_rank = newdata1 - newdata + ranks
    return future_rank

def performance_func(rtn,freq):
    '''
    本函数计算dataframe格式的表现情况,其中dataframe至少有两列数据
    rtn是log收益率
    freq:频率，日取250，月取12
    '''
    m_nav = np.exp(rtn.cumsum()) 
    lastnet = 100 * (np.exp(rtn.sum()) - 1)
    #m_nav.plot(figsize=(30,10))  
    m_nav_max = m_nav.cummax()
    m_nav_min= (m_nav /  m_nav_max - 1)  
    std = 100 * (np.exp(rtn)-1).std() * np.sqrt(freq)
    sy = 100 * (pow(np.exp(rtn.sum()),freq/rtn.count()) - 1 )
    sharpratio = (sy - 3) / std
    maxdrawdown = 100 * m_nav_min.min()
    calmar = sy / abs(maxdrawdown)
    performance = pd.DataFrame(np.array([lastnet,sy,std,maxdrawdown,sharpratio,calmar]).T,index = sy.index,
                             columns=['累计收益','年化收益率','波动率','最大回撤','夏普比例','calmar比率'])
    year_sy = 100 * (np.exp(rtn.resample('a',how='sum')) - 1)
    #by_year = m_nav_min.groupby(lambda x:x.year)
    by_year = rtn.groupby(lambda x:x.year)
    group_rtn = np.exp(by_year.cumsum())
    group_rtn = group_rtn / group_rtn.groupby(lambda x:x.year).cummax() - 1
        
    year_maxdrawdown = group_rtn.groupby(lambda x:x.year).min()
    #year_maxdrawdown = year_maxdrawdown.min()
    
    cumsum_year_sy = m_nav.resample('a',how='last') -1 
    
    return m_nav,performance,year_sy,year_maxdrawdown,cumsum_year_sy
 
    