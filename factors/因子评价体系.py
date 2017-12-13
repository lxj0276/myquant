# -*- coding: utf-8 -*-
"""
Created on Tue Nov 28 11:03:10 2017

@author: dylan
"""

#from factor0 import *
import pandas as pd
import numpy as np
import pymysql
import datetime
import scipy.stats as stats
class factor_evaluate():
    
    def __init__(self,cycle,startdate,enddate):
        '''
        cycle:因子评级的平度，有天/周/月等
        '''
        self._dbengine =  pymysql.connect(host='192.168.1.139',
                           port=3306,
                           user='jydb',
                           password='jydb',
                           database='jydb',
                           charset='gbk')
         
        self._dbengine1 =  pymysql.connect(host='192.168.1.139',
                           port=3306,
                           user='jydb',
                           password='jydb',
                           charset='gbk')
        self.cycle = cycle
        self.startdate = startdate
        self.enddate = enddate
        
    
    def get_因子数据(self,tablename,sheetname,indicators):
        '''
        获取因子库中的财务数据
        tablename = 'test'
        sheetname = 'pb'
        startdate = '20110101'
        enddate = '20110301'
        indicators = ['归母净利润同比','归母净利润同比的环比']
        '''
        #factors = ['secucode','secuabbr']
        indicators = str(indicators).replace("[","")
        indicators = indicators.replace("]","")
        indicators = indicators.replace("'","")
        sql = "select TradingDay,SecuCode,"+indicators+" from %s.%s where TradingDay>=%s and\
               TradingDay<=%s "%(tablename,sheetname, startdate, enddate)
        sql = "select TradingDay,SecuCode,"+indicators+" from %s.%s where TradingDay>=%s and\
               TradingDay<=%s "%(tablename,sheetname,self.startdate,self.enddate)
        if sheetname == 'sw_industry' or sheetname == 'zx_industry':
            sql = "select TradingDay,SecuCode,"+indicators+" from %s.%s"%(tablename,sheetname)
        data = pd.read_sql(sql,con=self._dbengine1)
        return data
    
    def get_equityquoe(self):
        #获取行情数据,获取至最新行情
        enddate2 = pd.to_datetime(self.enddate)+datetime.timedelta(32)
        enddate2 =  datetime.datetime.strftime(enddate2,"%Y%m%d")
        sql = "select B.SecuCode,B.InnerCode,B.CompanyCode\
                ,A.TradingDay,A.OpenPrice as op,A.ClosePrice as cp,A.HighPrice as hp,A.LowPrice as lp,\
                A.PrevClosePrice as precp,(A.ClosePrice * ifnull((select AdjustingFactor from QT_AdjustingFactor as O1 where  \
                O1.ExDiviDate <= A.TradingDay and O1.InnerCode = A.InnerCode order by O1.ExDiviDate desc limit 1),1))  as fq_cp,\
                A.TurnoverVolume as vol,A.TurnoverValue,A.TurnoverDeals from QT_DailyQuote  A \
                inner join  (select * from SecuMain where SecuCategory=1 and SecuMarket in (83,90)) as B on\
                A.InnerCode = B.InnerCode where TradingDay >=STR_TO_DATE("+self.startdate+",'%Y%m%d')\
                and TradingDay <=STR_TO_DATE("+enddate2+",'%Y%m%d') order by SecuCode,TradingDay" 
        quote = pd.read_sql(sql,con=self._dbengine)  
        print("行情数据获取完毕...")
        return quote
    
    def get_数据合并(self,data1,data2):
        '''
        两列日期对不上数据的合并程序
        取data1中小于该日期的，最新的data2中的值
        比如data1：行情数据，有TradingDay、SecuCode、cp等数据;data2:行业数据，有行业一级等字段，该字段随TradingDay不定期改变
        目的是合并data1,data2,在data1中每一天，都能够取到data2最新行业分类
   
        '''
        data = pd.merge(data1,data2,on=['SecuCode'],how='outer')
        data = data[data['TradingDay_x']>=data['TradingDay_y']]
        data = data.sort_values(['SecuCode','TradingDay_x','TradingDay_y'])
        data = data.drop_duplicates(['SecuCode','TradingDay_x'],keep='last')
        data['TradingDay'] =  data['TradingDay_x']
        data = data.drop(['TradingDay_y','TradingDay_x'],axis=1)
        return data
    
    def get_行情数据清洗(self,quote,industry):
        '''
        转为月度【指定频度】数据，并合并行情和行业分类数据
        1.先转为指定频度数据（月）数据，计算下一期收益率
        2.合并行业数据
           '''
             #转为月度数据
        time = quote[['TradingDay']].drop_duplicates()
        time = time.resample(gp.cycle,on='TradingDay').last()
        quote2 = quote[quote['TradingDay'].isin(time['TradingDay'])]
        #quote2['next_rtn'] = np.where(quote2['SecuCode']==quote2['SecuCode'].shift(-1),quote2['fq_cp'].shift(-1)/quote2['fq_cp']-1,np.nan)
        for i in range(1,7):
            quote2['next%s'%str(i)] = np.where(quote2['SecuCode']==quote2['SecuCode'].shift(-i),quote2['fq_cp'].shift(-i)/quote2['fq_cp']-1,np.nan)
        data = self.get_数据合并(quote2,industry)
        print("数据清洗完毕...")
        return data        
    
    def get_rankic(self,quote,indicator,industry_name=None):
        '''
        indicator = 'pettm'
        获取每一期的相关系数，rankic值,ICdecay值
        industry:非空时，则计算该指标的行业调整后的IC等值
        industry_name ='行业一级'
        indicator = '归母净利润同比'
        '''
        
        #quote3 = quote[['SecuCode','TradingDay','next1',indicator]]
        quote3 =  quote[['SecuCode','TradingDay','next1','next2','next3','next4','next5','next6',indicator]]
        if industry_name is not None:
           #quote3 = quote[['SecuCode','TradingDay','next1',indicator,industry_name]]
           quote3 =  quote[['SecuCode','TradingDay','next1','next2','next3','next4','next5','next6',indicator,industry_name]]
           median = quote3.groupby(['TradingDay',industry_name]).median()
           median['median'] = median[indicator]
           median['TradingDay']= pd.DataFrame(median.index)[0].apply(lambda x:x[0]).values
           median[industry_name] = pd.DataFrame(median.index)[0].apply(lambda x:x[1]).values
           quote3 = pd.merge(quote3,median[[industry_name,'TradingDay','median']],on=[industry_name,'TradingDay'],how='left')
           quote3['行业调整_%s'%indicator] = quote3[indicator]/ abs(quote3['median'])
           quote3 = quote3.drop(['median',industry_name1],axis=1)
        #获取IC、ir标准差等值
        group = quote3.drop(['next2','next3','next4','next5','next6'],axis=1).groupby(['TradingDay'])
        corr = group.corr(method='spearman')#秩相关系数
        corr['TradingDay'] = pd.DataFrame(corr.index)[0].apply(lambda x:x[0]).values
        corr =  corr.drop_duplicates(['TradingDay']) 
        corr =  corr.drop(['next1'],axis=1) 
        meanic = corr.mean()
        stdic = corr.std()
        minic = corr.drop(['TradingDay'],axis=1).min()
        maxic = corr.drop(['TradingDay'],axis=1).max()
        icir = meanic/stdic
        gl = 100*(corr[corr>0].count()/corr.count()).drop(['TradingDay'],axis=0) 
        result = pd.concat([meanic,stdic,minic,maxic,icir,gl],axis=1)
        result.columns = ['IC均值','IC标准差','IC最小值','IC最大值','ICIR','IC大于0的概率%'] 
        #获取因子decay
        group2 = quote3.groupby(['TradingDay'])
        corr2 = group2.corr(method='spearman')#秩相关系数
        #corr2['TradingDay'] = pd.DataFrame(corr2.index)[0].apply(lambda x:x[0]).values
        corr2['decay'] = pd.DataFrame(corr2.index)[0].apply(lambda x:x[1]).values
        corr2 =  corr2[corr2['decay'].isin(['next1','next2','next3','next4','next5','next6'])]
        corr2 =  corr2.drop(['next1','next2','next3','next4','next5','next6'],axis=1) 
        decay = corr2.groupby(['decay']).mean().T
        return corr,result,decay
    
if __name__ == "__main__":
    gp = factor_evaluate('m','20121231','20161231') 
    #提取行情、行业数据，并清洗合并
#    quote = gp.get_equityquoe()
#    industry_name = ['行业一级'] 
#    industry = gp.get_因子数据('test','sw_industry',industry_name)#获取申万行业数据   
    quote2 = gp.get_行情数据清洗(quote,industry) #提前数据行业分类数据
    
   
    #参数设置，提取因子数据
    tablename = 'test'
    sheetname = 'profit'
    indicator = ['归母净利润同比','单季归母净利润环比']
    factor = gp.get_因子数据(tablename,sheetname,indicator) #获取因子数据
    #转为设定频度数据
    quote3 = gp.get_数据合并(quote2,factor)
    #计算因子IC、ICIR值
    ic_corr,ic,ic_decay = gp.get_rankic(quote3,indicator[1],industry_name[0])
    
    
    
    
    