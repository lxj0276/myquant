# -*- coding: utf-8 -*-
"""
Created on Tue Jun 14 14:47:03 2016
多因子计算的父类最开始程序
其中
__init__：连接数据库
get_交易日期：获取交易日期
get_上市状态变更:获取上市公司的上市状态
get_info:获取A股代码、innercode、companycode是公用的函数
create_newdata:创建表
1. 该数据用的是新会计准则，因此回溯时，不能从2007年之前开始，得从2008年之后开始 
2. finance_getinfo_rank这个函数中，由于对指标进行了填充，取最新值，因此指标未来不能做横向处理，
    比如中国平安20170331，非经常损益值为空，但我们取了上一期20161231或者20160930的值，
    这个时候计算扣非净利润，不能用20170331的净利润-本期非经常性损益，而是在finance_getinfo_rank前就要进行计算，请注意。


"""
import pandas as pd
import numpy as np
import pymysql
import datetime
from tqdm import *
    
class public:
    '''
    数据源:聚源数据库
    get_bonus:提取聚源数据库分红数据
    get_price:提取聚源数据库的行情数据  

    '''
    
    def __init__(self): #连接聚源数据库
#        self._dbengine =  pymysql.connect(host='backtest.invesmart.net',
#                           port=3308,
#                           user='jydb',
#                           password='Jydb@123465',
#                           database='jydb',
#                           charset='gbk')
        self._dbengine =  pymysql.connect(host='192.168.1.139',
                           port=3306,
                           user='jydb',
                           password='jydb',
                           database='jydb',
                           charset='gbk')
        self.datapath  = "C:\\py_data\\datacenter\\quote.h5"   
        self.datapath2  = "C:\\py_data\\datacenter"   
        
    def 行业中性处理(self,data,indicator,industryname):
        '''
        行业中性化处理，采取中位数去极值法
        indicator:需要有industyrname名称，返回处理后的data值
        data = temp_value
        indicator = 'pb'
        industryname = 'FirstIndustryName'
        '''
        #中位数去极值法，进行极值处理
        industry_median = data.groupby([industryname])[[indicator]].median()
        industry_median[industryname] = industry_median.index
        data = pd.merge(data,industry_median,on=industryname)
        data['median'] =  abs(data['%s_x'%indicator] - data['%s_y'%indicator])
        median = data.groupby([industryname])[['median']].median()
        median[industryname] = median.index
        data = pd.merge(data,median,on=industryname)

        data['value_up'] = data['%s_y'%indicator] + 3*abs(data['median_y'] )
        data['value_down'] = data['%s_y'%indicator] - 3*abs(data['median_y'] )
        data['value'] = np.where(data['%s_x'%indicator]>data['value_up'],data['value_up'],
                            np.where(data['%s_x'%indicator]<data['value_down'],data['value_down'],data['%s_x'%indicator]))
        #进行标准化
        mean = data.groupby([industryname])[['value']].mean()
        mean[industryname] = mean.index
        data = pd.merge(data,mean,on=industryname)
        std = data.groupby([industryname])[['value_x']].std()
        std[industryname] = std.index
        data = pd.merge(data,std,on=industryname)
        data['value2'] =  (data['value_x_x'] - data['value_y'])/ data['value_x_y']             
        return data['value2'].values
    
    def finance_getinfo_rank(self,data,info,fill=False):
        '''
        财务数据的处理
        1. 获得innercode\secucode等字段
        3. TTM可能会用到上市前数据，这里可以忍受，进行忽略
        4. 对指标进行排序，以保证正确顺序，我们能够
        data:需要处理的财务数据
        info：innercode、sucucode\companycode等信息
        ListedDate:A股上市日期
        '''
        data = pd.merge(data,info[['InnerCode','CompanyCode','SecuCode','ListedDate']],on='CompanyCode')       
        #data = data[data['EndDate']>data['ListedDate']]
        data = data.sort_values(['CompanyCode','EndDate','InfoPublDate'],ascending=True)
        ##对应提取的基本面数据，若是空值，则取上一期的数组！！！
        if fill == True:
            code = data['CompanyCode']
            data = data.groupby(['CompanyCode']).fillna(method='ffill')
            data['CompanyCode'] = code
        return data
    
    
    def get_常规数据(self):
         #获取上市日期\代码\简称等数据
        info = pd.read_hdf(self.datapath2+"\\info.h5",'info',columns=['InnerCode','SecuCode','CompanyCode','ListedDate'])
        #获取上市状态变更，如退市、暂停上市等数据
        listedstate = pd.read_hdf(self.datapath2+"\\info.h5",'listedstate')    
        #获取ST情况
        st = pd.read_hdf(self.datapath2+"\\info.h5",'st')
        #获取停牌复牌表
        suspend =  pd.read_hdf(self.datapath2+"\\info.h5",'suspend',columns=['InnerCode','InfoPublDate','SuspendDate','ResumptionDate'])
        #h获取解禁数据
        lift = pd.read_hdf(self.datapath2+"\\info.h5",'lift',columns=[['InnerCode','InitialInfoPublDate','StartDateForFloating','Proportion1']])
        return info,st,listedstate,suspend,lift
    
    def get_常规剔除(self,info,st,suspend,listedstate,lift,date,days,dellift=False):
        '''
        info、st、suspend、listedstate分别为info数据、st数据、停复牌数据、上市状态变更数据
        date:当期的日期
        days:上市天数
        每一期选股的常规剔除模型,返回剔除以下后的股票信息
        1. 上市日期大于listeddays 默认365天
        2. 非ST、*ST、暂停上市、退市整理、高风险预警等股票
        3. 非停牌
        4. 非退市、暂停上市、重组退市等股票
        备注：
        由于聚源的停牌复盘表没有保存停牌复盘的公告日期，因此，统一采用日期变更日前40日即剔除该标的，
        有误差，但目前只能采取该方案
        '''
        #上市日期大于1年,
        temp_info =info[date>=info['ListedDate']+datetime.timedelta(days)] 
        #非ST、*ST、暂停上市、退市整理、高风险预警等股票
        temp_st = st[(date>=st['InfoPublDate'])]
        temp_st = temp_st.drop_duplicates(['InnerCode'],keep='last')
        temp_st = temp_st[~temp_st['SpecialTradeType'].isin((2,4,6))]
        #停牌股票
        temp_suspend = suspend[date>=suspend['InfoPublDate']]
        temp_suspend = temp_suspend.drop_duplicates(['InnerCode'],keep='last')
        temp_suspend = temp_suspend[(date<=temp_suspend['ResumptionDate'])|(temp_suspend['ResumptionDate']=='19000101')]
        #退市、暂停交易等股票
        temp_listedstate = listedstate[date>=listedstate['ChangeDate']-datetime.timedelta(40)]
        temp_listedstate = temp_listedstate.drop_duplicates(['InnerCode'],keep='last')
        temp_listedstate = temp_listedstate[~(temp_listedstate['ChangeType'].isin((1,3)))] 
       
        #最终排除
        temp_info = temp_info[~temp_info['InnerCode'].isin(temp_st['InnerCode'])]
        temp_info = temp_info[~temp_info['InnerCode'].isin(temp_suspend['InnerCode'])]
        temp_info = temp_info[~temp_info['InnerCode'].isin(temp_listedstate['InnerCode'])]
        temp_info = temp_info[~temp_info['InnerCode'].isin(temp_listedstate['InnerCode'])]
        if dellift==True:
            #解禁,解禁前2个月，后40天，比例占流通股本超过8%的个股进行剔除
            temp_lift = lift[(date>=lift['InitialInfoPublDate'])]
            temp_lift = temp_lift[(date<=temp_lift['InitialInfoPublDate']+datetime.timedelta(40))&(date>=temp_lift['StartDateForFloating']-datetime.timedelta(60))]
            temp_lift = temp_lift[temp_lift['Proportion1']>8]
            temp_info = temp_info[~temp_info['InnerCode'].isin(temp_lift['InnerCode'])]
        return temp_info
        
    
    def get_同比(self,data,indicator):
        '''
        计算财务指标的同比,需要有EndDate、CompanyCode字段
        '''
        data['lastdate'] = data['EndDate'].apply(lambda x:datetime.datetime(x.year-1,x.month,x.day))
        data['temp'] = data[indicator]
        data = pd.merge(data,data[['EndDate','CompanyCode','temp']],
                               left_on=['CompanyCode','lastdate'],right_on=['CompanyCode','EndDate'],how='left')        
        data['同比'] = np.where(data['temp_y']!=0,data[indicator]/data['temp_y']-1,np.nan)
        return data['同比'].values
    
    def get_环比(self,data,indicator):
        '''
        计算财务指标环比，需要有EndDate、CompanyCode字段
        只能计算单季度值，或者TTM值
        data = temp_OperatingRevenue
        indicator = '营业收入TTM'
        '''
        data['month'] = data['EndDate'].apply(lambda x:x.month)
        data['predate'] = np.where(data['month']==3,data['EndDate'].apply(lambda x:datetime.datetime(x.year-1,12,31)),
                            np.where(data['month']==6,data['EndDate'].apply(lambda x:datetime.datetime(x.year,3,31)),
                                np.where(data['month']==9,data['EndDate'].apply(lambda x:datetime.datetime(x.year,6,30)),
                                         data['EndDate'].apply(lambda x:datetime.datetime(x.year,9,30)))))
        data['temp'] = data[indicator]
        data = pd.merge(data,data[['CompanyCode','EndDate','temp']],left_on=['predate','CompanyCode'],
                                right_on = ['EndDate','CompanyCode'],how='left')
        data['环比'] = np.where(data['temp_y']!=0,data[indicator]/data['temp_y']-1,np.nan)
        return data['环比'].values
    
    def get_N年复合增长率(self,data,indicator,N):
        '''
        计算财务指标的同比,需要有EndDate、CompanyCode字段
        n:几年
        ntype:类型，复合还是算绝对
        n=3
        '''
        data['lastdate'] = data['EndDate'].apply(lambda x:datetime.datetime(x.year-N,x.month,x.day))
        data['temp'] = data[indicator]
        data = pd.merge(data,data[['EndDate','CompanyCode','temp']],
                               left_on=['CompanyCode','lastdate'],right_on=['CompanyCode','EndDate'],how='left')        
        data['增长率'] = np.where(data['temp_y']!=0,(data[indicator]/abs(data['temp_y']))**(1/N)-1,np.nan)
        return data['增长率'].values 
    
    def get_ttm(self,data,indicator):
        '''
        财务数据，当期数据计算该指标的TTM值
        需要有CompanyCode，EndDate，InfoPublDate字段
        需要：已经排序并且删除了重复数据
        data = data.sort_values(['CompanyCode','EndDate','InfoPublDate'])
        data = data.drop_duplicates(['CompanyCode','EndDate'],keep='last')
        indicator:需要计算的指标，如‘归属母公司的净利润’
        data = temp_profit
        indicator = 'TotalOperatingRevenue'
        ''' 
        data['month'] = data['EndDate'].apply(lambda x:x.month)  
        data['lastdate'] = data['EndDate'].apply(lambda x:datetime.datetime(x.year-1,x.month,x.day))
        data['yeardate'] = data['EndDate'].apply(lambda x:datetime.datetime(x.year-1,12,31))
        data['temp'] = data[indicator]
        data = pd.merge(data,data[['EndDate','CompanyCode','temp']],
                               left_on=['CompanyCode','yeardate'],right_on=['CompanyCode','EndDate'],how='left')        
        data = pd.merge(data,data[['EndDate_x','CompanyCode','temp_x']],
                               left_on=['CompanyCode','lastdate'],right_on=['CompanyCode','EndDate_x'],how='left')  
        data['TTM'] = np.where(data['month']!=12,data[indicator]+data['temp_y']-data['temp_x_y'],data[indicator])
        return data['TTM'].values
    
    def get_单季值(self,data,indicator):
        '''
        财务数据，当期数据计算该指标单季值
        需要有CompanyCode，EndDate，InfoPublDate字段
        需要：已经排序并且删除了重复数据
        data = data.sort_values(['CompanyCode','EndDate','InfoPublDate'])
        data = data.drop_duplicates(['CompanyCode','EndDate'],keep='last')
        indicator:需要计算的指标，如‘归属母公司的净利润’
        ''' 
        data['month'] = data['EndDate'].apply(lambda x:x.month)
        data['predate'] = np.where(data['month']==3,data['EndDate'].apply(lambda x:datetime.datetime(x.year-1,12,31)),
                            np.where(data['month']==6,data['EndDate'].apply(lambda x:datetime.datetime(x.year,3,31)),
                                np.where(data['month']==9,data['EndDate'].apply(lambda x:datetime.datetime(x.year,6,30)),
                                         data['EndDate'].apply(lambda x:datetime.datetime(x.year,9,30)))))
        
        data['temp'] = data[indicator]
        data = pd.merge(data,data[['CompanyCode','EndDate','temp']],left_on=['predate','CompanyCode'],
                                right_on = ['EndDate','CompanyCode'],how='left')
        data['单季值'] = np.where(((data['month']==3)|(data['month']==12)),data[indicator],
                            data[indicator]-data['temp_y'])
        return data['单季值'].values

        
    
    def get_industry(self,standard):
        '''
        #standard:'(3)':中信、'(9，24)'申万
        #3:中信 9：申万老版 24：申万2014版 19：银华自定义分类
#        sql = "select CompanyCode,InfoPublDate,Industry,FirstIndustryName,SecondIndustryName,\
#                ThirdIndustryName,FourthIndustryName,Standard,CancelDate from  LC_ExgIndustry where \
#                Standard in "+standard+" "
        standard='(9,24)'
        最终进行排序
        '''
        industry = pd.read_hdf(self.datapath2+'\\info.h5','industry',where="Standard in "+standard+"") 
        industry = industry.sort_values(['CompanyCode','InfoPublDate'],ascending=True) #排序
        return industry
    
    
    def get_财务股本表(self,sheetname,startdate,columns=[]):
        '''
        获取本地finance.h5中的财务数据
        1. 资产负债表_新会计准则 LC_BalanceSheetAll
        2. 利润分配表_新会计准则  LC_IncomeStatementAll
        3. 现金流量表_新会计准则 LC_CashFlowStatementAll
        4. 单季利润表_新会计准则 LC_QIncomeStatementNew
        5. 单季现金流量表_新会计准则 LC_QCashFlowStatementNew
        6. 公司股本变动表 LC_ShareStru
        '''
        names = list(['InfoPublDate','CompanyCode','EndDate'])
        names.extend(columns)
        if sheetname == 'LC_ShareStru':
            data = pd.read_hdf(self.datapath2+'\\info.h5','LC_ShareStru',where="InfoPublDate>="+startdate+"",columns=names)
        else:
            data = pd.read_hdf(self.datapath2+'\\%s.h5'%sheetname,'data',where="InfoPublDate>="+startdate+"",columns=names)
        return data
        
    
    def get_nextrtn(self,buylist,quote):
        '''
        获取buylist每期选股的下一期收益率
        buylist,dataframe有TradingDay、SecuCode、因子值等字段
        quote,dataframe有TradingDay、SecuCode、fq_cp等字段
        '''
        time0 = buylist[['TradingDay']].drop_duplicates()
        quote0 = quote[quote['TradingDay'].isin(time0['TradingDay'])]
        quote0 = quote0.sort_values(['SecuCode','TradingDay'])
        quote0['rtn'] = np.where(quote0['SecuCode']==quote0['SecuCode'].shift(1),
                              quote0['fq_cp'] /quote0['fq_cp'].shift(1)-1,np.nan)
        quote0['next_rtn'] = np.where(quote0['SecuCode']==quote0['SecuCode'].shift(-1),
                              quote0['rtn'].shift(-1),np.nan)
        buylist0 = buylist.sort_values(['TradingDay','SecuCode'])
        buylist0  = pd.merge(buylist0,quote0[['TradingDay','SecuCode','next_rtn']],on=['SecuCode','TradingDay'],how='left')
        return buylist0
          
    
    def ic_rtnk(self,buylist0,factor):
        '''
        计算IC即ICrank值
        buylist0 dataframe 有有TradingDay、next_rtn、因子值数据
        '''
        #factor = '总市值'
        corr = pd.DataFrame()
        time0 = buylist0[['TradingDay']].drop_duplicates()
        for i in range(len(time0)):
            date = time0.iloc[i]['TradingDay']
            data = buylist0[buylist0['TradingDay']==date]
            data = data.dropna(how='any',axis=0)
            rank_ic = data[[factor,'next_rtn']].rank().corr().ix[0][1]
            init_ic = data[[factor,'next_rtn']].corr().ix[0][1]
            corr = corr.append(pd.DataFrame([[date,init_ic,rank_ic]]))
        corr = pd.DataFrame([[date,corr.mean()[1],corr.mean()[2]]]).append(corr)   
        return corr.iloc
        
        
        
    