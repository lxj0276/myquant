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
import statsmodels.api as sm
    
class public:
    '''
    数据源:聚源数据库
    get_bonus:提取聚源数据库分红数据
    get_price:提取聚源数据库的行情数据  

    '''
    
    def __init__(self): #连接聚源数据库
        self._dbengine =  pymysql.connect(host='192.168.1.139',
                           port=3306,
                           user='jydb',
                           password='jydb',
                           database='jydb',
                           charset='gbk')
        self.datapath  = "C:\\py_data\\datacenter\\quote.h5"   
        self.datapath2  = "C:\\py_data\\datacenter"   
    
    def series_因子中性化(self,quote_factor,indicator,industry,mktcap=None):
        '''
        时间系列的因子中性化处理
        1. 因子中位数去极值；
        2. 因子标准
        3. 以对数总市值+行业虚拟变量为X值，以因子值为Y值，进行中性化回归处理，去残差作为新的因子值
        quote_factor = buylist2.loc[:,:]
        industry = 'FirstIndustryName'
        mktcap = '流通市值',若不为空，则进行市值中心化
        zs:z_core,代表标准化
        indicator = '扣非净利润同比的环比'
        nt:nuetralize,代表进行行业、市值中性化处理
        '''
        quote_factor = quote_factor.sort_values(['TradingDay'])
        quote_factor.index = range(len(quote_factor))
        temp = pd.get_dummies(quote_factor[industry])
        if  mktcap is not None:
            columns = list(quote_factor[industry].drop_duplicates())+[mktcap]
            temp = pd.merge(quote_factor[['SecuCode','TradingDay',indicator,mktcap]],temp,left_index=True,right_index=True)
            temp[mktcap] = temp[mktcap].apply(lambda x:np.log(x))
        else:
            columns = list(quote_factor[industry].drop_duplicates())
            temp = pd.merge(quote_factor[['SecuCode','TradingDay',indicator]],temp,left_index=True,right_index=True)
        temp = temp.dropna(subset=[columns],how='any',axis=0)
        temp = temp.dropna(subset=[indicator],how='any',axis=0)
        temp.index = range(len(temp))
        group = temp.groupby(['TradingDay']) #分日期
        #因子去极值并进行标准化
        z = group.apply(self.section_z_score标准化,indicator)
        z = z.apply(lambda x:pd.Series(x))
        z = z.stack()
        temp['%s_zs'%indicator] = z.values
        #中性化处理
        temp['%s_nt'%indicator] = group.apply(self.section_regress,'%s_zs'%indicator,columns).values
        quote_factor = pd.merge(quote_factor,temp[['SecuCode','TradingDay','%s_zs'%indicator,'%s_nt'%indicator]],on=['SecuCode','TradingDay'],how='left')
        return quote_factor
    
    def section_regress(self,data,y,x):
        '''
        截面回归
        '''
        y = data[y]
        x = data[x]
        result = sm.OLS(y,x).fit()
        return result.resid 
    
    def section_秩标准化(self,data,indicator,inudstryname=None):
        '''
        section:代表截面，标准化，
        inudstryname：非空时，进行行业调整【不是标准化】再标准化
        data = temp_value
        indicator = 'pb'
        对于某些因子，如股息率、PB等，行业之间差距很大，之间进行对比显然是不公平的，
        那么比较好的方式是对该值进行行业调整，行业调整的思路如下：亿PB为例=个股PB/行业PB中位数 
        '''
        data['value'] = data[indicator].rank(pct=True)
        data['value'] = (data['value']- data['value'].mean())/ data['value'].std()
        #行业调整，先进行标准化再打分
        if inudstryname is not None:
            median = data.groupby([industryname])[[indicator]].median()
            median[industryname] = median.index
            median['median'] = median[indicator]
            data = pd.merge(data,median[[industryname,'median']],on=industryname,how='left')
            data['value'] = data[indicator]/abs(data['median']) #行业调整
            data['value'] = data['value'].rank(pct=True) #全市场标准化
            data['value'] = (data['value']- data['value'].mean())/ data['value'].std()
        return data['value'].values
        
    def section_z_score标准化(self,data,indicator,industryname=None):
        '''
        标准化处理，采取中位数去极值法
        indicator:需要有industyrname名称，返回处理后的data值
        data = temp_value
        indicator = '单季销售毛利率同比'
        industryname = 'FirstIndustryName'
        inudstryname：非空时，进行行业标准处理，注意，不是行业调整，而是行业标准处理
        '''
        #中位数取极值法
        median = data[indicator].median()
        new_median = abs(data[indicator] - median).median()
        #data['value'] = np.clip(data['value'],median-5*newmedian,median+median+5*new_median)
        data['value'] = np.where(data[indicator]>median+5*new_median,median+5*new_median,
                            np.where(data[indicator]<median-5*new_median,median-5*new_median,data[indicator]))
        data['value2'] = (data['value'] -data['value'].mean()) / data['value'].std()
        #行业内Z_Score处理
        if industryname is not None:
            #中位数去极值法，进行极值处理
            industry_median = data.groupby([industryname])[[indicator]].median()
            industry_median[industryname] = industry_median.index
            data = pd.merge(data,industry_median,on=industryname,how='left')
            data['median'] =  abs(data['%s_x'%indicator] - data['%s_y'%indicator])
            median = data.groupby([industryname])[['median']].median()
            median[industryname] = median.index
            data = pd.merge(data,median,on=industryname,how='left')
    
            data['value_up'] = data['%s_y'%indicator] + 5*abs(data['median_y'] )
            data['value_down'] = data['%s_y'%indicator] - 5*abs(data['median_y'] )
            data['value'] = np.where(data['%s_x'%indicator]>data['value_up'],data['value_up'],
                                np.where(data['%s_x'%indicator]<data['value_down'],data['value_down'],data['%s_x'%indicator]))
            #进行标准化
            mean = data.groupby([industryname])[['value']].mean()
            mean[industryname] = mean.index
            data = pd.merge(data,mean,on=industryname,how='left')
            std = data.groupby([industryname])[['value_x']].std()
            std[industryname] = std.index
            data = pd.merge(data,std,on=industryname,how='left')
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
    
    def get_非常规数据(self,cyzb):
        '''
        产业资本增减持
        '''
        #获取产业资本净增减持数据
        cyzb = pd.read_hdf(self.datapath2+"\\risk.h5",'cyzb',columns=['SecuCode','TradingDay','净增持比例'])
        return cyzb
        
    
    def get_常规剔除(self,date,info,st,suspend,listedstate,days):
        '''
        info、st、suspend、listedstate分别为info数据、st数据、停复牌数据、上市状态变更数据
        date='20180111':当期的日期
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
        #上市日期大于days天，如60天
        temp_info =info[date>=info['ListedDate']+datetime.timedelta(days)] 
        #非ST、*ST、暂停上市、退市整理、高风险预警等股票
        temp_st = st[(date>=st['InfoPublDate'])]
        temp_st = temp_st.drop_duplicates(['InnerCode'],keep='last')
        temp_st = temp_st[~temp_st['SpecialTradeType'].isin((2,4,6))]
        #停牌股票
        temp_suspend = suspend[date>=suspend['InfoPublDate']]
        aa = temp_suspend[temp_suspend['InnerCode']==1432]
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
        return temp_info
    
    def get_非常规剔除(self,date,info,lift=None):
        '''
        info为常规剔除后的标的池
        lift = aa.loc[:,:]
        date = datetime.datetime(2018,1,12)
        '''
        temp_info = info
        if lift is not None:
            #解禁,解禁前2个月，后40天，比例占流通股本超过8%的个股进行剔除
            temp_lift = lift[(date>=lift['InitialInfoPublDate'])]
            temp_lift = temp_lift[(date>=(temp_lift['StartDateForFloating']-datetime.timedelta(60)))&(date<=temp_lift['StartDateForFloating']+datetime.timedelta(40))]
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
        data['同比'] = np.where(data['temp_y']!=0,(data[indicator]-data['temp_y'])/abs(data['temp_y']),np.nan)
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
        data['环比'] = np.where(data['temp_y']!=0,(data[indicator]-data['temp_y'])/abs(data['temp_y']),np.nan)
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
        data['单季值'] = np.where(data['month']==3,data[indicator],data[indicator]-data['temp_y'])
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
    
    def get_指数成分股(self,Index_SecuCode):
        '''
        指数成分股，Index_SecuCode为指数的代码
        '''
        Index_SecuCode = str(tuple(Index_SecuCode))
        constituent = pd.read_hdf(self.datapath2+'\\constituent.h5','data',where="Index_SecuCode in "+Index_SecuCode+"") 
        constituent = constituent.sort_values(['EndDate','Index_SecuCode'],ascending=True) #排序
        return constituent
    
    
    def get_财务股本表(self,sheetname,startdate,columns=[]):
        '''
        获取本地finance.h5中的财务数据
        1. 资产负债表_新会计准则 LC_BalanceSheetAll
        2. 利润分配表_新会计准则  LC_IncomeStatementAll
        3. 现金流量表_新会计准则 LC_CashFlowStatementAll
        4. 单季利润表_新会计准则 LC_QIncomeStatementNew
        5. 单季现金流量表_新会计准则 LC_QCashFlowStatementNew
        6. 公司股本变动表 LC_ShareStru
        7. 非经常性损益 LC_NonRecurringEvent 
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
        buylist0 = buylist9
        factor = '1个月动量'
        '''
        #factor = '总市值'
        corr = pd.DataFrame()
        time0 = buylist0[['TradingDay']].drop_duplicates()
        for i in range(len(time0)):
            date = time0.iloc[i]['TradingDay']
            data = buylist0[buylist0['TradingDay']==date]
            data = data.dropna(subset=[factor],how='any',axis=0)
            rank_ic = data[[factor,'next_rtn']].rank().corr().ix[0][1]
            init_ic = data[[factor,'next_rtn']].corr().ix[0][1]
            corr = corr.append(pd.DataFrame([[date,init_ic,rank_ic]]))
        corr1 = pd.DataFrame([[date,corr.mean()[1],corr.mean()[2]]]).append(corr)   
        corr2 = pd.DataFrame([[date,corr.mean()[1]/corr.std()[1],corr.mean()[2]/corr.std()[2]]]).append(corr1)   
        return corr2
    
    def insert_signal(self,buylist3):
        '''
        临时程序，加入宏观择时信号，还需要完善改进
        buylist4,每期选股，需要有weight字段,默认从20130101开始
        '''
        signal = pd.read_excel("C:\\Users\\dylan\\Desktop\\嘉实工作2\\指数部课题\宏观择时日信号.xlsx")
        signal.index = signal['m_date']
        buylist4 = pd.merge(buylist3,signal[['signal']],left_index=True,right_index=True,how='left')
        buylist4['signal'] = buylist4['signal'].fillna(1)
        buylist4['weight'] = 1/buylist4.groupby(['TradingDay'])['SecuCode'].count()
        buylist4['weight'] = np.where(buylist4['signal']==0,buylist4['weight']*0.3,buylist4['weight'])
        buylist4 = buylist4[buylist4.index>='20121228']
        return buylist4
        
        
        
    