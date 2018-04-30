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
import statsmodels.api as sm
import matplotlib.pyplot  as plt
import time

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
        self.tradefee = 0.0015
        
    
    def get_因子数据(self,tablename,sheetname,indicators):
        '''
        获取因子库中的财务数据
        tablename = 'test'
        sheetname = 'pb'
        startdate = '20110101'
        enddate = '20110301'
        indicators = ['A股流通市值','归母净利润同比的环比']
        indicators = ['A股流通市值','归母净利润同比的环比']
        '''
        #factors = ['secucode','secuabbr']
        indicators = str(indicators).replace("[","")
        indicators = indicators.replace("]","")
        indicators = indicators.replace("'","")
        sql = "select TradingDay,SecuCode,"+indicators+" from %s.%s where TradingDay>=%s and\
               TradingDay<=%s "%(tablename,sheetname,self.startdate,self.enddate)
        if sheetname == 'sw_industry' or sheetname == 'zx_industry':
            sql = "select TradingDay,SecuCode,"+indicators+" from %s.%s"%(tablename,sheetname)
        data = pd.read_sql(sql,con=self._dbengine1)
        return data
    
    def get_日期表(self):
        '''
        获取交易日和自然日对应的日期表，自然日字段：TradingDate，交易日字段：TradingDay
        '''
        sql = "select * from QT_TradingDayNew where SecuMarket=83 order by TradingDate"
        date = pd.read_sql(sql,con=self._dbengine)  
        date_jy = date[date['IfTradingDay']==1]
        date_jy['dt'] = date_jy['TradingDate']
        date2 = pd.merge(date,date_jy[['TradingDate','dt']],on='TradingDate',how='left')
        date2['dt'] = date2['dt'].fillna(method='ffill')  
        date2 =  date2[['TradingDate','dt']]
        return date2
    
    def get_equityquoe(self):
        #获取行情数据,获取至最新行情
        #enddate2 = pd.to_datetime(self.enddate)+datetime.timedelta(32)
        #enddate2 =  datetime.datetime.strftime(enddate2,"%Y%m%d")
        enddate = str(int(self.enddate)+10000)
        sql = "select B.SecuCode,B.InnerCode,B.CompanyCode\
                ,A.TradingDay,A.OpenPrice as op,A.ClosePrice as cp,A.HighPrice as hp,A.LowPrice as lp,\
                A.PrevClosePrice as precp,(A.ClosePrice * ifnull((select RatioAdjustingFactor from QT_AdjustingFactor as O1 where  \
                O1.ExDiviDate <= A.TradingDay and O1.InnerCode = A.InnerCode order by O1.ExDiviDate desc limit 1),1))  as fq_cp,\
                A.TurnoverVolume as vol,A.TurnoverValue,A.TurnoverDeals ,(select AFloats from lc_sharestru as o3 where \
                o3.InfoPublDate<=A.TradingDay and o3.EndDate<=A.TradingDay and o3.CompanyCode=B.CompanyCode order by EndDate \
                desc limit 1) as AFloats from QT_DailyQuote  A \
                inner join  (select * from SecuMain where SecuCategory=1 and SecuMarket in (83,90)) as B on\
                A.InnerCode = B.InnerCode where TradingDay >=STR_TO_DATE("+self.startdate+",'%Y%m%d')\
               and TradingDay <=STR_TO_DATE("+enddate+",'%Y%m%d') order by SecuCode,TradingDay" 
        quote = pd.read_sql(sql,con=self._dbengine)  
        print("行情数据获取完毕...")
        return quote
    
    def get_indexquote(self,benchmark_code):
        '''
        获取指数行情
        benchmark_code:='399317',国证A股
        '''
        enddate = str(int(self.enddate)+10000)
        sql = "select d.SecuCode,d.SecuAbbr,c.TradingDay,c.ClosePrice as cp,c.PrevClosePrice as precp,\
                c.ChangePCT as rtn from QT_IndexQuote c\
                inner join (select * from secumain where SecuCode="+benchmark_code+"  and secucategory\
                 in (4) and secumarket in (83,90) and listedstate=1) as d\
                on c.innercode=d.innercode where TradingDay >STR_TO_DATE("+self.startdate+",'%Y%m%d') \
                and TradingDay <=STR_TO_DATE("+enddate+",'%Y%m%d') order by TradingDay  "
        indexquote = pd.read_sql(sql,con=self._dbengine)
        print('基准行情获取完毕...')
        return indexquote
    
    def get_数据合并(self,data1,data2,how):
        '''
        两列日期对不上数据的合并程序
        取data1中小于该日期的，最新的data2中的值
        比如data1：行情数据，有TradingDay、SecuCode、cp等数据;
        data2:行业数据，有行业一级等字段，该字段随TradingDay不定期改变,需要一张自然日和工作日对应表的处理
        目的是合并data1,data2,在data1中每一天，都能够取到data2最新行业分类
        factor.columns[2:]
        data1 = quote_ic.loc[:,:]
        data2 = factor.loc[:,:]
        how:数据合并方式，TRUE:代表用outer，针对不定期存储因子；
        aa = data2[:100]
        '''
#        mindate = data1['TradingDay'].min()
#        maxdate = data1['TradingDay'].max()
#        rq2 =rq[(rq['TradingDate']>=mindate)&(rq['TradingDate']<=maxdate)]
#        rq2 = rq2[rq2['if']]
        
        #使用outer，用以因子数据及DATA2数据是不定期的情况
        if how == 'outer':
            data = pd.merge(data1,data2,on=['SecuCode'],how='outer')
            data = data[data['TradingDay_x']>=data['TradingDay_y']]
            data = data.sort_values(['SecuCode','TradingDay_x','TradingDay_y'])
            data = data.drop_duplicates(['SecuCode','TradingDay_x'],keep='last')
            data['TradingDay'] =  data['TradingDay_x']
            data = data.drop(['TradingDay_y','TradingDay_x'],axis=1)
        else: #用于data2数据是定期的情况
            time = data2[['TradingDay']].drop_duplicates() 
            time = time.resample(self.cycle,on='TradingDay').last()
            time['dt'] = time.index
            data3 = data2[data2['TradingDay'].isin(time['TradingDay'])]
            data3 = pd.merge(data3,time,on='TradingDay',how='left')
            data3 = data3.drop(['TradingDay'],axis=1)
            data = pd.merge(data1,data3,on=['SecuCode','dt'],how='left')        
        return data
        
     
    def get_行情数据清洗(self,quote,industry,how):
        '''
        转为月度【指定频度】数据，并合并行情和行业分类数据
        1.先转为指定频度数据（月）数据，计算下一期收益率
        2.合并行业数据
        rq:自然日和交易日对应表
        data = quote.loc[:,:]
           '''
        #转为月度数据
        time = quote[['TradingDay']].drop_duplicates()
        time = time.resample(self.cycle,on='TradingDay').last()
        time['dt'] = time.index
#        quote2 = quote.groupby(['SecuCode']).resample('m',on='TradingDay').last()
#        quote2['dt'] = pd.DataFrame(quote2.index)[0].apply(lambda x:x[1]).values
        quote2 = quote[quote['TradingDay'].isin(time['TradingDay'])]
        quote2 = pd.merge(quote2,time,on='TradingDay',how='left')
        for i in range(1,7):
            quote2['next%s'%str(i)] = np.where(quote2['SecuCode']==quote2['SecuCode'].shift(-i),quote2['fq_cp'].shift(-i)/quote2['fq_cp'].shift(-i+1)-1,np.nan)
        data = self.get_数据合并(quote2,industry,how)
        data['mktcap'] = data['cp'] *data['AFloats']
        print("IC计算用数据清洗完毕...")
        return data   
    
    def section_regress(self,data,y,x):
        y = data[y]
        x = data[x]
        result = sm.OLS(y,x).fit()
        return result.resid 
    
    def section_z_score标准化(self,data,indicator):
        '''
        标准化处理，采取中位数去极值法
        indicator:需要有industyrname名称，返回处理后的data值
        data = aa
        indicator = 'pb'
        industryname = 'FirstIndustryName'
        inudstryname：非空时，进行行业标准处理，注意，不是行业调整，而是行业标准处理
        '''
        #中位数取极值法
        median = data[indicator].median()
        new_median = abs(data[indicator] - median).median()
        #data['value'] = np.clip(data['value'],median-5*newmedian,median+median+5*new_median)
        data['value'] = np.where(data[indicator]>median+5*new_median,median+5*new_median,
                            np.where(data[indicator]<median-5*new_median,median-5*new_median,data[indicator]))
        #标准化处理
        data['value2'] = (data['value'] -data['value'].mean()) / data['value'].std()
        return data['value2'].values
    
    
    def series_因子中性化(self,quote_factor,indicator,industry,mktcap=None):
        '''
        时间系列的因子中性化处理
        1. 因子中位数去极值；
        2. 因子标准
        3. 以对数总市值+行业虚拟变量为X值，以因子值为Y值，进行中性化回归处理，去残差作为新的因子值
        quote_factor = buylist.loc[:,:]
        industry = 'FirstIndustryName'
        mktcap = 'A股市值',若不为空，则进行市值中心化
        zs:z_core,代表标准化
        indicator = 'A股市值'
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
      
#        aa = temp[temp['TradingDay']=='20170228']
#        fig = plt.figure(figsize=(20,8))
#        ax = aa['pb2'].plot.kde()
   
    
    def get_rankic(self,quote,indicator,ifnuetral=False):
        '''
        indicator = 'pettm'
        获取每一期的相关系数，rankic值,ICdecay值
        industry:非空时，则计算该指标的行业调整后的IC等值
        industry_name ='行业一级'
        indicator = 'A股流通市值'
        '''
        
        #quote3 = quote[['SecuCode','TradingDay','next1',indicator]]
        columns_names = ['ICRank']
        quote3 =  quote[['SecuCode','TradingDay','next1','next2','next3','next4','next5','next6',indicator]]
        if ifnuetral==True:
            quote3 =  quote[['SecuCode','TradingDay','next1','next2','next3','next4','next5','next6',indicator,'%s_nt'%indicator]]
            columns_names = ['ICRank','ICRank_netural']
        #获取IC、ir标准差等值
        group = quote3.drop(['next2','next3','next4','next5','next6'],axis=1).groupby(['TradingDay'])
        corr = group.corr(method='spearman')#秩相关系数
        corr['type'] = pd.DataFrame(corr.index)[0].apply(lambda x:x[1]).values
        corr =  corr[corr['type']=='next1']
        corr =  corr.drop(['next1','type'],axis=1) 
        meanic = corr.mean()
        stdic = corr.std()
        minic = corr.min()
        maxic = corr.max()
        icir = meanic/stdic
        gl = 100*(corr[corr>0].count()/corr.count())
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
        corr.columns = columns_names
        corr['指标'] = indicator
        corr['TradingDay'] = np.array(pd.DataFrame(corr.index)[0].apply(lambda x:x[0]))
        corr['TradingDay'] = corr['TradingDay'].apply(lambda x:datetime.datetime.strftime(x,"%Y%m%d"))
        result['指标'] = result.index
        decay['指标'] = decay.index
        return corr,result,decay
    
    def get_分组行情清洗(self,quote):
        '''
        日线行情处理，月频为例，每一行提取后一个的月的数据到列数据中
        下个工作日是否涨停、停牌
        '''
        quote4 = quote[['TradingDay','SecuCode','fq_cp','precp','cp','vol']]
        quote4['ifhalt'] = np.where((quote4['SecuCode']==quote4['SecuCode'].shift(-1))&(quote4['vol']==0),1,0)
        quote4['ifzhangting'] = np.where((quote4['SecuCode']==quote4['SecuCode'].shift(-1))&(quote4['cp']<=0.9*quote4['precp'] + 0.01),1,0)
        quote4['date'] =  quote4['TradingDay'].apply(lambda x:int(datetime.datetime.strftime(x,"%Y%m%d")))
        #quote4['logrtn'] = quote4['cp']/quote4['precp']
        maxdate =  20990101 
        maxdays = 24 #默认月度数据，则每个月最大交易天数为23天
        if self.cycle=='w':
            days = 6 #每周最大天数为5天
        elif self.cycle == 'd':
            days = 2
        else:
            days=24
                 
        for i in range(1,days):#收益&日期
            quote4[i] = np.where(quote4['SecuCode']==quote4['SecuCode'].shift(-i),quote4['fq_cp'].shift(-i)/quote4['fq_cp'],np.nan)
            quote4['next%s'%str(i)] = np.where(quote4['SecuCode']==quote4['SecuCode'].shift(-i),quote4['date'].shift(-i),maxdate)
        print("多空收益计算用数据清洗完毕...")
        return quote4
        
    
    def performance_func(self,rtn,freq,hsl,benchmark_quote=None):
        '''
        本函数计算dataframe格式的表现情况,其中dataframe至少有两列数据
        rtn是log收益率
        freq=250:频率，日取250，月取12
        rtn = sy4
        hsl = hsl2
        '''
        unit = 1
        m_nav = np.exp(rtn.cumsum()) 
        lastnet = unit * (np.exp(rtn.sum()) - 1)
        #m_nav.plot(figsize=(30,10))  
        m_nav_max = m_nav.cummax()
        m_nav_min= (m_nav /  m_nav_max - 1)  
        std = unit * (np.exp(rtn)-1).std() * np.sqrt(freq)
        sy = unit * (pow(np.exp(rtn.sum()),freq/rtn.count()) - 1 )
        sharpratio = (sy - 0.03) / std
        sharpratio.iloc[len(sharpratio)-1] = sy[-1]/std[-1] #多空组计算信息比率
        maxdrawdown = 1 * m_nav_min.min()
        calmar = sy / abs(maxdrawdown)
        turnover = unit*12*hsl.sum()/len(hsl) #年化换手率
        m_rtn = unit *(np.exp(rtn.resample('m').sum()) - 1)
        m_ratio =  unit *m_rtn[m_rtn>0].count()/m_rtn.count()#月度胜利率
        m_winloss = (-1*m_rtn[m_rtn>0].mean() / m_rtn[m_rtn<0].mean())#月度盈亏比
        performance = pd.DataFrame(np.array([lastnet,sy,std,turnover,sharpratio,calmar,maxdrawdown,m_ratio,m_winloss]).T,index = sy.index,
                         columns=['累计收益','年化收益率','年化波动率','年化换手率','夏普比率','calmar比率','最大回撤','月度胜利率','月度盈亏比'])
        year_sy = unit * (np.exp(rtn.resample('a',how='sum')) - 1)
        by_year = rtn.groupby(lambda x:x.year)
        group_rtn = np.exp(by_year.cumsum())
        group_rtn = group_rtn / group_rtn.groupby(lambda x:x.year).cummax() - 1           
        year_maxdrawdown = group_rtn.groupby(lambda x:x.year).min()      
        cumsum_year_sy = m_nav.resample('a',how='last') -1 
        
        if  benchmark_quote is not None:
            benchmark_quote = benchmark_quote[benchmark_quote['TradingDay'].isin(rtn.index)]
            benchmark_quote.index = benchmark_quote['TradingDay']
            alpha = np.log(1+benchmark_quote['rtn']/100)
            alpha_sy = unit*(pow(np.exp(rtn.sum()-alpha.sum()),freq/rtn.count()) - 1 ) #超额年化收益
            alpha_sy.iloc[len(alpha_sy)-1] =np.nan #多空组设定为空
            alpha_rtn = np.log(1+(1 + m_nav.sub(np.exp(alpha.cumsum()),axis=0)).pct_change(periods=1))
            alpha_rtn['多空组'] = np.nan
            alpha2 = 1 + m_nav.sub(np.exp(alpha.cumsum()),axis=0)
            alpha2['多空组'] = np.nan #多空组设定为空
            alpha_sharpratio = alpha_sy / (unit*(np.exp(alpha_rtn)-1).std()*np.sqrt(freq)) #信息比率
            alpha_maxdarwdown = unit*(alpha2/alpha2.cummax() - 1).min() #超额最大回撤
            alpha_year_sy = unit * (np.exp(alpha_rtn.resample('a').sum()) - 1)
            alpha_m_rtn =  unit *(np.exp(alpha_rtn.resample('m').sum()) - 1)
            alpha_m_ratio =  unit *alpha_m_rtn[alpha_m_rtn>0].count()/alpha_m_rtn.count()#月度胜利率
            alpha_m_winloss =-1*alpha_m_rtn[alpha_m_rtn>0].mean() / alpha_m_rtn[alpha_m_rtn<0].mean()#月度盈亏比
            performance = pd.DataFrame(np.array([lastnet,sy,alpha_sy,std,turnover,sharpratio,alpha_sharpratio,calmar,maxdrawdown,
                                                 alpha_maxdarwdown,m_ratio,alpha_m_ratio,m_winloss,alpha_m_winloss]).T,index = sy.index,
                         columns=['累计收益','年化收益率','年化超额收益','年化波动率','年化换手率','夏普比率','信息比率','calmar比率','最大回撤',
                                    '超额最大回撤','月度胜利率','超额月度胜利率','月度盈亏比','超额月度盈亏比'])
            m_nav['index'] =   np.exp(alpha.cumsum()) 
            year_sy = pd.merge(year_sy,alpha_year_sy,left_index=True,right_index=True)
            year_sy.columns = ['第1组','第2组','第3组','第4组','第5组','多空组','超额-第1组','超额-第2组','超额-第3组','超额-第4组','超额-第5组','超额-多空组']
            year_sy = year_sy.drop(['超额-多空组'],axis=1)
            m_rtn= pd.merge(m_rtn,alpha_m_rtn,left_index=True,right_index=True)
            m_rtn.columns = ['第1组','第2组','第3组','第4组','第5组','多空组','超额-第1组','超额-第2组','超额-第3组','超额-第4组','超额-第5组','超额-多空组']
            m_rtn = m_rtn.drop(['超额-多空组'],axis=1)
        return m_nav,performance,year_sy,m_rtn
    
    def get_分组收益(self,quote_group,quote_factor,benchmark_quote,indicator,ifnuetral=False):
        '''
        分组收益，多空收益计算程序
         1. 控制涨停、停牌不买入;
         2.无法控制卖出情况，即停牌、跌停我们默认可以卖出；
         3.考虑每次换仓千分之1.5的手续费

        quote_group:日行情，包括后1个月的行情列数据；
        quote_factor=quote_factor2.loc[:,:]：月度数据，包括因子值数据
        indicator:因子如'pb'\'pettm'
        indicator='pb'
        industry_name='行业一级'
        benchmark_quote = index_quote
        '''
        maxdays = 24 #默认月度数据，则每个月最大交易天数为23天
        if self.cycle=='w':
            maxdays = 6 #每周最大天数为5天
        if ifnuetral==True:#是否是中性化后的因子
            indicator = '%s_nt'%indicator
        #通过列日期数据，是否大于下一个月日期，来判断本月天数，从而知道本月收益率计算应该取到哪一列
        keepname1 = ['TradingDay','SecuCode',indicator]
        keepname2 = ['TradingDay','SecuCode','ifhalt','ifzhangting']+[i for i in range(1,maxdays)]+['next%s'%str(i) for i in range(1,maxdays)]
  
        #aa  =quote5[keepname2][:20000] 
        quote5 = pd.merge(quote_factor[keepname1],quote_group[keepname2],on=['TradingDay','SecuCode'])
        quote5 =  quote5.sort_values(['SecuCode','TradingDay'])
        quote5['nextdate'] = quote5['TradingDay'].apply(lambda x:int(datetime.datetime.strftime(x,"%Y%m%d")))
        quote5['nextdate'] = np.where(quote5['SecuCode']==quote5['SecuCode'].shift(-1),quote5['nextdate'].shift(-1),19890101)
        for i in range(1,maxdays):#该列日期大于下个月值，则该列数据设为空
            quote5['next%s'%str(i)] = np.where((quote5['next%s'%str(i)]>quote5['nextdate'])|(quote5['ifhalt']==1)|(quote5['ifzhangting']==1),np.nan,1)
            quote5[i] = quote5[i]*quote5['next%s'%str(i)]#判断哪些列需要计算收益率
        keepname3 = ['TradingDay','SecuCode']+[indicator]+[i for i in range(1,maxdays)]
        keepname4 = [i for i in range(1,maxdays)]
        quote6 = quote5[keepname3] #仅保留有用字段
        quote6 = quote6.dropna(axis=0,how='all',subset=keepname4) #全为空的列进行删除,即剔除涨停、停牌不能买股票  
        quote6 = quote6.dropna(axis=0,how='all',subset=[indicator]) #因子数据为空则剔除不进行排序计算
        #默认分成5组
        tepm_fz = list(quote6.groupby(['TradingDay'])[indicator].apply(lambda x:pd.qcut(x,5,range(1,6))).values)
        quote6['分组'] = tepm_fz 

        #计算换手率，粗略计算=本期新买入权重+上期卖出权重+持有权重调整，单边默认手续费千分之1.5
        hsl =  pd.DataFrame(list(quote6.groupby(['分组','TradingDay'])['SecuCode']))
        hsl = hsl.drop([1],axis=1)
        code = list(quote6.groupby(['分组','TradingDay'])['SecuCode'])
        hsl['code']  = [tuple(x[1].values) for x in code] #取每次调仓日每个分组的股票
        hsl['TradingDay'] = hsl[0].apply(lambda x:x[1])
        hsl['分组'] = hsl[0].apply(lambda x:x[0])
        hsl = hsl.sort_values(['分组','TradingDay'])
        hsl['precode'] = np.where(hsl['分组']==hsl['分组'].shift(1),hsl['code'].shift(1),hsl['code'])
        hsl['temp'] = list(zip(hsl['code'],hsl['precode']))
        hsl['保持个数'] =  hsl['temp'].apply(lambda x:len(set(x[0])&set(x[1])))#取交集
        hsl['本期个数']  = hsl['code'].apply(lambda x:len(x))
        hsl['上期个数'] = hsl['precode'].apply(lambda x:len(x)) 
        hsl['换手率'] = (abs(hsl['保持个数']/hsl['上期个数']-hsl['保持个数']/hsl['本期个数'])+
                       abs(1-hsl['保持个数']/hsl['本期个数'])+abs(1-hsl['保持个数']/hsl['上期个数']))
        hsl2 = hsl.pivot('TradingDay','分组','换手率') 
        hsl2[6] = np.nan #为了多空收益准备的
        
        
        #计算分组收益
        sy = quote6.groupby(['分组','TradingDay'])[keepname4].mean()    
        sy2 = sy.unstack().T              
        sy2 = sy2.dropna(how='all',axis=0)
        sy2['天数'] = pd.DataFrame(sy2.index)[0].apply(lambda x:x[0]) .values
        sy2['日期'] = pd.DataFrame(sy2.index)[0].apply(lambda x:x[1]).values
        sy2 = sy2.sort_values(['日期','天数'])       
        sy2 = pd.merge(sy2,self.tradefee*hsl2,left_on=['日期'],right_index=True,how='left')   
        for i in range(1,6):
            sy2['第%s组'%str(i)] = np.where(sy2['天数']==1,sy2['%s_x'%i]-sy2['%s_y'%i],sy2['%s_x'%i]/sy2['%s_x'%i].shift(1))
        sy2['多空组'] = 1+sy2['第1组'] - sy2['第5组']

        #获取日期数据
        date = quote_group[['TradingDay']].drop_duplicates()
        date2 = date.resample(self.cycle,on='TradingDay').last()
        date2['日期'] = date2['TradingDay']
        date3 = pd.merge(date,date2,on=['TradingDay'],how='left')
        date3['日期'] = date3['日期'].shift(1).fillna(method='ffill')
        date4 = date3[(date3['TradingDay']==date3['日期'])==False]
        date4['天数'] = date4.groupby(['日期'])['TradingDay'].rank()
        sy3 = pd.merge(sy2,date4[['日期','TradingDay','天数']],on=['日期','天数'],how='left')
        sy3.index = sy3['TradingDay']
        sy4 = np.log(sy3[['第1组','第2组','第3组','第4组','第5组','多空组']]) #获得最终logrtn数组
        sy4 = sy4[sy4.index<=self.enddate]
        net,performance,year_sy,month_sy = self.performance_func(sy4,250,hsl2,benchmark_quote)
        #month_sy.columns = [str(x.year)+str(x.month) if x.month>=10 else str(x.year)+str(0)+str(x.month) for x in  list(month_sy.columns) ]
        net['indicator'] = indicator
        net['TradingDay'] = net.index
        net['TradingDay'] = net['TradingDay'].apply(lambda x:datetime.datetime.strftime(x,"%Y%m%d"))
        performance['indicator'] = indicator
        performance['指标'] = performance.index
        year_sy['indicator'] = indicator
        month_sy['indicator'] = indicator
        month_sy['TradingDay'] = pd.DataFrame(month_sy.index)['TradingDay'].apply(lambda x:datetime.datetime.strftime(x,"%Y%m%d")).values 
        return net,performance,year_sy,month_sy
    
    #----以下是第二层封装---------------------------------------------------------------------------------
    def get_基础数据(self,benchmark_code):
        '''
        对基础数据进行封装
        '''
        quote = self.get_equityquoe()
        benchmark_quote = self.get_indexquote(benchmark_code) #获取国政A指基准
        industry = self.get_因子数据('test','sw_industry',['行业一级','行业二级'])#获取申万行业数据   
#        rq = self.get_日期表()
#        industry = pd.merge(industry,rq,left_on=['TradingDay'],right_on=['TradingDate'],how='left')
#        industry['TradingDay'] = industry['dt']
#        industry = industry.drop(['TradingDate','dt'],axis=1)   
#        industry = industry.dropna(subset=['TradingDay'])
        quote_ic = self.get_行情数据清洗(quote,industry,how='outer') #行业分类数据插入到行情中,IC计算用
        quote_group = self.get_分组行情清洗(quote) #获取分组用的收益数据，分组多空收益计算用
        print("基础数据提取完毕")
        return benchmark_quote,quote_ic,quote_group
   
    def get_因子评价绩效(self,indicator,industry_name,quote_factor,quote_group,benchmark_quote,ifnuetral,ifmktcap=True):
        '''
        对基础数据进行封装
        '''
       
        #因子中心化处理,默认行业中心化和市值中性化 
        quote_factor2 = self.series_因子中性化(quote_factor,indicator,industry_name,'mktcap')
        if ifmktcap==False: #仅行业中性化，针对总市值、流通市值等因子
            quote_factor2 = self.series_因子中性化(quote_factor,indicator,industry_name)
        #计算因子IC、ICIR值
        ic_corr,ic,ic_decay = self.get_rankic(quote_factor2,indicator,True)
        #获得分组\多空收益数据
        net,performance,year_sy,month_sy = self.get_分组收益(quote_group,quote_factor2,benchmark_quote,indicator,ifnuetral)
        return ic_corr,ic,ic_decay,net,performance,year_sy,month_sy
    
    
    def get_指定因子绩效(self,tablename,sheetname,indicator,industry_name,benchmark_quote,quote_ic,quote_group,how,ifnuetral=False,ifmktcap=True):
        '''
        从无到有，指定某个数据库，某个数据库中的某个字段的因子评价绩效
        tablename='value'：数据库
        sheetname='pb:数据表
        indicator=['pb','A股流通市值']:具体的因子，如'pb'
        indicator = 'pb'
        industry_name:行业，如'行业一级'，行业中性用
        benchmark_quote:基准行情，计算超额收益等绩效指标
        rq:自然日和交易日对应的日期表
        quote_ic：计算因子ICrank绩效
        quote_group：计算因子分组收益绩效
        how:数据合并方式 分为'outer'和'left',outer针对不定期的因子数据，如财务数据，其他对应定期因子数据，如按月或者周存储的pb因子数据
        ifnuetral: True or False，是否是行业中性指标，默认是False,True:获取该因子行业中性后的绩效
        ifmktcap：该因子是否市值风格中性，默认True,如果因子是‘流通市值’则可以选择False
        '''
        
        #参数设置，提取因子数据
        ifmktcap = indicator.find("市值")<0 # 
        factor = self.get_因子数据(tablename,sheetname,[indicator]) #获取因子原始数据
        #转为设定频度数据，如月度数据
        quote_factor = self.get_数据合并(quote_ic,factor,how)
        #计算因子评价体系
        ic_corr,ic,ic_decay,net,performance,year_sy,month_sy = \
             self.get_因子评价绩效(indicator,industry_name,quote_factor,quote_group,benchmark_quote,ifnuetral,ifmktcap)
        return ic_corr,ic,ic_decay,net,performance,year_sy,month_sy
         
        
    
if __name__ == "__main__":
    gp = factor_evaluate('m','20141231','20171231') 
    benchmark_code= '399317' 
    import time
    t0 = time.time()
    #获取基准用数据
    benchmark_quote,quote_ic,quote_group = gp.get_基础数据(benchmark_code)
    #获取单个因子绩效  
    t1 = time.time()
    ic_corr,ic,ic_decay,net,performance,year_sy,month_sy = \
        gp.get_指定因子绩效('value','pb','pb','行业一级',benchmark_quote,quote_ic,quote_group,how='inner',ifnuetral=False,ifmktcap=True)
    t2 = time.time()
    #print("提取行情所用时间为%f \n 计算绩效所用时间为%f"%(t1-t0,t2-t1))