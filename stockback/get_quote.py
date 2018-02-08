# -*- coding: utf-8 -*-
"""
Created on Wed Jan 11 16:52:33 2017

@author: chenghg
说明
quote.hdf中有以下价格数据
1. equity_quote:股票数据 来自股票行情表
2. index_quote:指数行情数据 来自指数行情表
3. equity_bonus:分红数据

info.hdf 有以下数据
1. listedstate:上市状态变更数据，来自聚源的LC_ListStatus表
2. info:A股最新inercode\secucode\secuabbr\companycode等字段，来自聚源Secumain表
3. suspend:停牌复盘表，来自LC_SuspendResumption
4. st:st情况表，来自LC_SpecialTrade
5. industry：行业表，来自LC_ExgIndustry，申万、中信、银华自定义
6. LC_ShareStru:股本变动表，来自LC_ShareStru

finance.hdf中有以下数据
1. 资产负债表_新会计准则 LC_BalanceSheetAll
2. 利润分配表_新会计准则  LC_IncomeStatementAll
3. 现金流量表_新会计准则 LC_CashFlowStatementAll
4. 单季利润表_新会计准则 LC_QIncomeStatementNew
5. 单季现金流量表_新会计准则 LC_QCashFlowStatementNew
"""

import numpy as np
import pandas as pd
import pymysql
import datetime

class get_quote:
    '''
    聚源数据库
    提取\更新全市场行情数据，包括复权因子、申万行业、成交量、总股本、流通股本等
    '''
    
    def __init__(self):
        self._dbengine1 =  pymysql.connect(host='192.168.1.139',
                           port=3306,
                           user='jydb',
                           password='jydb',
                           database='jydb',
                           charset='gbk')
        self.datapath = "C:\\py_data\\datacenter\\quote.h5"
        self.datapath2 = "C:\\py_data\\datacenter"
    
    def transto_None(self,data):
        '''
        处理data数据中包含nan、inf数据，转为None,以便mysql数据库能够认识
        '''
        columns = list(data.columns)
        for i in range(len(columns)):
            names = columns[i]
            data[names] = np.where(pd.isnull(data[names])!=True, data[names], None)
            data[names] = np.where(data[names]==np.inf, None,data[names])
        return data

    #-------------------------------获取聚源数据---------------------------------------------     
    def get_equityquote(self,startdate,enddate):
        #获取行情数据,复权价格【比例法】,并获得股票简称历次变更名称【用的是公告日数据，即公告将ST，既是还没正式改名，这边也先记录ST】
        #而股本的变动也是一样，取日期大于等于公告日和变更日的最新一条数据
        sql = "select B.SecuCode,(select SecurityAbbr from  LC_SecuChange as o2 where o2.InfoPublDate<=A.TradingDay \
                and o2.InnerCode=A.innercode ORDER BY o2.InfoPublDate desc limit 1) as SecuAbbr ,B.InnerCode,B.CompanyCode,\
                A.TradingDay,A.OpenPrice as op,A.ClosePrice as cp,A.HighPrice as hp,A.LowPrice as lp,\
                A.PrevClosePrice as precp,(A.ClosePrice * ifnull((select RatioAdjustingFactor from QT_AdjustingFactor as O1 where  \
                O1.ExDiviDate <= A.TradingDay and O1.InnerCode = A.InnerCode order by O1.ExDiviDate desc limit 1),1))  as fq_cp,\
                A.TurnoverVolume as vol,A.TurnoverValue,A.TurnoverDeals from QT_DailyQuote  A \
                inner join SecuMain B   on A.InnerCode = B.InnerCode   and  B.SecuCategory=1 and\
                B.SecuMarket in (83,90)  where TradingDay >STR_TO_DATE("+startdate+",'%Y%m%d')   and\
                 TradingDay <=STR_TO_DATE("+enddate+",'%Y%m%d') order by TradingDay desc "
#        sql = "select B.SecuCode,(select SecurityAbbr from  LC_SecuChange as o2 where o2.InfoPublDate<=A.TradingDay \
#                and o2.InnerCode=A.innercode ORDER BY o2.InfoPublDate desc limit 1) as SecuAbbr ,B.InnerCode,B.CompanyCode,\
#                A.TradingDay,A.OpenPrice as op,A.ClosePrice as cp,A.HighPrice as hp,A.LowPrice as lp,\
#                A.PrevClosePrice as precp,(A.ClosePrice * ifnull((select RatioAdjustingFactor from QT_AdjustingFactor as O1 where  \
#                O1.ExDiviDate <= A.TradingDay and O1.InnerCode = A.InnerCode order by O1.ExDiviDate desc limit 1),1))  as fq_cp,\
#                A.TurnoverVolume as vol,A.TurnoverValue,A.TurnoverDeals,(select Ashares from lc_sharestru as o3 where \
#                o3.InfoPublDate<=A.TradingDay and o3.EndDate<=A.TradingDay and o3.CompanyCode=B.CompanyCode order by enddate \
#                desc limit 1) as Ashares,(select AFloats from lc_sharestru as o3 where \
#                o3.InfoPublDate<=A.TradingDay and o3.EndDate<=A.TradingDay and o3.CompanyCode=B.CompanyCode order by enddate \
#                desc limit 1) as AFloats from QT_DailyQuote  A \
#                inner join SecuMain B   on A.InnerCode = B.InnerCode   and  B.SecuCategory=1 and\
#                B.SecuMarket in (83,90)  where TradingDay >STR_TO_DATE("+startdate+",'%Y%m%d')   and\
#                 TradingDay <=STR_TO_DATE("+enddate+",'%Y%m%d') order by TradingDay desc "
        quote = pd.read_sql(sql,con=self._dbengine1)
        return quote
    
    def get_indexquote(self,startdate,enddate):
        '''
        获取指数行情，包括综合指数10\规模类指数47\风格类指数43\和申万、中信行业指数，三大类
        '''
        sql = "select d.SecuCode,d.SecuAbbr,c.TradingDay,c.ClosePrice as cp,c.PrevClosePrice as precp from QT_IndexQuote c\
                INNER JOIN (select b.Secucode,b.SecuAbbr,b.innercode,a.pubdate as ListedDate from LC_IndexBasicInfo as a \
                inner join (select * from secumain where secucategory in (4) and secumarket in (83,90) and listedstate=1)as b\
                on a.IndexCode=b.innercode where (IndexType in (10,47) or (IndexType=30 and industrystandard in (3,9,24)))) as d\
                on c.innercode=d.innercode where TradingDay >STR_TO_DATE("+startdate+",'%Y%m%d')   and\
                 TradingDay <=STR_TO_DATE("+enddate+",'%Y%m%d') order by TradingDay desc "
        indexquote = pd.read_sql(sql,con=self._dbengine1)
        return indexquote
  
    
    def get_bonus(self):
        '''
        获取分红、送股数据
        '''
        sql =  "select a.SecuCode,a.InnerCode,a.CompanyCode,b.IfDividend,b.Enddate,b.AdvanceDate,\
                b.RightRegDate,b.ExDiviDate,b.BonusShareRatio,b.TranAddShareRaio,b.CashDiviRMB \
                from secumain a inner join LC_Dividend b on a.InnerCode=b.InnerCode \
                 where a.SecuMarket in (83,90) and a.SecuCategory =1"
        bonus = pd.read_sql(sql,con=self._dbengine1)
        bonus = bonus.fillna(0) #以免在stockback使用时，由于有NaN导致的误差
        #有些分红，年报和季报一起在同一天实施，如航发控制[000738.SZ]，2013-6-14日
        bonus = bonus.groupby(['SecuCode','ExDiviDate']).sum()
        bonus['SecuCode'] = pd.DataFrame(bonus.index)[0].apply(lambda x:x[0]).values
        bonus['ExDiviDate'] = pd.DataFrame(bonus.index)[0].apply(lambda x:x[1]).values
        bonus.index = range(len(bonus))
        bonus.to_hdf(self.datapath,key='bonus',format='table',mode='a',data_columns=bonus.columns)
       
    def new_data(self,dataname,func):
        '''
        从无到有提取数据
        dataname:数据名“quote”
        func: get_quote 或者 get_indexquoe
        '''
        quote = func('19900101','19991231') 
        quote01 = func('19991231','20091231')
        quote02 = func('20091231','20191231')
        quote = quote.append(quote01)
        quote = quote.append(quote02)         
        #quote = func('20091231','20191231')
        quote.to_hdf(self.datapath,key='%s'%dataname,format='table',mode='a',data_columns=quote.columns)
        print("%s数据提取完毕"%dataname)
        return quote
    
    def update_quote(self,dataname,func):
        '''
        更新行情数据
        '''
        enddate = '20990101'
        dt = pd.read_hdf("C:\\py_data\\datacenter\\quote.h5",'%s'%dataname,columns=['TradingDay'],where='SecuCode="000001"')     
        startdate = dt['TradingDay'].max()
        startdate = datetime.datetime.strftime(startdate,"%Y%m%d")
        new_quote = func(startdate,enddate) #提取更新数据
        new_quote.to_hdf(self.datapath,'%s'%dataname,format='table',mode='r+',data_columns=new_quote.columns,append=True)
        print("%s更新完毕"%dataname)
        
    def get_ST(self):
        '''
        股票ST、退市整理、高风险预警等情况
        '''
        sql = "select b.SecuCode,a.SecurityAbbr as SecuAbbr,b.CompanyCode,a.* from LC_SpecialTrade as a\
                INNER JOIN  (select * from secumain where SecuMarket in (83,90) and\
                SecuCategory =1) as b  on a.innercode=b.innercode order by InnerCode,SpecialTradeTime,InfoPublDate"
        st = pd.read_sql(sql,con=self._dbengine1)
        return st
       
        
    def get_suspend(self):
        '''
        停牌、复牌表
        '''
        sql = "select b.SecuCode,b.CompanyCode,a.* from LC_SuspendResumption as a\
                INNER JOIN  (select * from secumain where SecuMarket in (83,90) and\
                SecuCategory =1) as b  on a.innercode=b.innercode order by InnerCode,ResumptionDate,InfoPublDate"
        suspend = pd.read_sql(sql,con=self._dbengine1)
        return suspend    
    
    def get_上市状态变更(self):
        #获取A股上市、暂停、退市、恢复上市等信息
        sql = "select * from  LC_ListStatus  where innercode in (SELECT innercode from SecuMain\
                where SecuMarket in (83,90)  and SecuCategory=1) order by innercode,changedate "
        listedstate = pd.read_sql(sql,con=self._dbengine1)
        return listedstate
    
    def get_info(self):
        #获取A股代码、简称等信息
        sql = "select InnerCode,CompanyCode,SecuCode,SecuAbbr,ListedDate,ListedState from SecuMain \
                where SecuMarket in (83,90)  and SecuCategory=1  "
        info = pd.read_sql(sql,con=self._dbengine1)
        return info
    
    def get_解禁表(self):
        sql = "select * from LC_SharesFloatingSchedule where CompanyCode in (SELECT CompanyCode from \
                    secumain where  SecuMarket in (83,90) and SecuCategory=1 )"
        lift = pd.read_sql(sql,con=self._dbengine1)
        return lift
    
    def get_股本表(self):
        sql = "select * from LC_ShareStru where CompanyCode in (SELECT CompanyCode from \
                    secumain where  SecuMarket in (83,90) and SecuCategory=1 )"
        data = pd.read_sql(sql,con=self._dbengine1)
        return data
    
    def info_to_hdf(self):
        
        listedstate = self.get_上市状态变更() #输出被退市和暂停上市的股票
        listedstate = listedstate.sort_values(['InnerCode','ChangeDate'])
        suspend = self.get_suspend()
        info = self.get_info() #获取代码、简称等数据
        st = self.get_ST()
        LC_ShareStru = self.get_股本表() 
        industry = self.get_industry() #行业表
        lift = self.get_解禁表()
        
        listedstate.to_hdf(self.datapath2+"\\info.h5",'listedstate',format='table',mode='a',data_columns=listedstate.columns)
        info.to_hdf(self.datapath2+"\\info.h5",'info',format='table',mode='a',data_columns=info.columns)
        suspend.to_hdf(self.datapath2+"\\info.h5",'suspend',format='table',mode='a',data_columns=suspend.columns)
        st.to_hdf(self.datapath2+"\\info.h5",'st',format='table',mode='a',data_columns=st.columns)
        LC_ShareStru.to_hdf(self.datapath2+"\\info.h5",'LC_ShareStru',format='table',mode='a',data_columns=LC_ShareStru.columns)
        lift.to_hdf(self.datapath2+"\\info.h5",'lift',format='table',mode='a',data_columns=lift.columns)
        industry.to_hdf(self.datapath2+"\\info.h5",'industry',format='table',mode='a',data_columns=industry.columns)
        print('info相关数据更新完毕')
    #------------------------获取财报数据--------------------------------------------------------
    def get_财务表(self,sheetname):
        '''
        获取原始财务报表数据，从无到有，
        sheetnam：
        1. 资产负债表_新会计准则 LC_BalanceSheetAll
        2. 利润分配表_新会计准则  LC_IncomeStatementAll
        3. 现金流量表_新会计准则 LC_CashFlowStatementAll
        4. 单季利润表_新会计准则 LC_QIncomeStatementNew
        5. 单季现金流量表_新会计准则 LC_QCashFlowStatementNew
        6. 公司股本结构变动 LC_ShareStru
        7. 公司衍生报表数据_新会计准则（新） LC_FSDerivedData 备注：该表中涉及TTM的数据计算有问题，不用
        sheetname='LC_BalanceSheetAll'
        '''  
        if sheetname=='LC_QIncomeStatementNew' or  sheetname=='LC_QCashFlowStatementNew':
            sql = "select * from "+sheetname+" where AccountingStandards = 1 and Mark in (1,2)"
        elif sheetname=='LC_FSDerivedData':
            sql = "select * from "+sheetname+" where AccountingStandards = 1 and IfAdjusted in (1,2) "
        else:
            sql = "select * from "+sheetname+" where AccountingStandards = 1 and IfMerged=1 and\
                    IfAdjusted in (1,2) "
        
        data = pd.read_sql(sql,con=self._dbengine1)
        data.to_hdf(self.datapath2+'\\%s.h5'%sheetname,key='data',format='table',mode='w',data_columns=data.columns)
        print("%s提取完毕"%sheetname)
#        data.to_hdf("C:\\py_data\\datacenter\\test.h5",key='test',format='table',mode='a',data_columns=data.columns)
#        aa  = pd.read_hdf("C:\\py_data\\datacenter\\test.h5",'test',columns=['AccountingStandards'])
    
    def astype(self,data,bench_data):
        '''
        把data中的列数据格式转换成bench_data中的列数据格式
        '''
        for i in data.columns:
            data[i] = data[i].astype(bench_data[i].dtype)
        return data
     
        
    
    def update_财务股本表(self,sheetname):
        '''
        更新财务数据,不包括股本表
        sheetname='LC_FSDerivedData'
        '''
        olddata = pd.read_hdf(self.datapath2+'\\%s.h5'%sheetname,key='data',where='InfoPublDate>="20180201"')
        #startdate = datetime.datetime.strftime(olddata['InfoPublDate'].max() ,"%Y%m%d")
        id2  = str(olddata['ID'].max())
        
        if sheetname=='LC_QIncomeStatementNew' or  sheetname=='LC_QCashFlowStatementNew':
            sql = "select * from "+sheetname+" where AccountingStandards = 1 and Mark in (1,2) \
                    and ID>"+id2+""
        elif sheetname=='LC_FSDerivedData':
            sql = "select * from "+sheetname+" where AccountingStandards = 1 and IfAdjusted in (1,2)\
                    and  ID>"+id2+""
        else:
            sql = "select * from "+sheetname+" where AccountingStandards = 1 and IfMerged=1\
                    and IfAdjusted in (1,2) and ID>"+id2+""
        newdata = pd.read_sql(sql,con=self._dbengine1)
        if len(newdata) > 0:
            newdata =  self.astype(newdata,olddata)
            newdata.to_hdf(self.datapath2+'\\%s.h5'%sheetname,key='data',format='table',mode='r+',data_columns=newdata.columns,append=True)         
            #data = olddata.append(newdata)
            #data.to_hdf(self.datapath2+'\\%s.h5'%sheetname,key='data',format='table',mode='w',data_columns=data.columns)         
        print("%s更新完毕"%sheetname)
        
    def get_industry(self):
        #standard:'(3)':中信、'(9，24)'申万
        #3:中信 9：申万老版 24：申万2014版 19：银华自定义分类
        sql = "select CompanyCode,InfoPublDate,Industry,FirstIndustryName,SecondIndustryName,\
                ThirdIndustryName,FourthIndustryName,Standard,CancelDate from  LC_ExgIndustry where \
                Standard in (3,9,24,19)"
        industry = pd.read_sql(sql,con=self._dbengine1)
        return industry
    
    def get_指数成分股(self):
        '''
        获取指数成分股,获取get_indexquote中的指数成分股信息
        '''
        sql1 = "select b.SecuCode as Index_SecuCode,b.SecuAbbr,a.IndexCode,b.CompanyCode,a.pubdate as ListedDate from LC_IndexBasicInfo as a\
                inner join (select * from secumain where secucategory in (4) and secumarket in (83,90) and listedstate=1)as b\
                on a.IndexCode=b.InnerCode where (IndexType in (10,47) or (IndexType=30 and industrystandard in (3,9,24)))"
        index = pd.read_sql(sql1,con=self._dbengine1)
        indexcode = str(tuple(index['IndexCode']))
        sql2 = "select IndexCode,InnerCode,EndDate,Weight,UpdateTime from LC_IndexComponentsWeight where\
                 IndexCode in "+indexcode+" "
        constituent = pd.read_sql(sql2,con=self._dbengine1)
        constituent = pd.merge(constituent,index[['IndexCode','Index_SecuCode']],on='IndexCode',how='left')
        constituent.to_hdf(self.datapath2+'\\constituent.h5',key='data',format='table',mode='w',data_columns=constituent.columns)
        print("指数成分股提取完毕....")
    
    def update_指数成分股(self):
        data = pd.read_hdf(self.datapath2+'\\constituent.h5',key='data',where='EndDate>="20180101"')
        startdate = datetime.datetime.strftime(data['EndDate'].max() ,"%Y%m%d")
        indexcode = str(tuple(data['IndexCode'].drop_duplicates()))
        sql = "select IndexCode,InnerCode,EndDate,Weight,UpdateTime from LC_IndexComponentsWeight where\
                EndDate>"+startdate+" and IndexCode in "+indexcode+""
        newdata = pd.read_sql(sql,con=self._dbengine1)
        if len(newdata) >0:
            newdata = pd.merge(newdata,data[['IndexCode','Index_SecuCode']],on='IndexCode',how='left')
            newdata = self.astype(newdata,data)
            #newdata = data.append(newdata)
            newdata.to_hdf(self.datapath2+'\\constituent.h5',key='data',format='table',mode='r+',data_columns=newdata.columns,append=True)
        print("指数成分股更新完毕....") 
    
    def 产业资本增减持(self):
        '''
        获取历史产业资本增减持数据
        '''
        sql = "select b.SecuCode,b.SecuAbbr,a.CompanyCode,a.AlternationDate as TradingDay,\
                a.ReportDate as InfoPublDate,a.LeaderName as 姓名, a.PositionDesc as 职务,a.ConnectionDesc\
                as 关系, a.StockSumChanging as 变动股数,a.AvgPrice as 变动价格,\
                (select Ashares from LC_ShareStru as p where \
                p.CompanyCode=a.CompanyCode and a.AlternationDate>=p.InfoPublDate and a.AlternationDate>=p.EndDate\
                order by p.enddate desc limit 1) as A股股本,(select AFloats from LC_ShareStru as p where \
                p.CompanyCode=a.CompanyCode and a.AlternationDate>=p.InfoPublDate and a.AlternationDate>=p.EndDate\
                order by p.enddate desc limit 1) as 流通股本 from LC_LeaderStockAlter a\
                INNER JOIN (SELECT * FROM secumain where SecuCategory=1 AND\
                SecuMarket in (83,90) and ListedState=1) as b on \
                a.companycode=b.companycode and AlternationReason in(11,12,23)"
       
        sql = "select b.SecuCode,b.SecuAbbr,a.CompanyCode,a.InfoPublDate,a.TranDate as TradingDay,\
                a.TransfererName,a.InfoSource,a.InvolvedSum as 变动股数,a.DealPrice as 变动价格,a.DealTurnover ,\
                a.TranMode,a.ReceiverName as 姓名,(select Ashares from LC_ShareStru as p where \
                p.CompanyCode=a.CompanyCode and a.TranDate>=p.InfoPublDate and a.TranDate>=p.EndDate\
                order by p.enddate desc limit 1) as A股股本,(select AFloats from LC_ShareStru as p where \
                p.CompanyCode=a.CompanyCode and a.TranDate>=p.InfoPublDate and a.TranDate>=p.EndDate\
                order by p.enddate desc limit 1) as 流通股本,c.ClosePrice as cp from LC_ShareTransfer a\
                INNER JOIN (SELECT * FROM secumain where SecuCategory=1 AND\
                SecuMarket in (83,90) and ListedState=1) as b on  a.companycode=b.companycode \
                INNER JOIN QT_DailyQuote c on a.innercode=c.innercode and a.TranDate=c.TradingDay\
                where TranShareType=5 and  TranMode in (5,8,12,51,53,55,56)"
        data1 = pd.read_sql(sql,con=self._dbengine1)
        data2 = pd.read_sql(sql,con=self._dbengine1)
        #数据清洗
        temp_data = data2[data2['TranMode']==55]
        data11 = pd.merge(data1,temp_data[['SecuCode','TradingDay','姓名','TranMode']],how='left')
        data11 = data11[data11['TranMode']!=55] #删除高管增减持中的重复数据，因为股东增减持数据质量更高
        data11['DealTurnover'] =  data11['变动股数']* data11['变动价格']/100000000
        data11 = data11[['SecuCode','CompanyCode','InfoPublDate','TradingDay','变动股数','DealTurnover','流通股本']]
        
        #处理股东增减持价格为空的问题，用当日收盘价替代
        data2['变动价格'] = np.where(pd.isnull(data2['变动价格'])==True, data2['cp'],data2['变动价格'])
        data2['变动股数'] = np.where(pd.isnull(data2['TransfererName'])==True,data2['变动股数'],-data2['变动股数'])
        data2['DealTurnover'] = np.where(pd.isnull(data2['TransfererName'])==True,data2['DealTurnover']/100000000,-data2['DealTurnover']/100000000)
        data2['DealTurnover'] = np.where(pd.isnull(data2['DealTurnover'])==True,data2['变动股数']*data2['变动价格']/100000000,data2['DealTurnover'])
        #data2['变动占流通市值比'] =  100*data2['变动股数']/ data2['流通股本']
        data22 = data2[['SecuCode','CompanyCode','InfoPublDate','TradingDay','变动股数','DealTurnover','流通股本']]
        #董监高+股东增减持数据合并
        data = data11.append(data22)
        data['净增持比例'] = 100*data['变动股数']/data['流通股本']
        #获取交易日期
        #data3['TradingDay'] = data3['公告日期']
        date = data[['TradingDay']].drop_duplicates()
        date = date.sort_values(['TradingDay'])
        date.index = range(len(date))
        cyzb = pd.DataFrame()
        for i in  range(len(date)):
            time = date['TradingDay'][i]
            pretime = time-datetime.timedelta(30)

            temp_data = data[data['InfoPublDate']<=time]
            temp_data = temp_data[(temp_data['TradingDay']>=pretime)&(temp_data['TradingDay']<=time)]
            single = temp_data.groupby(['SecuCode'])[['净增持比例','DealTurnover']].sum()
            single['TradingDay'] = time
            cyzb = cyzb.append(single)
        cyzb['SecuCode'] = cyzb.index
        cyzb = cyzb.drop_duplicates(['SecuCode','净增持比例','DealTurnover'])
        cyzb.to_hdf(self.datapath2+'\\risk.h5',key='cyzb',format='table',mode='w',data_columns=cyzb.columns)
        print("产业资本增减持数据更新完毕....") 
        
        
    
#    def get_业绩预告(self,sheetname):
#        '''
#        获取原始财务报表数据，从无到有，
#        sheetnam：LC_PerformanceLetters
#        '''  
#        sql = "select * from "+sheetname+" where AccountingStandards = 1 and IfMerged=1 and\
#                    IfAdjusted in (1,2) "
#        
#        data = pd.read_sql(sql,con=self._dbengine1)
#        data.to_hdf(self.datapath2+'\\%s.h5'%sheetname,key='data',format='table',mode='w',data_columns=data.columns)
#        print("%s提取完毕"%sheetname)
       
        
    
    
if __name__ == '__main__':
     get =  get_quote()
    

    
    #当且仅当从头开始提取数据时运行，否则不运行,更新行情、指数及财务报表数据—------------------------
#     get.new_data('equity_quote',get.get_equityquote) #提取股票行情
#     get.new_data('index_quote',get.get_indexquote) #提取指数行情
#     get.get_指数成分股()
#     get.get_财务表('LC_BalanceSheetAll')
#     get.get_财务表('LC_IncomeStatementAll')
#     get.get_财务表('LC_CashFlowStatementAll')
#     get.get_财务表('LC_QIncomeStatementNew')
#     get.get_财务表('LC_QCashFlowStatementNew')
#     get.get_财务表('LC_FSDerivedData')

     #---数据更新-------------------------------------------------------------------------------    
     get.update_指数成分股()   
     get.update_quote('equity_quote',get.get_equityquote)#更新股票程序
     get.update_quote('index_quote',get.get_indexquote) #更新指数行情程序
     get.update_财务股本表('LC_BalanceSheetAll')
     get.update_财务股本表('LC_IncomeStatementAll')
     get.update_财务股本表('LC_CashFlowStatementAll')
     get.update_财务股本表('LC_QIncomeStatementNew')
     get.update_财务股本表('LC_QCashFlowStatementNew') 
     get.update_财务股本表('LC_FSDerivedData')    
     get.get_bonus()
     get.info_to_hdf() #上市状态、代码、简称、公司代码等数据
     get.产业资本增减持() #产业资本增减持数据
     
     
     