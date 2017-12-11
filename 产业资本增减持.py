# -*- coding: utf-8 -*-
"""
Created on Wed Jan 11 16:52:33 2017

@author: chenghg

产业资本 = 董监高+股东的净增减持数据
1. 董监高数据：导人及其相关人员持有股份变动情况 LC_LeaderStockAlter 交易所的数据，延迟一天公布
2. 股东增减持数据：股东股权变动 LC_ShareTransfer ，取TranMode in(5,8,12,51,53,55,56)的数据
    a) 其中55中的数据与LC_LeaderStockAlter中有部分重回，需要进行剔除，比如中国安保，
        大股东陈政立的增减持数据即记录LC_ShareTransfer中，也记录在LC_LeaderStockAlter
    b)  LC_ShareTransfer中，DealPrice 交易价格(元/股) 可能为空，若是为空，我们以当天的收盘价进行替代

"""


import sys
sys.path.append('C:\\Phython\\myquant\\stockback')
from public import *

class 产业资本(public):
    '''
    聚源数据库
    提取\更新全市场行情数据，包括复权因子、申万行业、成交量、总股本、流通股本等
    '''
    def get_高管增减持(self,startdate):
        sql = "select b.SecuCode,b.SecuAbbr,a.CompanyCode,a.AlternationDate as 变动日期,\
                a.ReportDate as 公告日期,a.LeaderName as 姓名, a.PositionDesc as 职务,a.ConnectionDesc\
                as 关系, a.StockSumChanging as 变动股数,a.AvgPrice as 变动价格,\
                (select Ashares from LC_ShareStru as p where \
                p.CompanyCode=a.CompanyCode and a.AlternationDate>=p.InfoPublDate and a.AlternationDate>=p.EndDate\
                order by p.enddate desc limit 1) as A股股本,(select AFloats from LC_ShareStru as p where \
                p.CompanyCode=a.CompanyCode and a.AlternationDate>=p.InfoPublDate and a.AlternationDate>=p.EndDate\
                order by p.enddate desc limit 1) as 流通股本 from LC_LeaderStockAlter a\
                INNER JOIN (SELECT * FROM secumain where SecuCategory=1 AND\
                SecuMarket in (83,90) and ListedState=1) as b on \
                a.companycode=b.companycode and AlternationReason in(11,12,23)\
                where reportdate >"+startdate+" order by reportdate desc"
        holder = pd.read_sql(sql,con=self._dbengine)
        return holder
    
    def get_股东增减持(self,startdate):
        sql = "select b.SecuCode,b.SecuAbbr,a.CompanyCode,a.InfoPublDate as 公告日期,a.TranDate as 变动日期,\
                a.TransfererName,a.InvolvedSum as 变动股数,a.DealPrice as 变动价格,a.DealTurnover ,\
                a.TranMode,a.ReceiverName as 姓名,(select Ashares from LC_ShareStru as p where \
                p.CompanyCode=a.CompanyCode and a.TranDate>=p.InfoPublDate and a.TranDate>=p.EndDate\
                order by p.enddate desc limit 1) as A股股本,(select AFloats from LC_ShareStru as p where \
                p.CompanyCode=a.CompanyCode and a.TranDate>=p.InfoPublDate and a.TranDate>=p.EndDate\
                order by p.enddate desc limit 1) as 流通股本,c.ClosePrice as cp from LC_ShareTransfer a\
                INNER JOIN (SELECT * FROM secumain where SecuCategory=1 AND\
                SecuMarket in (83,90) and ListedState=1) as b on  a.companycode=b.companycode \
                INNER JOIN QT_DailyQuote c on a.innercode=c.innercode and a.TranDate=c.TradingDay\
                where TranShareType=5 and  TranMode in (5,8,12,51,53,55,56)\
                 and InfoPublDate >="+startdate+""
        data = pd.read_sql(sql,con=self._dbengine)
        return data
    
    def get_交易日期(self,startdate):
        sql = "select TradingDay,ClosePrice as cp from QT_IndexQuote  where innercode=1 and \
                TradingDay>=STR_TO_DATE("+startdate+", '%Y%m%d') "
        TradingDay = pd.read_sql(sql,con=self._dbengine)
        return TradingDay
    
    def get_产业资数据清洗(self,data1,data2):
        '''
        产业资本 = 董监高+股东增减持数据
        剔除重复性，价格确实的填充等
    
        '''
        #处理 LC_ShareTransfer中TranMode=55高管增持和data1中数据的重复项
        temp_data = data2[data2['TranMode']==55]
        data11 = pd.merge(data1,temp_data[['SecuCode','变动日期','姓名','TranMode']],how='left')
        data11 = data11[data11['TranMode']!=55] #删除高管增减持中的重复数据，因为股东增减持数据质量更高
        data11['DealTurnover'] =  data11['变动股数']* data11['变动价格']/100000000
        #data11['变动占流通市值比'] =  100*data11['变动股数']/ data11['流通股本']
        data11 = data11[['SecuCode','CompanyCode','公告日期','变动日期','变动股数','DealTurnover','流通股本']]
        
        #处理股东增减持价格为空的问题，用当日收盘价替代
        aa = data2[pd.isnull(data2['DealTurnover'])==True]
        
        #data2['变动价格'] = np.where((pd.isnull(data2['变动价格'])==True)&(data2['DealTurnover']>0),data2['DealTurnover']/data2['变动股数'],data2['变动价格'])
        data2['变动价格'] = np.where(pd.isnull(data2['变动价格'])==True, data2['cp'],data2['变动价格'])
        data2['变动股数'] = np.where(pd.isnull(data2['TransfererName'])==True,data2['变动股数'],-data2['变动股数'])
        data2['DealTurnover'] = np.where(pd.isnull(data2['TransfererName'])==True,data2['DealTurnover']/100000000,-data2['DealTurnover']/100000000)
        data2['DealTurnover'] = np.where(pd.isnull(data2['DealTurnover'])==True,data2['变动股数']*data2['变动价格']/100000000,data2['DealTurnover'])
        #data2['变动占流通市值比'] =  100*data2['变动股数']/ data2['流通股本']
        data22 = data2[['SecuCode','CompanyCode','公告日期','变动日期','变动股数','DealTurnover','流通股本']]
        #董监高+股东增减持数据合并
        data3 = data11.append(data22)
        return data3
    
    def get_历史产业增减持数据(self,startdate,data3):
        '''
        获取产业资本增减持历史数据，包括板块、行业的增减持数据
        data:全市场和板块增减持数据
        data2:行业增减持金额数据
        data3:行业增减持比例数据
        '''
        #获取行业数据
        industry = gp.get_industry('(9,24)')
        industry = industry.drop_duplicates(['CompanyCode'],keep='last')
        #板块分类
        data3 = pd.merge(data3,industry[['CompanyCode','FirstIndustryName']],on=['CompanyCode'],how='left')
        data3['中小板'] = data3[['SecuCode']].applymap(lambda x: 1 if str(x[:3])=='002' else 0)
        data3['创业板'] = data3[['SecuCode']].applymap(lambda x: 1 if str(x[:3])=='300' else 0)
        data3['板块'] = np.where(data3['中小板']==1,'中小板',np.where(data3['创业板']==1,'创业板','主板'))    
         
        #获取交易日期
        date = gp.get_交易日期(startdate)
        date = date.sort_values(['TradingDay'])
        #获取每一天，最新已经公开的近一个月产业资本增减持数据，不用到未来数据
        sj = pd.DataFrame()
        sj2 = pd.DataFrame()
        sj3 = pd.DataFrame()
        
        for i in  tqdm(range(len(date))):
            time = date.loc[i,'TradingDay']
            pretime = time-datetime.timedelta(30)

            temp_data = data3[data3['公告日期']<=time]
            temp_data = temp_data[(temp_data['变动日期']>=pretime)&(temp_data['变动日期']<=time)]
            
            add_all= pd.DataFrame(temp_data[['DealTurnover']].sum()).T 
            #add_all['变动股数'] = 100*add_all['变动股数']/temp_data.drop_duplicates(['SecuCode'])['流通股本'].sum()
            add_all['上证指数'] = date.loc[i,'cp']
            add_yhcash = pd.DataFrame(temp_data.groupby(['FirstIndustryName'])[['DealTurnover']].sum()).T
            #add_yhratio = 100*pd.DataFrame(temp_data.groupby(['FirstIndustryName'])['变动股数'].sum()/ \
            #                           temp_data.drop_duplicates(['SecuCode']).groupby(['FirstIndustryName'])['流通股本'].sum()).T
            #add_bkratio = 100*pd.DataFrame(temp_data.groupby(['板块'])['变动股数'].sum()/ \
            #                           temp_data.drop_duplicates(['SecuCode']).groupby(['板块'])['流通股本'].sum()).T
            add_bkcash = temp_data.groupby(['板块'])[['DealTurnover']].sum().T
            
            add_all.index = [time]
            add_yhcash.index = [time]
           # add_yhratio.index = [time]
           # add_bkratio.index = [time]
            add_bkcash.index = [time]
            
            add = pd.concat([add_all,add_bkcash],axis=1)
            if len(add.columns)==5:
                add.columns = ['A股-增减持金额' ,'上证指数','中小板-增减持金额','主板-增减持金额',
                               '创业板-增减持金额']
            else:
                 add.columns = ['A股-增减持金额','上证指数','中小板-增减持金额','主板-增减持金额']
            sj = sj.append(add)
            sj2 = sj2.append(add_yhcash)
            #sj3 = sj3.append(add_yhratio)
        return sj,sj2
    
  
    
if __name__ == "__main__":
    #获取数据
    gp  = 产业资本()
    startdate = "20100101"
    data1 = gp.get_高管增减持(startdate)
    data2 = gp.get_股东增减持(startdate)
    data3 = gp.get_产业资数据清洗(data1,data2)
  
    cyzb,cyzb_industry = gp.get_历史产业增减持数据(startdate,data3)
    with pd.ExcelWriter("C:/Users/dylan/Desktop/慧网工作/其他研究/产业资本增减持数.xlsx") as writer:
        cyzb.to_excel(writer,"A股产业资本增减持")
        cyzb_industry.to_excel(writer,"行业产业资本增减持金额")
        #cyzb2.to_excel(writer,"行业产业资本增减持比例")

    
    
    
    
    
    