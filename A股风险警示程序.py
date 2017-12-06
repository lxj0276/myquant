# -*- coding: utf-8 -*-
"""
Created on Wed Jan 11 16:52:33 2017

@author: chenghg
"""

import numpy as np
import pandas as pd
import pymysql
import datetime
import sys
sys.path.append('C:\\Phython\\myquant')
from sendmail import *



class risk:
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
        #self._dbengine1 = create_engine("jydb://sa:jbt.123456@10.31.30.201",echo=True,encoding="gbk")
        #self._dbengine2 = create_engine("mysql://sa:jbt.123456@10.31.30.201",echo=True,encoding="gbk")
    

    #-------------------------------获取聚源数据---------------------------------------------
    def A股_重大违规处罚(self,startdate):
        #提取重大违规处罚
        sql = "select b.SecuCode,b.SecuAbbr,a.InfoPublDate,a.InfoSource ,c.ms as 处罚机构,\
            	d.ms as 处理方式,f.ms as 处理原因,g.ms as 关联关系 from LC_Deregulation as a\
            	INNER JOIN  (select * from secumain where SecuMarket in (83,90) and \
              SecuCategory =1 and ListedState=1) as b on a.companycode=b.companycode\
            	left join (select * from ct_systemconst where lb=1015) as c on a.AdminInst=c.dm\
            	left join (select * from ct_systemconst where lb=1164) as d on a.AdminType=d.dm\
            	left join (select * from ct_systemconst where lb=1165) as f on a.ReasonType=f.dm\
            	left join (select * from ct_systemconst where lb=1036) as g on a.subject=g.dm\
             where InfoPublDate >= STR_TO_DATE("+startdate+", '%Y%m%d') order by InfoPublDate desc"
        punish = pd.read_sql(sql,con=self._dbengine1)
        return punish
    
    def A股_特别处理(self,startdate):
        sql = "select b.SecuCode,a.SecurityAbbr as SecuAbbr,a.InfoPublDate,c.ms as 特别处理类型,\
                a.SpecialTradeTime as 处理时间,SpecialTradeReason as 原因 from LC_SpecialTrade as a\
                INNER JOIN  (select * from secumain where SecuMarket in (83,90) and\
                SecuCategory =1 and ListedState=1) as b  on a.innercode=b.innercode\
                left join (select * from ct_systemconst where lb=1185) as c on a.SpecialTradeType=c.dm\
                where SpecialTradeTime >= STR_TO_DATE("+startdate+", '%Y%m%d') order by InfoPublDate desc"
        st = pd.read_sql(sql,con=self._dbengine1)
        return st
    
    def A股_业绩预告(self,startdate):
        sql = "select b.SecuCode,b.SecuAbbr,a.InfoPublDate,a.enddate,a.mark,a.OperatingRevenueYOY as\
                '主营业务收入同比',a.NPParentCompanyOwnersYOY as '归属母公司利润同比' from LC_PerformanceLetters\
                as a inner join (select * from secumain where SecuMarket in (83,90) and \
                SecuCategory =1 and ListedState=1) as b on a.CompanyCode=b.CompanyCode \
                where mark in (1,2) and InfoPublDate >= STR_TO_DATE("+startdate+", '%Y%m%d') \
                order by InfoPublDate desc"
        f_profit = pd.read_sql(sql,con=self._dbengine1)
        return f_profit   
    
    def get_利润表(self,startdate):
        #获取利润表
        sql = "select b.SecuCode,b.SecuAbbr,c.ms as '企业性质',a.InfoPublDate,a.EndDate,\
                a.IfAdjusted,a.NPParentCompanyOwners  as '归属母公司的净利润',\
                a.OperatingRevenue as '营业收入'  from LC_IncomeStatementAll as a\
                inner join (select * from secumain where SecuMarket in (83,90) and \
                SecuCategory =1 and ListedState=1) as b on a.CompanyCode=b.CompanyCode\
                left join (SELECT * from ct_systemconst where lb=1414) as c \
                on a.EnterpriseType = c.dm where AccountingStandards = 1 and  IfMerged=1  \
                and InfoPublDate >= STR_TO_DATE("+startdate+", '%Y%m%d') "
        cum_profit = pd.read_sql(sql,con=self._dbengine1)
        return cum_profit
    
    def get_诉讼(self,startdate):
        sql = "select b.SecuCode,b.SecuAbbr,a.infopubldate,a.InitialInfoPublDate as '首次信息发布日'\
                ,a.InfoSource as 信息来源,c.ms as 公告类型,a.EventContent as 事件内容,d.ms\
                as 事件主体,e.ms as 事件进程,f.ms as 行为方式,g.ms as 与公司关联,\
                a.IfEnded as 是否终止, a.FirstSuitSum as 首次诉讼仲裁金额,LatestSuitSum as 最新诉讼仲裁金额,\
                a.Plaintiff as 原告,g2.ms as 原告与上市公司关联,a.Defendant as 被告,\
                g3.ms as 被告与上市公司关联,a.JSRParty as 责任连带人,g4.ms as 连带人与上市公司关联,\
                a.OtherParty as 其他方,g5.ms as 其它方与上市公司关联,a.SubjectMatterStat As 案由描述,h.ms \
                as 事件主体角色,a.InquisitionInstitute as 诉讼仲裁处理机构,i.ms as 仲裁状态,j.ms as 一审状态, \
                k.ms as 二审状态,l.ms as 最高院监督状态,m.ms as 财产执行情况,a.PropertyEnforced As 被执行财产,\
                n.ms as 判决执行状态 from   LC_SuitArbitration as a\
                inner join (select * from secumain where SecuCategory=1 and SecuMarket in (83,90) and ListedState=1)\
                as b on a.CompanyCode = b.CompanyCode \
                left join (SELECT * from ct_systemconst where lb=1109) as c on a.AnnouncementType = c.dm \
                left join (SELECT * from ct_systemconst where lb=1246) as d on a.EventSubject = d.dm \
                left join (SELECT * from ct_systemconst where lb=1059) as e on a.EventProcedure = e.dm \
                left join (SELECT * from ct_systemconst where lb=1063) as f on a.ActionWays = f.dm \
                left join (SELECT * from ct_systemconst where lb=1036) as g on a.SubjectAssociation = g.dm \
                left join (SELECT * from ct_systemconst where lb=1036) as g2 on a.PlaintiffAssociation = g2.dm \
                left join (SELECT * from ct_systemconst where lb=1036) as g3 on a.DefendantAssociation = g3.dm \
                left join (SELECT * from ct_systemconst where lb=1036) as g4 on a.JSRPartyAssociation = g4.dm\
                left join (SELECT * from ct_systemconst where lb=1036) as g5 on a.OtherPartyAssociation = g5.dm\
                left join (SELECT * from ct_systemconst where lb=1060) as h on a.EventSubjectRole = h.dm \
                left join (SELECT * from ct_systemconst where lb=1065) as i on a.CaseStatus = i.dm \
                left join (SELECT * from ct_systemconst where lb=1061) as j on a.FirstInstanceStatus = j.dm \
                left join (SELECT * from ct_systemconst where lb=1061) as k on a.SecondInstanceStatus = k.dm \
                left join (SELECT * from ct_systemconst where lb=1061) as l on a.SPPStatus = l.dm \
                left join (SELECT * from ct_systemconst where lb=1054) as m on a.PropertyEnforcement = m.dm \
                left join (SELECT * from ct_systemconst where lb=1058) as n on a.AdjudgementStatus = n.dm \
                where InfoPublDate >STR_TO_DATE("+startdate+",'%Y%m%d') order by InfoPublDate desc"

        lawing = pd.read_sql(sql,con=self._dbengine1)
        return lawing   
    
    def get_解禁(self,startdate,enddate):
        #获取利润表
        sql = "select b.SecuCode,b.SecuAbbr,a.InitialInfoPublDate AS\
                首次信息公布日,a.StartDateForFloating AS 解禁日,a.Proportion1 as 占流通股比例\
                ,a.Proportion2 as 占总股本比例,a.NewMarketableSharesSource as 解禁来源\
                 from LC_SharesFloatingSchedule as a\
                INNER JOIN (SELECT * from secumain where SecuCategory=1 and SecuMarket\
                in (83,90) and ListedState=1) as b on a.InnerCode=b.InnerCode\
                where StartDateForFloating>=STR_TO_DATE("+startdate+",'%Y%m%d') and\
                StartDateForFloating<=STR_TO_DATE("+enddate+",'%Y%m%d') order by StartDateForFloating desc"
        lift_ban = pd.read_sql(sql,con=self._dbengine1)
        return lift_ban
    
    def get_审计意见(self,startdate):
        #获取利润表
        sql = "select b.SecuCode,b.SecuAbbr,a.EndDate,a.AccountingFirms AS 会计师事务所,\
                a.OpinionType,c.ms as 审计意见类型,a.OpinionFullText as 审计全文, \
                a.xgrq as 更新时间 from LC_AuditOpinion  as a\
                INNER JOIN (SELECT * from secumain where SecuCategory=1 and SecuMarket\
                in (83,90) and ListedState=1) as b on a.CompanyCode=b.CompanyCode\
                left join (SELECT * from ct_systemconst where lb=1051) as c on a.OpinionType=c.dm\
                where OpinionType in(2,3,4,5,7,9,70) and a.xgrq>=STR_TO_DATE("+startdate+",'%Y%m%d')\
                order by enddate desc"
        audit = pd.read_sql(sql,con=self._dbengine1)
        return audit
    
    def get_高管增减持(self,startdate):
        sql = "select b.SecuCode,b.SecuAbbr,a. AlternationDate as 变动日期,\
                a.ReportDate as 公告日期,a.LeaderName as 姓名, a.PositionDesc as 职务,a.ConnectionDesc\
                as 关系, a.StockSumChanging as 变动股数,\
                (select Ashares from LC_ShareStru as p where \
                p.CompanyCode=a.CompanyCode and a.AlternationDate>=p.InfoPublDate and a.AlternationDate>=p.EndDate\
                order by p.enddate desc limit 1) as A股股本,(select AFloats from LC_ShareStru as p where \
                p.CompanyCode=a.CompanyCode and a.AlternationDate>=p.InfoPublDate and a.AlternationDate>=p.EndDate\
                order by p.enddate desc limit 1) as 流通股本，\
                ,a.ChangeProportion as 变动比例,e.ClosePrice/a.AvgPrice-1 as 盈亏比例\
                from LC_LeaderStockAlter a\
                INNER JOIN (SELECT * FROM secumain where SecuCategory=1 AND\
                SecuMarket in (83,90) and ListedState=1) as b on \
                a.companycode=b.companycode and AlternationReason in(11,12,23)\
                left join QT_DailyQuote e on a.innercode=e.innercode and e.TradingDay=\
                (select max(TradingDay) from qt_dailyquote where innercode=3)\
                where reportdate >"+startdate+" order by reportdate desc"
        holder = pd.read_sql(sql,con=self._dbengine1)
        return holder
    
if __name__ == "__main__":
    #获取数据
    year_begin = '20170101'
    ratio = -0.05
    risk= risk()
    
    date = datetime.datetime.today()
    last_month = date- datetime.timedelta(30)
    last_3month = date- datetime.timedelta(90)
    nex_month = date + datetime.timedelta(30)
    last_year = date- datetime.timedelta(400)
    
    date = datetime.datetime.strftime(date,"%Y%m%d")
    last_month = datetime.datetime.strftime(last_month,"%Y%m%d")
    nex_month = datetime.datetime.strftime(nex_month,"%Y%m%d")
    last_year = datetime.datetime.strftime(last_year,"%Y%m%d")
    
    file_name = "C:\\Users\\Dylan\\Desktop\\慧网工作\\A股风险事件每日提醒.xlsx" 
    
    #--重大违规---------------------------------------
    punish = risk.A股_重大违规处罚(last_month)
    punish['type'] = '重大违规处罚风险'
    #---ST-------------
    st =  risk.A股_特别处理(last_month)
    st['type'] = 'ST风险'
    #--业绩预告-------------------
    f_profit = risk.A股_业绩预告(last_3month)
    f_profit = f_profit[(f_profit['主营业务收入同比']<ratio*100)|(f_profit['主营业务收入同比']<ratio*100)]
    f_profit['type'] = '业绩预告大幅下滑风险'
    #--业绩报告--------------------------------  
    cum_profit = risk.get_利润表(last_year)
    cum_profit = cum_profit.sort_values(['SecuCode','InfoPublDate','EndDate'],ascending=True)
    cum_profit = cum_profit.drop_duplicates(subset=['SecuCode','EndDate'],keep='last')
    cum_profit['month'] = cum_profit['EndDate'].apply(lambda x:x.month)
    cum_profit['year'] = cum_profit['EndDate'].apply(lambda x:x.year)
    cum_profit['营业收入当期同比'] = np.where((cum_profit['SecuCode']==cum_profit['SecuCode'].shift(4))
                                            &(cum_profit['month']==cum_profit['month'].shift(4))&
                                            (cum_profit['year']==cum_profit['year'].shift(4)+1)&
                                            (cum_profit['营业收入'].shift(4)!=0),
                                            cum_profit['营业收入']/cum_profit['营业收入'].shift(4)-1,None)
    cum_profit['归属母公司净利润当期同比'] = np.where((cum_profit['SecuCode']==cum_profit['SecuCode'].shift(4))
                                            &(cum_profit['month']==cum_profit['month'].shift(4))&
                                            (cum_profit['year']==cum_profit['year'].shift(4)+1)&
                                            (cum_profit['归属母公司的净利润'].shift(4)!=0),
                                            cum_profit['归属母公司的净利润']/cum_profit['归属母公司的净利润'].shift(4)-1,None)
    cum_profit = cum_profit.drop_duplicates(['SecuCode'])
    cum_profit = cum_profit[(cum_profit['营业收入当期同比']<ratio)|(cum_profit['归属母公司净利润当期同比']<ratio)]
    cum_profit['type'] = '业绩大幅下滑风险'
    
    #--解禁-------------
    lift_ban =  risk.get_解禁(last_month,nex_month) #大于5%
    lift_ban = lift_ban[lift_ban['占流通股比例']>=5]
    lift_ban['type'] = '解禁风险'
    #--审计-------------
    audit = risk.get_审计意见(year_begin) 
    audit['type'] = '审计风险'
    #--高管增减持-------
    holder = risk.get_高管增减持(last_month) 
    hold_add = 100 *holder.groupby(['SecuCode'])['变动股数'].sum() /holder.groupby(['SecuCode'])['流通股本'].mean()
    hold_add = pd.DataFrame(hold_add.rename('占流通股本比例'))
    hold_add['占总股本比例'] =100* holder.groupby(['SecuCode'])['变动股数'].sum() /holder.groupby(['SecuCode'])['总股本'].mean()
    hold_add['增减持以来最大亏损'] = 100*holder.groupby(['SecuCode'])['盈亏比例'].min()
    hold_add['增减持以来最大盈利'] = 100*holder.groupby(['SecuCode'])['盈亏比例'].max()
    hold_add = hold_add.dropna(how='any',axis=0)
    hold_add = hold_add.sort_values(['占流通股本比例'])
    hold_add['SecuCode'] = hold_add.index
    hold_add = pd.merge(hold_add,holder,on='SecuCode',how='left')
    hold_add = hold_add[hold_add['占流通股本比例']<-0.5]
    hold_add['type'] = '高管减持风险' 
    
    #--诉讼-------------
    lawing = risk.get_诉讼(year_begin)
    lawing = lawing[(lawing['最新诉讼仲裁金额']>20000000)|(lawing['首次诉讼仲裁金额']>20000000)]
    lawing['type'] = '诉讼风险'
    #--汇总------------------------------
    pool = pd.DataFrame()  
    pool = pool.append(st[['SecuCode','SecuAbbr','type']])   
    pool = pool.append(hold_add[['SecuCode','SecuAbbr','type']])
    pool = pool.append(f_profit[['SecuCode','SecuAbbr','type']])
    pool = pool.append(cum_profit[['SecuCode','SecuAbbr','type']])
    pool = pool.append(punish[['SecuCode','SecuAbbr','type']])
    pool = pool.append(lift_ban[['SecuCode','SecuAbbr','type']])
    pool = pool.append(audit[['SecuCode','SecuAbbr','type']])
    pool = pool.append(lawing[['SecuCode','SecuAbbr','type']])
    
    
    with pd.ExcelWriter(file_name) as writer:    
        pool.to_excel(writer,"风险个股汇总")     
        st.to_excel(writer,"近期ST情况")       
        hold_add.to_excel(writer,"近期高管增减持情况")        
        f_profit.to_excel(writer,"业绩预告大幅下滑")
        cum_profit.to_excel(writer,"业绩大幅下滑")        
        punish.to_excel(writer,"A股重大违规处罚标的")
        lift_ban.to_excel(writer,"近期解禁股")
        audit.to_excel(writer,"审计风险")
        lawing.to_excel(writer,"涉及诉讼")
        
    #主动发送邮件---------------------------
    send = SendMail()
    content = "Dear all,\n    近期个股风险事件，请查收。\n    ST风险：记录近一个月,被ST股票,重要程度*****！\n    高管增减持风险:记录近一个月,公司董、监、高等高管净减持比例超过流通股本0.5%的个股，重要程度****;\n    业绩下滑风险：记录近一个月,业绩预告或者业绩报告主营收入或者利润下滑超过5%的股票，重要程度****;\n    重大违规处罚风险:记录近一个月,A股重大违规被证监会警告、处罚等风险事件，重要程度***;\n    解禁风险:记录过去一个月至未来一个月，A股解禁占流通股本超过5%的个股，重要程度***;\n    审计风险：记录年初以来，上市公司财务被审计公司出具不太标准意见事件，该公司可能存在重大隐患或者财务造假，重要程度***;\n    诉讼风险:记录年初以来，上市公司及其关联公司涉及诉讼金额超过2000万元的事件，重要程度***;\n    备注：这是一封自动发送邮件。"
    #receiver = ['dylan.cheng@invesmart.cn','kevin.wang@invesmart.cn','ithrael.gao@invesmart.cn']
    receiver = ['kevin.wang@invesmart.cn','will.xia@invesmart.cn',
                'ithrael.gao@invesmart.cn','simons.xie@invesmart.cn',
                'kaan.han@invesmart.cn','dylan.cheng@invesmart.cn',
                'winsen.huang@invesmart.cn','jason.jiang@invesmart.cn']
    #receiver = ['dylan.cheng@invesmart.cn']
    subject = '%s-个股风险事件提示'%date
    send.SendTo_add(file_name,receiver,subject,content)
    
    
    
    
    
    
    
    