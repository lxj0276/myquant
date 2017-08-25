# -*- coding: utf-8 -*-
"""
Created on Tue Jun 14 14:47:03 2016
#营业收入TTM的表，包括三个字段 营业收入TTM原始值、TTM同比、TTM环比、SecuCode
index是日期格式
1. 计算每周末的每个截面数据的以上的值
2. 其中剔除退市、暂停、摘牌等A股的数据
@author: chenghg
"""
import datetime 
from factor0 import *
    
class industry0(init_factor):
    '''
    数据源:聚源数据库
    get_bonus:提取聚源数据库分红数据
    get_price:提取聚源数据库的行情数据  

    '''
 
    def get_industry(self,standard):
        #standard:
        #3:中信 9：申万老版 24：申万2014版 19：银华自定义分类
        sql = "select CompanyCode,InfoPublDate,Industry,FirstIndustryName,SecondIndustryName,\
                ThirdIndustryName,FourthIndustryName,Standard,CancelDate from  LC_ExgIndustry where \
                Standard in "+standard+" "
        sw_industry = pd.read_sql(sql,con=self._dbengine)
        return sw_industry
    
    def industry_data(self,startdate,standard,resample_type):
        '''
        处理行业数据变成每一周、每一个月等数据
        startdate：开始日期
        #standard = str((9,24))
        resample_type：'w':周 'm'：月 'a':年
        '''
        listedstate = self.get_上市状态变更() #输出被退市和暂停上市的股票
        listedstate = listedstate.sort_values(['InnerCode','ChangeDate'])
        up_date = listedstate[listedstate['ChangeType']==1] #获取上市日期
        info = self.get_info() #获取代码、简称等数据
      
        #从无到有处理因子---------------------------------------------
        
        industry = self.get_industry(standard)
        industry = pd.merge(industry,info[['CompanyCode','InnerCode','SecuCode']],on='CompanyCode',how='left')
        industry = industry.dropna(subset=['InnerCode'],axis=0) #删除空值
        industry = industry.sort_values(['CompanyCode','InfoPublDate','Standard'],ascending=True) #排序
        TradingDay = self.get_交易日期(startdate)
        #--转为周度、月度日期--------------------------------------------------------------------------
        week_day = TradingDay.resample(resample_type,on='TradingDay').last() #获取每周数据
        week_day = week_day.dropna()
        week_day.index = week_day['TradingDay']
           
        industry_week = pd.DataFrame()
        for i in week_day.index:
            print(i)
            #本期所属中信行业
            temp_industry = industry[(i>=industry['InfoPublDate'])]      
            temp_industry = temp_industry.drop_duplicates(subset=['CompanyCode'],keep='last')#保留最新行业分类
            #只记录当前处于正常上市状态和恢复上市的股票------------------------------
            now_state = listedstate[i>listedstate['ChangeDate']].drop_duplicates(['InnerCode'],keep='last')
            now_state = now_state[now_state['ChangeType'].isin((1,3))] 
            temp_industry = temp_industry[temp_industry['InnerCode'].isin(now_state['InnerCode'])]#上市状态正常 
            temp_industry['dt'] = datetime.datetime.strftime(i,'%Y%m%d')
            temp_industry = temp_industry[['dt','SecuCode','FirstIndustryName','SecondIndustryName','ThirdIndustryName','FourthIndustryName']]
            industry_week = industry_week.append(temp_industry)
        return industry_week


    

   
        
        
        
        
    