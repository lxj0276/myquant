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
from tqdm import tqdm
from factor0 import *
    
class industry0(factor0):
    '''
    数据源:聚源数据库
    get_bonus:提取聚源数据库分红数据
    get_price:提取聚源数据库的行情数据  

    '''
    def get_industry(self,startdate,standard):
        #standard:
        #3:中信 9：申万老版 24：申万2014版 19：银华自定义分类
        sql = "select CompanyCode,InfoPublDate,Industry,FirstIndustryName,SecondIndustryName,\
                ThirdIndustryName,FourthIndustryName,Standard,CancelDate from  LC_ExgIndustry where \
                Standard in "+standard+" and InfoPublDate>=STR_TO_DATE("+startdate+", '%Y%m%d')"
        sw_industry = pd.read_sql(sql,con=self._dbengine)
        return sw_industry
    

if __name__ == "__main__":
    get = 申万行业() #初始化
    

   
        
        
        
        
    