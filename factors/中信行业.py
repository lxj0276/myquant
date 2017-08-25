# -*- coding: utf-8 -*-
"""
Created on Wed Aug  9 14:41:45 2017
中信行业因子表创建 周度
@author: dylan
"""

from industry0 import *
    
class 中信行业(industry0):
    pass


if __name__ == "__main__":
    get = 中信行业() #初始化
    #从无到有处理因子,获取每周因子数据---------------------------------------------
    databasename = 'test'
    tablename = 'test0'
    startdate = '20030101'
    standard = '(3)' #(9,24)属于申万标准，(3)是中信标准
    tiemtype = 'w'  #timetype 转为周或者月数据
    industry_week = get.industry_data(startdate,standard,tiemtype)
    
    #-------创建表，并插入数据------------------------------------------------------------------
    #因子表结构
    data_structure = "(id int  not null primary key AUTO_INCREMENT,\
                        dt datetime,   \
                        SecuCode varchar(6),  \
                        行业一级 varchar(20), \
                        行业二级 varchar(20),\
                        行业三级 varchar(20),\
                        行业四级 varchar(20))"
    names = "(dt,SecuCode,行业一级,行业二级,行业三级,行业四级)"
    
    get.create_newdata(databasename,tablename,data_structure) #创建表
    get.insert_data(databasename,tablename,names,industry_week) #插入数据
   
   
        
        
        
    