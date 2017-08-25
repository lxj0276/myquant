# -*- coding: utf-8 -*-
"""
Created on Wed Aug  9 14:43:38 2017
中信行业 银子表更新 周度
@author: dylan
"""

from industry0 import *
    
class 中信行业更新(industry0):
    pass
    
if __name__ == "__main__":
    get = 中信行业更新() #初始化
     #获取数据库最后的更新日期--------------------------------
    databasename = 'test'
    tablename = 'test0'
    standard = '(3)'
    tiemtype = 'w'
    #--获取更新时间及数据------------------------------------
    last_data = get.get_最近的数据(databasename,tablename)
    startdate =  last_data['dt'].max()
    startdate = datetime.datetime.strftime(maxdt,'%Y%m%d')
    #获取更细数据---------------------------------------------
    
    new_industry_week = get.industry_data(startdate,standard,tiemtype)   
    #-------创建表，并插入数据------------------------------------------------------------------
    data_structure = "(idM int  not null primary key AUTO_INCREMENT,\
                        dt datetime,   \
                        SecuCode varchar(6),  \
                        行业一级 varchar(20), \
                        行业二级 varchar(20),\
                        行业三级 varchar(20),\
                        行业四级 varchar(20))"
    names = "(dt,SecuCode,行业一级,行业二级,行业三级,行业四级)"
    
    get.update_data(databasename,tablename,last_data) #更新数据
    get.insert_data(databasename,tablename,new_industry_week) #插入更新数据
   
   
        
        
        
        
    
