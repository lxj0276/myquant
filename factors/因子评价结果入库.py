# -*- coding: utf-8 -*-
"""
Created on Tue Jan 16 16:31:27 2018

@author: dylan
"""

from 因子评价体系 import *

class factor_result_to_mysql(factor_evaluate):
    
    def create_result_sheet(self):
        '''
        zero_begin:True or False,代表是否初始化，初始化则数据清空，重新建表
        ifnuetral：入库的是否是中性化的因子的绩效
        '''
         #-------创建表，并插入数据------------------------------------------------------------------
        #IC_CORR
        data_structure0 = "( id int  not null primary key AUTO_INCREMENT,\
                            ICRank double,\
                            ICRank_neutral double,\
                            indicator varchar(100),\
                            TradingDay datetime)"
        #IC
        data_structure1 = "( id int  not null primary key AUTO_INCREMENT,\
                            IC均值 double,\
                            IC标准差 double,\
                            IC最小值 double,\
                            IC最大值 double,\
                            ICIR double,\
                            IC大于0概率 double,\
                            indicator varchar(100))"
        #IC_DECAY
        data_structure2 = "( id int  not null primary key AUTO_INCREMENT,\
                            next1 double,\
                            next2 double,\
                            next3 double,\
                            next4 double,\
                            next5 double,\
                            next6 double,\
                            indicator varchar(100))"
        #net
        data_structure3 = "( id int  not null primary key AUTO_INCREMENT,\
                            第1组 double,\
                            第2组 double,\
                            第3组 double,\
                            第4组 double,\
                            第5组 double,\
                            多空组 double,\
                            benchmark double,\
                            indicator varchar(100),\
                            TradingDay datetime)"
        #peformance
        data_structure4 = "( id int  not null primary key AUTO_INCREMENT,\
                            累计收益 double,\
                            年化收益率 double,\
                            年化超额收益 double,\
                            年化波动率 double,\
                            年化换手率 double,\
                            夏普比率 double,\
                            信息比率 double,\
                            calmar比率 double,\
                            最大回撤 double,\
                            超额最大回撤 double,\
                            月度胜利率 double,\
                            超额月度胜利率 double,\
                            月度盈亏比 double,\
                            超额月度盈亏比 double,\
                            indicator varchar(100),\
                            指标 varchar(100))"
        #year_sy
        data_structure5 = "( id int  not null primary key AUTO_INCREMENT,\
                            第1组 double,\
                            第2组 double,\
                            第3组 double,\
                            第4组 double,\
                            第5组 double,\
                            多空组 double,\
                            超额第1组 double,\
                            超额第2组 double,\
                            超额第3组 double,\
                            超额第4组 double,\
                            超额第5组 double,\
                            indicator varchar(100))"
        
        #month_sy
        data_structure6 = "( id int  not null primary key AUTO_INCREMENT,\
                            第1组 double,\
                            第2组 double,\
                            第3组 double,\
                            第4组 double,\
                            第5组 double,\
                            多空组 double,\
                            超额第1组 double,\
                            超额第2组 double,\
                            超额第3组 double,\
                            超额第4组 double,\
                            超额第5组 double,\
                            indicator varchar(100),\
                            TradingDay datetime)"
        sheetnames = ['ic_corr','ic_performance','ic_decay','net','group_performance','year_sy','month_sy']
        
        for i in range(7):
            self.create_newtable('result',sheetnames[i],eval('data_structure%s'%i)) #创建表    
    
    def insert_to_result(self,ic_corr,ic_performance,ic_decay,net,group_performance,year_sy,month_sy,ifnuetral):
        '''
        ifnuetral:是否因子中性
        #由于data_structure3-7需要指定是否行业中性，得到结果是原始值绩效或者行业中性后的绩效，而0-2结果本身包含了原始值和中性后绩效
        '''
        data = ['ic_corr','ic_performance','ic_decay','net','group_performance','year_sy','month_sy']
        _dbengine = gp.connect_mysql('result')#连接到reslut数据库
        if ifnuetral == False:
            for i in range(7):
                sql2 = "select column_name from information_schema.COLUMNS where table_name='%s' "%data[i]
                names = list(pd.read_sql(sql2,con=_dbengine)['column_name'])[1:]
                names = str(names).replace('[','(').replace(']',')').replace("'","")               
                self.transto_None(eval(data[i])) #空值处理
                self.insert_data('result',data[i],names,eval(data[i])) #插入数据
                #print('%s数据插入完毕'%sheetnames[i])        
        elif ifnuetral == True:
            for i in range(3,7):
                sql2 = "select column_name from information_schema.COLUMNS where table_name='%s' "%data[i]
                names = list(pd.read_sql(sql2,con=_dbengine)['column_name'])[1:]
                names = str(names).replace('[','(').replace(']',')').replace("'","")      
                self.transto_None(eval(data[i])) #空值处理
                self.insert_data('result',data[i],names,eval(data[i])) #插入数据
                #print('%s数据插入完毕'%sheetnames[i])
        
   
    def create_newtable(self,databasename,tablename,data_structure):
        '''
        databasename：数据库名字
        tablename：表明
        data_structure：字段及相关类型
        创建新的数据表
         drop_sql = "drop table if exists test.OperatingRevenueTTM_week "  
         sql = "create table  if not exists test.OperatingRevenueTTM_week (\
            id int  not null primary key AUTO_INCREMENT,\
            TradingDay datetime,   \
            SecuCode varchar(6),  \
            营业收入TTM float, \
            营业收入TTM同比 float,\
            营业收入TTM环比 float\
            )"
    
        '''
        cursor = self._dbengine1.cursor()
        drop_sql = "drop table if exists %s.%s "%(databasename,tablename)  
        create_sql = "create table  if not exists %s.%s %s"%(databasename,tablename,data_structure)
        
        try:       
            cursor.execute(drop_sql)   
            cursor.execute(create_sql) 
            self._dbengine1.commit()# 提交到数据库执行
        except Exception as e:         
            # 如果发生错误则回滚
            print(e)
            self._dbengine1.rollback()
                
    def insert_data(self,databasename,tablename,names,values):
        '''
        插入数据
        insert_sql = "insert into test.OperatingRevenueTTM_week (dt,SecuCode,营业收入TTM,营业收入TTM同比,营业收入TTM环比)\
                            values(%s,%s,%s,%s,%s)" 
        names = "(dt,SecuCode,营业收入TTM,营业收入TTM同比,营业收入TTM环比)"
        tuple(len(names.split(','))*[%s])
        values=OperatingRevenue_week[:10000]:插入的数据，np.array格式
        databasename = 'growth'
        tablename = 'OperatingRevenue_week'
        
        '''
        cursor =  self._dbengine1.cursor()
        vv = str(('%s,'*len(names.split(','))))[:-1]
        try:
            insert_sql = "insert into %s.%s %s values(%s)"%(databasename,tablename,names,vv)
            values = np.array(values)                 
            #t1 = time()
            for i in range(0,len(values),50000):            
                v1 = values[i:i+50000]
                v1 = tuple(map(tuple,v1))         
                cursor.executemany(insert_sql,v1) 
                self._dbengine1.commit()# 提交到数据库执行          
        except Exception as e:            
            # 如果发生错误则回滚
            self._dbengine1.rollback()
            print(e)         
#        finally:
#            self._dbengine.close()
#            self._dbengine1.close()
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
    
    def connect_mysql(self,table):
        '''
        连接数据库
        '''
        dbengine =  pymysql.connect(host='192.168.1.139',
                           port=3306,
                           user='jydb',
                           password='jydb',
                           database=table,
                           charset='gbk')
        return dbengine
    
    def all因子绩效入库(self,tablename,benchmark_quote,quote_ic,quote_group,how):
        '''
        获取某个数据库中，某个字段表的名称
        tablename = 'value' 数据库名称
        sheetname = 'pb'
        benchmark_quote：基准行情，计算超额收益等绩效用
        quote_ic：计算IC用行情
        quote_group：计算因子分组绩效用
        indicator = '流通市值'
        performance_data_name:绩效结果的名称,list=['ic_corr','ic','ic_decay','net','performance','year_sy','month_sy']
        how='inner'
        '''
        #获取数库中所以数据的表名
        _dbengine = self.connect_mysql(tablename) #连接到该数据库
        sql = "SELECT TABLE_NAME from information_schema.`TABLES` where TABLE_SCHEMA = '%s'"%tablename       
        sheetnames = pd.read_sql(sql,con=_dbengine)
        for i in range(len(sheetnames)):
            sheetname = sheetnames.iloc[i,0]
            #获取数据库表中
            sql2 = "select column_name from information_schema.COLUMNS where table_name='%s' "%sheetname
            indicators = list(pd.read_sql(sql2,con=_dbengine).drop_duplicates()['column_name'])[3:]
            factor = self.get_因子数据(tablename,sheetname,indicators) #获取因子原始数据
            #转为设定频度数据，如月度数据
            quote_factor = self.get_数据合并(quote_ic,factor,how)
               
            for j in range(len(indicators)):
                indicator = indicators[j]
                print("计算的因子是：%s"%indicator)
                ifmktcap = indicator.find("市值")<0 #带有市值的估值因子仅作行业中性化处理，不做市值中性
                #计算因子评价体系,先计算原始因子的绩效
                ic_corr,ic,ic_decay,net,performance,year_sy,month_sy = \
                self.get_因子评价绩效(indicator,'行业一级',quote_factor,quote_group,benchmark_quote,False,ifmktcap)               
                self.insert_to_result(ic_corr,ic,ic_decay,net,performance,year_sy,month_sy,ifnuetral=False)#结果保存至数据库
                #后计算因子中性化后的绩效
                ic_corr,ic,ic_decay,net,performance,year_sy,month_sy = \
                self.get_因子评价绩效(indicator,'行业一级',quote_factor,quote_group,benchmark_quote,True,ifmktcap)               
                self.insert_to_result(ic_corr,ic,ic_decay,net,performance,year_sy,month_sy,ifnuetral=True)

            print("%s表中的因子全部插入数据库"%sheetname)
        print("%s数据库因子绩效结果全部入库"%tablename)
        
        
    
if __name__ == "__main__":
    lastdate = datetime.datetime.today()
    lastdate = datetime.datetime.strftime(lastdate,"%Y%m%d")
    gp = factor_result_to_mysql('m','20071231',lastdate) 
     #提取行情、行业数据，并清洗合并,得到计算IC、分组收益、基准基础数据   
    benchmark_quote,quote_ic,quote_group = gp.get_基础数据('399317')
 
    #获取单个因子绩效 
#    ic_corr,ic,ic_decay,net,performance,year_sy,month_sy = \
#        gp.get_指定因子绩效('test','pb','A股流通市值','行业一级',benchmark_quote,quote_ic,quote_group,ifnuetral=True,ifmktcap=True)

    
    #获取这个因子库的绩效，并入库
    gp.create_result_sheet() #初始化因子库，建表
    gp.all因子绩效入库('value',benchmark_quote,quote_ic,quote_group,'inner')
