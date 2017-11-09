
import pandas as pd
import numpy as np
import logging
from functools import reduce
import datetime
import time
import sys
import warnings
warnings.filterwarnings("ignore")

capital_ratio = 0.005 #每次交易的金额
fundid = '099928'
userid = 'dylan'
strategy_id = 'kevin-hls'
products = ["OI","j","jm","pp","i","T","TF","cs",
            "TA","cu","hc","SR","TC","ru","y","pb",
            "ni","zn","c","jd","RO","WS","SF","ag",
            "l","al","m","ZC","p","CF","SM","sn","RM",
            "ER","au","bu","rb","v","FG","a","WT","MA",
            "ME","QM"]

#products= ["l","j","MA","ru","i","SF","ZC","au","pb","jd"]
#products = ['ru']
#products = ["T","IC"]
mytoken = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJfaWQiOiI1OTQ3OWExMWU4YjVkNDY1MjNhZDM3NWYiLCJ1c2VyaWQiOiJkeWxhbiIsImRwdCI6WyLkuqTmmJPpg6giXSwiaWF0IjoxNDk3ODY0NzIxfQ.8bXRtfGzhqfmyju74idjkslO3KGoONaTs91YOf8yFm8'
time_type = '30minute' 
startdate = datetime.date.today().strftime('%Y-%m-%d')


class data:
    pass
#MACD
def hhv(df,length): #滚动最高值
    return df['high'].rolling(window=length).max()
def llv(df,lenght): #滚动最低值
    return df['low'].rolling(window=lenght).min()
def fast_func(df): #macd快慢线
    short_ma =  df['close'].ewm(span=12).mean()
    long_ma = df['close'].ewm(span=26).mean()
    fast = short_ma - long_ma
    return fast
def slow_func(df): #macd快慢线
    short_ma = df['close'].ewm(span=12).mean()
    long_ma = df['close'].ewm(span=26).mean()
    fast = short_ma - long_ma
    slow = fast.ewm(span=9).mean()
    return slow

#获取主要品种主力合约
def get_maian_instrumentids(ctx):
	return ctx.feed.get_instruments(
                			productClasses = ["1"],
                			ranks = [1],
                          products = products, #获取自定义品种合约
                			isTrading = [1]).instruments
                        			

def init(ctx):
    ctx.set_feed(catch=True,name='sw_futures_md',token=mytoken) #设置行情源
    ctx.set_broker(catch=True, name = 'sw_futures_fund', token = mytoken,fundid=fundid) #设置交易商
    instruments = get_maian_instrumentids(ctx)
    instrument_symbols = [x.instrumentid for x in instruments]
    #定义symbolsdata对象，用以保存历史行情数
    ctx.symbolsdata = {}
    for x in instruments:
        ctx.symbolsdata[x.instrumentid] = data()
        ctx.symbolsdata[x.instrumentid].symbolinfo = x
        ctx.symbolsdata[x.instrumentid].symbol = x.instrumentid
        ctx.symbolsdata[x.instrumentid].idata = pd.DataFrame()
        ctx.symbolsdata[x.instrumentid].initflag = False #记录高地价
        ctx.symbolsdata[x.instrumentid].price = None #记录高地价
        ctx.symbolsdata[x.instrumentid].tr = None #该合约能够亏损的幅度
        ctx.symbolsdata[x.instrumentid].trade_price = None #记录开仓信号得收盘价
        ctx.symbolsdata[x.instrumentid].sign = 0 #记录止盈1半后的信号
        ctx.symbolsdata[x.instrumentid].maxprice = 0 #记录分钟收盘价的最高价
        ctx.symbolsdata[x.instrumentid].minprice = 99999999 #记录分钟收盘价的最低价
#        ctx.symbolsdata[x.instrumentid].long_position = 0 #记录持仓数量
#        ctx.symbolsdata[x.instrumentid].short_position = 0 #记录持仓数量
        ctx.symbolsdata[x.instrumentid].fast = 0 #记录持仓数量
        ctx.symbolsdata[x.instrumentid].hl_fast = 0 #记录持仓数量
        ctx.symbolsdata[x.instrumentid].last_hl_fast = 0 #记录持仓数量
        ctx.symbolsdata[x.instrumentid].hl_price = 0
        ctx.symbolsdata[x.instrumentid].last_hl_price = 0 #记录持仓数量
        ctx.symbolsdata[x.instrumentid].hold = 0 #记录持仓数量
    positions = ctx.broker.get_positions().positions
    for x in positions:
        if(x.position > 0 and x.direction == 'long')  :
            ctx.symbolsdata[x.instrumentid].hold = 1
        if(x.position > 0 and x.direction == 'short') : 
            ctx.symbolsdata[x.instrumentid].hold = -1

    ctx.subscribe(
                symbols=instrument_symbols,
                start=startdate,
                end='live',
                type='bar',
                resolution=time_type,
                warmup=235,
                )

    ctx.subscribe(
                symbols=instrument_symbols,
                start='live',
                #end='live',
                type='ticker',
                resolution='snapshot',
                warmup=1,
                )

def get_positions(ctx,symbol,direction):
    positions = ctx.broker.get_positions().positions
    positions = reduce(lambda x, y: x+y, [x.position for x in positions if (x.instrumentid == symbol and x.direction==direction)], 0)  
    return positions

# 计算头寸
def get_capital(ctx):
        return ctx.broker.get_account().capital

def open_by_stop(o, capital, rate): #可以亏损的资金占比
        return int((capital * rate) / (o.symbolinfo.volumemultiple * o.tr))

def call_signal(o,bars):
    #计算是否出现MACD背离信号
    zfz = 95
    ffz = 100-zfz
    f1 = 55
    f2 = 89
    f3 = 144
    f4 = 233
    o.idata = bars[['open','low','high','close']]
    #import pdb
    #pdb.set_trace()
    o.idata['hhv1'] = hhv(o.idata,f1) 
    o.idata['hhv2'] = hhv(o.idata,f2)
    o.idata['hhv3'] = hhv(o.idata,f3)
    o.idata['hhv4'] = hhv(o.idata,f4)
    o.idata['llv1'] = hhv(o.idata,f1)
    o.idata['llv2'] = hhv(o.idata,f2)
    o.idata['llv3'] = hhv(o.idata,f3)
    o.idata['llv4'] = hhv(o.idata,f4)
    o.idata['d1'] = 100 * (o.idata['close'] - o.idata['llv1']) / (o.idata['hhv1'] - o.idata['llv1'])
    o.idata['d2'] = 100 * (o.idata['close'] - o.idata['llv2']) / (o.idata['hhv2'] - o.idata['llv2'])
    o.idata['d3'] = 100 * (o.idata['close'] - o.idata['llv3']) / (o.idata['hhv3'] - o.idata['llv3'])
    o.idata['d4'] = 100 * (o.idata['close'] - o.idata['llv4']) / (o.idata['hhv4'] - o.idata['llv4'])
    o.idata['fast'] = fast_func(o.idata)
    o.idata['slow'] = slow_func(o.idata)
    o.idata['cross'] = np.where((o.idata['fast']>o.idata['slow'])&(o.idata['fast'].shift(1)<=o.idata['slow'].shift(1)),
                               1,np.where((o.idata['fast']<o.idata['slow'])&(o.idata['fast'].shift(1)>=o.idata['slow'].shift(1)),-1,0))
    o.idata['sign'] = np.where(((o.idata['d1']>=zfz)|(o.idata['d2']>=zfz)|(o.idata['d3']>=zfz)|(o.idata['d4']>=zfz)),
                                1,np.where(((o.idata['d1']<=ffz)|(o.idata['d2']<=ffz)|(o.idata['d3']<=ffz)|        
                                (o.idata['d4']<=ffz)),-1,0))
    o.idata['signal'] = 0
    if o.idata['cross'][-1] != 0: #当且仅当本根均线交叉是进行判定
        cross = o.idata[o.idata['cross']!=0]
        if o.idata['cross'][-1] > 0 :      
            last_sign = o.idata[(o.idata.index>=cross.index[-3])]['sign'].min() #是否出现过超跌情况
            if last_sign == -1:              
                max_fast = o.idata[o.idata.index>=cross.index[-3]]['fast'].max()
                min_fast = o.idata[o.idata.index>=cross.index[-2]]['fast'].min()
                min2_fast = o.idata[(o.idata.index>=cross.index[-4])&(o.idata.index<=cross.index[-3])]['fast'].min()
                min_price = o.idata[o.idata.index>=cross.index[-2]]['low'].min()
                min2_price = o.idata[(o.idata.index>=cross.index[-4])&(o.idata.index<=cross.index[-3])]['low'].min() 
                o.minprice = min_price #最低价，止损用
                o.tr = o.idata['close'][-1] - min_price #可以亏损的幅度
                o.fast = max_fast
                o.hl_fast = min_fast
                o.last_hl_fast = min2_fast
                o.hl_price = min_price
                o.last_hl_price = min2_price
                if max_fast<0 and min_fast > min2_fast and min_price<min2_price:
                    o.idata['signal'] = 1
        else:
            last_sign = o.idata[(o.idata.index>=cross.index[-3])]['sign'].max()
            if last_sign == 1:
                min_fast = o.idata[o.idata.index>=cross.index[-3]]['fast'].min()
                max_fast = o.idata[o.idata.index>=cross.index[-2]]['fast'].max()
                max2_fast = o.idata[(o.idata.index>=cross.index[-4])&(o.idata.index<=cross.index[-3])]['fast'].max()
                max_price = o.idata[o.idata.index>=cross.index[-2]]['high'].max()
                max2_price = o.idata[(o.idata.index>=cross.index[-4])&(o.idata.index<=cross.index[-3])]['high'].max() 
                o.maxprice = max_price 
                o.tr = max_price - o.idata['close'][-1]
                o.fast = min_fast
                o.hl_fast = max_fast
                o.last_hl_fast = max2_fast
                o.hl_price = max_price
                o.last_hl_price = max2_price
                if min_fast>0 and max_fast < max2_fast and max_pric>max2_price:
                    o.idata['signal'] = -1      
	
def on_bar(ctx,bars):
    #instruments = get_maian_instrumentids(ctx)
#    print(instruments)
#    print("end")
    t1 = time.time()
    index = bars.index[-1]
    symbol = bars['symbol'][-1] #获取最新的代码
    o = ctx.symbolsdata.get(symbol) #获取代码对应的的对象
    t2 =  time.time()
    #if o.initflag == False:
    logging.info("index=%s,symbole=%s,t2-t1=%s,initflag=%s,hold=%s"
                 %(index,symbol,t2-t1,o.initflag,o.hold))

    if bars['resolution'][-1] == time_type and  o.initflag==True  :
        call_signal(o,bars) #获取交易信号
        t3 = time.time()
         
        long_position = get_positions(ctx,symbol,'long')
        short_position = get_positions(ctx,symbol,'short')
        if long_position > 0:
            o.hold =  1
        elif short_position>0:
            o.hold = -1
        else:
            o.hold = 0
        
        t4 = time.time()
    
#        if long_position > 0:
#            o.hold = 1
#        if short_position >0:
#            o.hold = -1
        logging.info("time=%s,symbol=%s,close=%s,cross=%s,hhv1=%s,fast=%s,slow=%s,signal=%s,sign=%s,long_position=%s,short_position=%s,intiflag=%s,hold=%s"
                         %(index,symbol,o.idata['close'][-1],o.idata['cross'][-1],o.idata['hhv1'][-1],o.idata['fast'][-1],o.idata['slow'][-1],
                           o.idata['signal'][-1],o.idata['sign'][-1],long_position,short_position,o.initflag,o.hold))  
#        if o.idata['cross'][-1] != 0 and o.initflag==True:
#            logging.info("time=%s,symbol=%s,close=%s,cross=%s,signal=%s,long_position=%s,short_position=%s"
#                         %(index,symbol,o.idata['close'][-1],o.idata['cross'][-1],o.idata['signal'][-1],long_position,short_position))  
#            
         
        #底背离开仓买入
        if o.idata['signal'][-1] == 1 and long_position==0 : 
            capital = get_capital(ctx)
            vol = open_by_stop(o,capital,capital_ratio)
            ctx.broker.buy(volume=vol)
            o.trade_price = o.idata['close'][-1]
            #o.long_position = vol
            o.hold = 1
            logging.info('多头开仓 capital=%s, time=%s,symbol=%s,price=%s,signal=%s,vol=%s,fast0=%s,hl_fast=%s,last_hl_fast=%s,hl_price=%s,last_hl_price=%s'
                  %(capital,o.idata.index[-1],symbol,o.idata['close'][-1],o.idata['signal'][-1],vol,o.fast,o.hl_fast,o.last_hl_fast,o.hl_price,o.last_hl_price))
            
            #o.signal.append([symbol,str(o.idata.index[-1]),o.idata['close'][-1],'多开',vol,o.fast,o.hl_fast,o.last_hl_fast,o.hl_price,o.last_hl_price])
         #顶背离卖出
        if  o.idata['signal'][-1] == -1 and short_position==0: 
            capital = get_capital(ctx)
            vol = open_by_stop(o,capital,capital_ratio)
            ctx.broker.sell(volume=vol)
            o.trade_price = o.idata['close'][-1]
            o.hold = -1
            #o.short_position = vol
            logging.info('空头开仓 capital=%s, time=%s,symbol=%s,price=%s,signal=%s,vol=%s,fast0=%s,hl_fast=%s,last_hl_fast=%s,hl_price=%s,last_hl_price=%s'
                  %(capital,o.idata.index[-1],symbol,o.idata['close'][-1],o.idata['signal'][-1],vol,o.fast,o.hl_fast,o.last_hl_fast,o.hl_price,o.last_hl_price))     
            #o.signal.append([symbol,str(o.idata.index[-1]),o.idata['close'][-1],'空开',vol,o.fast,o.hl_fast,o.last_hl_fast,o.hl_price,o.last_hl_price])
           
        #死叉止损、止盈
        if  o.idata['cross'][-1]==-1 and  long_position > 0  :
            ctx.broker.sell(offsetflag='close',volume=long_position)
            o.hold = 0
            logging.info('多头平仓 time=%s,symbol=%s,price=%s,signal=%s,vol=%s,fast0=%s,hl_fast=%s,last_hl_fast=%s,hl_price=%s,last_hl_price=%s'
                  %(o.idata.index[-1],symbol,o.idata['close'][-1],'多平',long_position,o.fast,o.hl_fast,o.last_hl_fast,o.hl_price,o.last_hl_price))
            #o.long_position = 0
            #o.signal.append([symbol,str(o.idata.index[-1]),o.idata['close'][-1],'多平',o.long_position,o.fast,o.hl_fast,o.last_hl_fast,o.hl_price,o.last_hl_price])
            
           
        #创新高止损
        if  o.idata['cross'][-1] == 1 and short_position > 0  :
            ctx.broker.buy(offsetflag='close',volume=short_position)
            o.hold = 0
            #o.short_position= 0
            logging.info('空头平仓  time=%s,symbol=%s,price=%s,signal=%s,vol=%s,hl_fast=%s,last_hl_fast=%s,hl_price=%s,last_hl_price=%s'
                  %(o.idata.index[-1],symbol,o.idata['close'][-1],'空平',short_position,o.fast,o.hl_fast,o.last_hl_fast,o.hl_price,o.last_hl_price))
            #o.signal.append([symbol,str(o.idata.index[-1]),o.idata['close'][-1],'空平',o.short_position,o.fast,o.hl_fast,o.last_hl_fast,o.hl_price,o.last_hl_price])
        t5 = time.time()
        logging.info("index=%s,symbole=%s,总耗时=%s,t2-t1=%s,t3-t2=%s,t4-t3=%s,t5-t4=%s"
                 %(index,symbol,t5-t1,t2-t1,t3-t2,t4-t3,t5-t4))
            
def on_ticker(ctx,ticker):
    t6 = time.time()
    index = ticker.index[-1]
    symbol = ticker['symbol'][-1] #获取最新的代码
    o = ctx.symbolsdata.get(symbol) #获取代码对应的的对象       
    t7 = time.time()
#    if o.initflag==True and o.hold>0 :
#        print('------------------------------------>',symbol,index,o.initflag,o.hold,o.minprice,o.maxprice)
#   
    if o.hold>0:
        o.maxprice = max(o.maxprice,ticker['price'][-1])
        o.minprice = min(o.minprice,ticker['price'][-1])  
        if ticker['price'][-1] <= o.minprice: #创新低止损
            long_position = get_positions(ctx,symbol,'long')
            if long_position >0:
                ctx.broker.sell(offsetflag='close',volume=long_position)
                logging.info('多头创新低止损 hold=%s, time=%s,symbol=%s,price=%s,vol=%s'
                             %(o.hold,index,symbol,ticker['price'][-1],long_position))
                o.hold = 0
        if  ticker['price'][-1] >= o.trade_price*(1+0.5*o.symbolinfo.volumemultiple):#盈利保证金得50%止盈一半
            long_position = get_positions(ctx,symbol,'long')
            if long_position >0:
                ctx.broker.sell(offsetflag='close',volume=int(0.5*long_position))
                o.sign = 1
                logging.info('多头止盈一半 hold=%s, time=%s,symbol=%s,price=%s,vol=%s'
                             %(o.hold,index,symbol,ticker['price'][-1],long_position))    
        if o.sign==1 and ticker['price'][-1] < o.trade_price + 0.5*(o.maxprice -o.trade_price):#浮盈回撤一半止盈
            long_position = get_positions(ctx,symbol,'long')
            if long_position>0:
                ctx.broker.sell(offsetflag='close',volume=long_position)
                o.sign = 0
                logging.info('多头利润回撤一半止盈 hold=%s, time=%s,symbol=%s,price=%s,vol=%s'
                             %(o.hold,index,symbol,ticker['price'][-1],long_position))    
                 
    if o.hold < 0:
        o.maxprice = max(o.maxprice,ticker['price'][-1])
        o.minprice = min(o.minprice,ticker['price'][-1])  
        if ticker['price'][-1] >= o.maxprice: #创新低止损
            short_position = get_positions(ctx,symbol,'short')
            if short_position>0:
                ctx.broker.buy(offsetflag='close',volume=short_position)
                logging.info('空头创新高止损 capital=%s, time=%s,symbol=%s,price=%s,vol=%s'
                             %(capital,index,symbol,ticker['price'][-1],short_position))
                o.hold = 0
            
            
        if  ticker['price'][-1] <= o.trade_price*(1-0.5*o.symbolinfo.volumemultiple):#盈利保证金得50%止盈一半
            short_position = get_positions(ctx,symbol,'short')
            if short_position>0:
                ctx.broker.buy(offsetflag='close',volume=int(0.5*short_position))
                o.sign = 1
                logging.info('空头止盈一半 hold=%s, time=%s,symbol=%s,price=%s,vol=%s'
                             %(o.hold,index,symbol,ticker['price'][-1],short_position))    
        if o.sign==1 and ticker['price'][-1] > o.minprice + 0.5*(o.trade_price-o.minprice):
            short_position = get_positions(ctx,symbol,'short')
            if short_position>0:
                ctx.broker.buy(offsetflag='close',volume=short_position)
                o.sign = 0
                logging.info('空头利润回撤一半止盈 hold=%s, time=%s,symbol=%s,price=%s,vol=%s'
                             %(o.hold,index,symbol,ticker['price'][-1],short_position))    
    
        
def on_end(ctx, sub):
    logging.info("on_end: %s", sub)
    symbol = sub.symbol
    ctx.symbolsdata[symbol].initflag = True
        
               
def on_error(ctx, msg):
     logging.error("error:%s", msg)               
#    print(o.signal)           
#    data2 = pd.DataFrame(np.array(o.signal))
##                          columns=['代码','时间','价格','type','手数','快线','快线高低价','上一根快线高低价','高低价','上一根高低价'])
#    data2.to_excel("C:\\Users\\dylan\\Desktop\\慧网工作\\期货量化策略\\test.xlsx")
#      
 

 
