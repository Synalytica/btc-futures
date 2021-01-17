from datetime import time
import pandas as pd
import matplotlib.pyplot as plt
import talib
import mplfinance as mpf 

df=pd.read_csv("binance_data.csv")

df['emaShort'] = talib.EMA(df["Close"], timeperiod=10)
df['emaLong'] = talib.EMA(df["Close"], timeperiod=25)
df['adx'] = talib.ADX(df['High'], df['Low'], df['Close'], timeperiod=14)
signal = "None"
inPosition = False
sl=0
tp=0
buyPrice=0
win=0
loss=0
ordersDF = pd.DataFrame(columns = ['Timestamp', 'BuyPrice', 'TP', 'SL', 'W/L'])
orderTime = df.at[0,'Timestamp']


for i in df.index:
    if (signal=="None" and not(inPosition)):
        if ((df.at[i,'emaShort']>df.at[i,'emaLong']) and (df.at[i-1,'emaShort']<df.at[i-1,'emaLong'])) :
            if df.at[i,'adx']<40 and df.at[i,'adx']>30:
                signal = "Short"
        elif ((df.at[i,'emaShort']<df.at[i,'emaLong']) and (df.at[i-1,'emaShort']>df.at[i-1,'emaLong'])) :
            if df.at[i,'adx']<40 and df.at[i,'adx']>30:
                signal = "Long"
    elif not(inPosition) :
        buyPrice = (df.at[i,'Close'] + df.at[i,'Open'])/2
        inPosition=True
        orderTime = df.at[i,'Timestamp']
        if signal=="Long" :
            tp = buyPrice + 150
            sl = buyPrice - 100
        elif signal=="Short" :
            tp = buyPrice - 150
            sl = buyPrice + 100
    else :
        if signal=="Long" :
            if tp<=df.at[i,'High'] :
                win=win+1
                ordersDF = ordersDF.append({'Timestamp': orderTime, 'BuyPrice': buyPrice, 'TP':tp, 'SL':sl, 'W/L': 'W'}, ignore_index = True) 
                signal = "None"
                inPosition = False
                sl=0
                tp=0
                buyPrice=0
            elif sl>=df.at[i,'Low'] :
                loss=loss+1
                ordersDF = ordersDF.append({'Timestamp': orderTime, 'BuyPrice': buyPrice, 'TP':tp, 'SL':sl, 'W/L': 'L'}, ignore_index = True)
                signal = "None"
                inPosition = False
                sl=0
                tp=0
                buyPrice=0
        elif signal=="Short" :
            if tp>=df.at[i,'Low'] :
                win=win + 1
                ordersDF = ordersDF.append({'Timestamp': orderTime, 'BuyPrice': buyPrice, 'TP':tp, 'SL':sl, 'W/L': 'W'}, ignore_index = True)
                signal = "None"
                inPosition = False
                sl=0
                tp=0
                buyPrice=0
            elif sl<=df.at[i,'High'] :
                loss=loss+1
                ordersDF = ordersDF.append({'Timestamp': orderTime, 'BuyPrice': buyPrice, 'TP':tp, 'SL':sl, 'W/L': 'L'}, ignore_index = True)
                signal = "None"
                inPosition = False
                sl=0
                tp=0
                buyPrice=0

total = win+loss
print ("total bets: " + str(total))
print("win: "+ str(win) + " loss: "+ str(loss))
winP = (win/total)*100
print ("win% : " + str(winP))
print (ordersDF.head)
ordersDF.to_csv('orders.csv')