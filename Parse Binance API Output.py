#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import datetime as dt


# In[2]:


# !pip install pandas


# In[3]:


api_key = 'wps4QMtjIW0GhKjA13XpwYkz3tpjJO5pqj8MqlY1TpLElKUhMJL7cvupiuQEk2hX'
api_secret = 'sLUzuD1NESEzyTOoR07vE95GbjJwKM4KiJsmOhZHg7Ls2Pfahr9bZoMHNwDer8fY'

from binance.client import Client
client = Client(api_key, api_secret)


# In[ ]:


DATETIME = dt.datetime.now()
# START_DATE = dt.datetime.strptime('2021-09-12', '%Y-%m-%d')
START_DATE = '2021-09-12'
START_DATE_TS = dt.datetime.strptime(START_DATE, '%Y-%m-%d').timestamp() * 1000


# In[ ]:


pairs = [
    'BTCBRL',
    'ETHBRL',
    'ADABRL',
    'BNBBRL',
    'ADAUSDT',
    'BTTUSDT',
    'SHIBUSDT',
    'BNBETH',
    'VETBUSD',
    'VETUSDT',
    'SLPETH',
    'FTTUSDT',
    'USDTBRL',
    'DOTBRL',
]


# In[ ]:


trades = []

for p in pairs:
#     print(p)
    t = client.get_my_trades(symbol=p, startTime=int(START_DATE_TS))
    if t:
        trades.extend(t)

# trades


# In[ ]:


from datetime import datetime

def convert_unix_ts(ts):
    
    if type(ts) != int:
        ts = int(ts)
        
    try:
        new_ts = datetime.utcfromtimestamp(ts) #.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        new_ts = datetime.utcfromtimestamp(ts/1000) #.strftime('%Y-%m-%d %H:%M:%S')
    return new_ts

def parse_trading_pair(pair):

    if pair == 'SHIBUSDT':
        return 'SHIB', 'USDT'
    elif pair == 'USDTBRL':
        return 'USDT', 'BRL'
    else:
        return pair[:3], pair[3:]

convert_unix_ts(1623266016648)


# In[ ]:





# In[ ]:


columns = [
    'Date', 
    'Type', 
    'Buy',
    'Currency (Buy)',
    'Fiat value (Buy)',
    'Sell',
    'Currency (Sell)',
    'Fiat value (Sell)',
    'Fee',
    'Currency (Fee)',
    'Fiat value (Fee)',
    'Exchange',
    'Wallet',
    'Account',
    'Transfer-Code',
    'Comment'
]


# In[ ]:


df_trades = pd.DataFrame(trades)


df_trades.head()


for c in columns:
    
    df_trades[c] = ""
    
df_trades["Type"] = "Trade"

df_trades["Date"] = df_trades['time'].map(convert_unix_ts)

# df_trades = df_trades.set_index(['Date'])
# # filter by start date
# df_trades = df_trades.loc[START_DATE:]

print('Trades fetched.')
print(df_trades.head())


for i, row in df_trades.iterrows():
    
    buy_currency, sell_currency = parse_trading_pair(row['symbol'])
    
    if row['isBuyer']:
        
        df_trades.at[i, 'Buy'] = row['qty']        
        df_trades.at[i, 'Currency (Buy)'] = buy_currency

        df_trades.at[i, 'Sell'] = row['quoteQty']
        df_trades.at[i, 'Currency (Sell)'] = sell_currency
        
    else:

        df_trades.at[i, 'Sell'] = row['qty']        
        df_trades.at[i, 'Currency (Sell)'] = buy_currency

        df_trades.at[i, 'Buy'] = row['quoteQty']
        df_trades.at[i, 'Currency (Buy)'] = sell_currency

    
    df_trades.at[i, 'Currency (Fee)'] = row['commissionAsset']
    df_trades.at[i, 'Fee'] = row['commission']
    df_trades.at[i, 'Date'] = datetime.fromtimestamp(row['time']/1000)

withdraws = client.get_withdraw_history(startTime=int(START_DATE_TS))

df_withdraws = pd.DataFrame(withdraws)

for c in columns:
    
    df_withdraws[c] = ""
    
df_withdraws["Type"] = "Withdraw"

# df_withdraws = df_withdraws.set_index(['Date'])
# # filter by start date
# df_withdraws = df_withdraws.loc[START_DATE:]


# In[ ]:


for i, row in df_withdraws.iterrows():
    
#     buy_currency = row['symbol'][:3]
    sell_currency = row['coin']
    
#     print(buy_currency, sell_currency)
    
    
#     df_trades.at[i, 'Currency (Buy)'] = buy_currency
#     df_trades.at[i, 'Buy'] = row['quoteQty']
    df_withdraws.at[i, 'Sell'] = row['amount']
    
    df_withdraws.at[i, 'Currency (Sell)'] = sell_currency
    
    df_withdraws.at[i, 'Currency (Fee)'] = sell_currency
    df_withdraws.at[i, 'Fee'] = row['transactionFee']
    df_withdraws.at[i, 'Transfer-Code'] = row['txId']
    df_withdraws.at[i, 'Date'] = row['applyTime']


print('Withdraws processed.')
print(df_withdraws)

deposits = client.get_deposit_history(startTime=int(START_DATE_TS))

df_deposits = pd.DataFrame(deposits)

for c in columns:
    
    df_deposits[c] = ""
    
df_deposits["Type"] = "Deposit"

for i, row in df_deposits.iterrows():
    
#     buy_currency = row['symbol'][:3]
    buy_currency = row['coin']
    
#     print(buy_currency, sell_currency)
    
    
    df_deposits.at[i, 'Currency (Buy)'] = buy_currency
    df_deposits.at[i, 'Buy'] = row['amount']
#     df_deposits.at[i, 'Sell'] = row['amount']
    
#     df_deposits.at[i, 'Currency (Sell)'] = sell_currency
    
#     df_deposits.at[i, 'Currency (Fee)'] = buy_currency
#     df_deposits.at[i, 'Fee'] = row['transactionFee']
    df_deposits.at[i, 'Transfer-Code'] = row['txId']
#     df_deposits.at[i, 'Date'] = row['applyTime']
df_deposits['Date'] = df_deposits['insertTime'].map(convert_unix_ts)


print('Deposits processed.')
print(df_deposits)

df_final = pd.concat([df_trades, df_deposits, df_withdraws])[columns]
df_final['Comment'] = 'Parsed via Binance API script at %s' % DATETIME

df_final.head()

df_final.to_clipboard(index=False, header=False)
df_final.to_csv('/tmp/%s_binance_parsed_data.csv' % DATETIME.strftime('%Y-%m-%d'), index=False, header=False)
print(df_final.head())
print('Data copied to clipboard')


# In[ ]:


# client.get_deposit_history(coin='BRL')


# In[ ]:


# 1 Rent vs owning the house
# 2 Appreciation
# 3 How much crypto would increase in the time frame

