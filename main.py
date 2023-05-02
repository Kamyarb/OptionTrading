#%%
import datetime
import pandas as pd
import numpy as np
import psycopg2
from utils.portfolio import *
from utils.base import *
from utils.report import *
from utils.config import *
from utils.instance import *
import os
import warnings
warnings.filterwarnings('ignore')

lookup = pd.read_excel('./data/params.xlsx')
lookup = lookup.drop(['ضریب حد پایین', 'ضریب حد بالا'], axis=1)

lookup['min_leverage'] = lookup['leverage'].apply(lambda x: eval(x)[0])
lookup['max_leverage'] = lookup['leverage'].apply(lambda x: eval(x)[1])
lookup['min_dtm'] = lookup['dtm'].apply(lambda x: eval(x)[0])
lookup['max_dtm'] = lookup['dtm'].apply(lambda x: eval(x)[1])
lookup['r_min_soft'] = lookup['r_min_soft']/100
lookup['r_max_soft'] = lookup['r_max_soft']/100

lookup['r_min_hard'] = lookup['r_min_hard']/100
lookup['r_max_hard'] = lookup['r_max_hard']/100

lookup = lookup[lookup['max_dtm']>25].reset_index(drop=True)
lookup.drop(['dtm', 'leverage'], axis=1, inplace=True)


portfolio = pd.read_excel('p.xlsx')
cols = ['نماد', 'تعداد خرید', 'تعداد فروش', 'ق تمام شده',
       'ق میانگین', 'قیمت سهم', 'خالص ارزش فروش']
portfolio = portfolio[cols]
portfolio.columns = ['symbol', 'buy_vol', 'sell_vol', 'final_value',
                     'final_price', 'current_price', 'net_current_value']

portfolio['final_price'] = portfolio['final_price']/1000
portfolio['net_volume'] = portfolio['buy_vol']-portfolio['sell_vol']
portfolio['side'] = np.where((portfolio['buy_vol']-portfolio['sell_vol'])>0, 'buy', 'sell')
portfolio['time'] = datetime.datetime.now()

monthly_trade_statistics = pd.read_csv('./data/daily_trades_statistics.csv')
monthly_trade_statistics = monthly_trade_statistics.groupby('id')['value'].mean().reset_index()
monthly_trade_statistics['id'] = monthly_trade_statistics['id'].astype(str)
engine = create_engine("postgresql://postgres:postgres@78.109.200.41:4330/market")
status, msg = update_portfolio(portfolio, engine)

if status:
    print(msg)
else:
    print("Can't update portfolio")
    raise Exception(msg)

options, market = fetch_real_time_last_snapshot()
options = extract_call_options_features(options)
non_option_portfolio = portfolio[~portfolio['symbol'].str.startswith('ض')].reset_index(drop=True) 
portfolio = portfolio.merge(options, right_on='symbol_option', left_on='symbol')

create_report(portfolio, non_option_portfolio, options)

options = find_proper_symbols(options[~options['symbol_option'].isin(portfolio['symbol'])], monthly_trade_statistics)

hard_o, soft_o = out_of_portfolio_config_generator(options, lookup)
hard_p, soft_p = portfolio_config_generator(portfolio, lookup)

if len(os.listdir('./previous_configs/')) > 0:
    new_hard_o = drop_duplicate_configs(hard_o, './previous_configs/hard_o.csv')
    new_hard_p = drop_duplicate_configs(hard_p, './previous_configs/hard_p.csv')
    new_soft_p = drop_duplicate_configs(soft_p, './previous_configs/soft_p.csv')
else:
    new_hard_o = hard_o.copy()
    new_hard_p = hard_p.copy()
    new_soft_p = soft_p.copy()

NUM_OF_COLS = -1
if new_hard_o.shape[0] > 0:
    excel_writer(new_hard_o ,name='hard_o', num_of_cols=NUM_OF_COLS)

if new_hard_p.shape[0] > 0:
    excel_writer(new_hard_p ,name='hard_p', num_of_cols=NUM_OF_COLS)

if new_soft_p.shape[0] > 0:
    excel_writer(new_soft_p ,name='soft_p', num_of_cols=NUM_OF_COLS)
#%%
if len(os.listdir('./previous_configs/')) == 0:
    create_instances('./configs/')

hard_o.to_csv('./previous_configs/hard_o.csv', index=False)
hard_p.to_csv('./previous_configs/hard_p.csv', index=False)
soft_p.to_csv('./previous_configs/soft_p.csv', index=False)
# %%
