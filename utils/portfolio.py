#%%
from sqlalchemy import create_engine
import pandas as pd
import numpy as np 

    
# with open('../data/blocked_money.txt', 'r') as bm:
#     blocked_money = bm.readlines()[0]
#     blocked_money = blocked_money.replace(',', '')
#     blocked_money = float(blocked_money)
# with open('../data/cash_money.txt', 'r') as cm:
#     cash_money = cm.readlines()[0]
#     cash_money = cash_money.replace(',', '')
#     cash_money = float(cash_money)


def update_portfolio(portfolio, engine):
    portfolio = portfolio[portfolio['symbol'].str.startswith('ض')].reset_index(drop=True)
    try:
        portfolio.to_sql("portfolio", engine, index=False, if_exists='append')
        return True, 'Update portfolio successfully'
    except Exception as e:
        return False, str(e)

def portfolio_size(cash_money, blocked_money):
    portfolio = pd.read_excel('../p.xlsx', engine='openpyxl')
    portfolio['buy_net_volume'] = portfolio['تعداد خرید'] - portfolio['تعداد فروش']
    portfolio['ق میانگین'] = portfolio['ق میانگین']/1000

    long_pos = portfolio[portfolio['buy_net_volume']>0].reset_index(drop=True)
    short_pos = portfolio[portfolio['buy_net_volume']<0].reset_index(drop=True)
    long_positions_value = long_pos['خالص ارزش فروش'].sum()
    short_positions_profit = short_pos['ق تمام شده'].sum() - short_pos['خالص ارزش فروش'].sum()

    portfolio_size = long_positions_value +\
                    short_positions_profit +\
                    (blocked_money-short_pos['ق تمام شده'].sum()) +\
                    cash_money
    return portfolio_size    
# %%
