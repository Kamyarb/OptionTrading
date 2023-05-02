#%%
import pandas as pd
import numpy as np
# import sys
# sys.append('..')
from utils.base import *

def create_report(portfolio, non_option_portfolio, options):
    grouped = portfolio.groupby('symbol_base')
    report_dict = {}
    for base_symbol, df in grouped:
        size_of_contracts = (df['final_value']/(df['final_price']*df['net_volume'])).abs()
        net_vol = (df['net_volume']*size_of_contracts).sum()
        conceptual_value = net_vol*df['last_trade_base'].iloc[0]
        if base_symbol == 'اهرم':
            conceptual_value *= 1.6
        report_dict[base_symbol] = conceptual_value

    if non_option_portfolio.shape[0] > 0:
        for dic in non_option_portfolio.to_dict('records'):
            if dic['symbol'] in report_dict.keys():
                report_dict[dic['symbol']] += dic['net_current_value']
            else:
                report_dict[dic['symbol']] = dic['net_current_value']

    report_msg = '📢📢 گزارش پورتفوی\n'
    report_msg += 'ارزش خرید مفهومی هر دارایی پایه\n\n'
    total_conceptual_value = 0
    for symbol, value in report_dict.items():
        if value < 0:
            report_msg += f'🔴 {symbol} : {readable(value)}\n\n'
        else:
            report_msg += f'🟢 {symbol} : {readable(value)}\n\n'

        total_conceptual_value += value
    if total_conceptual_value < 0:
        report_msg += f'🟥 ارزش کل مفهومی خرید : {readable(total_conceptual_value)}'
    
    else:
        report_msg += f'🟩 ارزش کل مفهومی خرید : {readable(total_conceptual_value)}'
        
    telegram_msg(report_msg)
# %%
