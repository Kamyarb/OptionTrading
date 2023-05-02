import pandas as pd
import numpy as np
import datetime 

def out_of_portfolio_config_generator(options, lookup):
    option_hard_config = pd.DataFrame()
    option_soft_config = pd.DataFrame()
    lookup = lookup[lookup['max_dtm']>25].reset_index(drop=True)

    n_min_value = 100000000
    for option in options.to_dict('records'):
        buy_limit_interest_hard = find_limit_interest(option,
                                   lookup.loc[:, ~lookup.columns.str.contains('soft')], 'buy')
        sell_limit_interest_hard = find_limit_interest(option,
                                    lookup.loc[:, ~lookup.columns.str.contains('soft')], 'sell')
        
        conf_buy_hard = pd.DataFrame({'base_symbol':option['symbol_base'],'base_isin':option['isin_base'], 'oisin':option['isin_option'],
                             'k':option['due_date_price'], 'T':str(option['miladi_due_date']).split(' ')[0],
                             'n-min':round(n_min_value/(option['last_trade_option']*1000)),
                             'lot-size':1, 'side':'buy', 'trade-limit':5,
                             'limit-interest':round(buy_limit_interest_hard,2), 'limit-leverage':0,
                             'c-amount':1}, index=[0])
        conf_sell_hard = pd.DataFrame({'base_symbol':option['symbol_base'], 'base_isin':option['isin_base'], 'oisin':option['isin_option'],
                             'k':option['due_date_price'], 'T':str(option['miladi_due_date']).split(' ')[0],
                             'n-min':round(n_min_value/(option['last_trade_option']*1000)),
                             'lot-size':1, 'side':'sell', 'trade-limit':5,
                             'limit-interest':round(sell_limit_interest_hard,2), 'limit-leverage':10000000,
                             'c-amount':1}, index=[0])
        
        option_hard_config = pd.concat([option_hard_config, conf_buy_hard])
        option_hard_config = pd.concat([option_hard_config, conf_sell_hard])


        buy_limit_interest_soft = find_limit_interest(option,
                                   lookup.loc[:, ~lookup.columns.str.contains('hard')], 'buy')
        sell_limit_interest_soft = find_limit_interest(option,
                                    lookup.loc[:, ~lookup.columns.str.contains('hard')], 'sell')
        
        conf_buy_soft = pd.DataFrame({'base_symbol':option['symbol_base'],'base_isin':option['isin_base'], 'oisin':option['isin_option'],
                             'k':option['due_date_price'], 'T':str(option['miladi_due_date']).split(' ')[0],
                             'n-min':round(n_min_value/(option['last_trade_option']*1000)),
                             'lot-size':1, 'side':'buy', 'trade-limit':5,
                             'limit-interest':round(buy_limit_interest_soft,2), 'limit-leverage':0,
                             'c-amount':1}, index=[0])
        conf_sell_soft = pd.DataFrame({'base_symbol':option['symbol_base'],'base_isin':option['isin_base'], 'oisin':option['isin_option'],
                             'k':option['due_date_price'], 'T':str(option['miladi_due_date']).split(' ')[0],
                             'n-min':round(n_min_value/(option['last_trade_option']*1000)),
                             'lot-size':1, 'side':'sell', 'trade-limit':5,
                             'limit-interest':round(sell_limit_interest_soft,2), 'limit-leverage':10000000,
                             'c-amount':1}, index=[0])
        
        option_soft_config = pd.concat([option_soft_config, conf_buy_soft])
        option_soft_config = pd.concat([option_soft_config, conf_sell_soft])


    return option_hard_config.reset_index(drop=True), option_soft_config.reset_index(drop=True)


def portfolio_config_generator(portfolio, lookup):
    portfolio = portfolio[portfolio['days_until_due']>25].reset_index(drop=True)
    option_hard_config = pd.DataFrame()
    option_soft_config = pd.DataFrame()
    n_min_value = 100000000
    for option in portfolio.to_dict('records'):
        buy_limit_interest_hard = find_limit_interest(option,
                                   lookup.loc[:, ~lookup.columns.str.contains('soft')], 'buy')
        sell_limit_interest_hard = find_limit_interest(option,
                                    lookup.loc[:, ~lookup.columns.str.contains('soft')], 'sell')
        
        conf_buy_hard = pd.DataFrame({'base_symbol':option['symbol_base'], 'base_isin':option['isin_base'], 'oisin':option['isin_option'],
                             'k':option['due_date_price'], 'T':str(option['miladi_due_date']).split(' ')[0],
                             'n-min':round(n_min_value/(option['last_trade_option']*1000)),
                             'lot-size':1, 'side':'buy', 'trade-limit':5,
                             'limit-interest':round(buy_limit_interest_hard,2), 'limit-leverage':0,
                             'c-amount':1}, index=[0])
        conf_sell_hard = pd.DataFrame({'base_symbol':option['symbol_base'], 'base_isin':option['isin_base'], 'oisin':option['isin_option'],
                             'k':option['due_date_price'], 'T':str(option['miladi_due_date']).split(' ')[0],
                             'n-min':round(n_min_value/(option['last_trade_option']*1000)),
                             'lot-size':1, 'side':'sell', 'trade-limit':5,
                             'limit-interest':round(sell_limit_interest_hard,2), 'limit-leverage':10000000,
                             'c-amount':1}, index=[0])
        
        option_hard_config = pd.concat([option_hard_config, conf_buy_hard])
        option_hard_config = pd.concat([option_hard_config, conf_sell_hard])

        if option['side'] == 'buy':
            sell_limit_interest_soft = find_limit_interest(option,
                                    lookup.loc[:, ~lookup.columns.str.contains('hard')], 'sell')
            
            conf_sell_soft = pd.DataFrame({'base_symbol':option['symbol_base'], 'base_isin':option['isin_base'], 'oisin':option['isin_option'],
                             'k':option['due_date_price'], 'T':str(option['miladi_due_date']).split(' ')[0],
                             'n-min':round(n_min_value/(option['last_trade_option']*1000)),
                             'lot-size':1, 'side':'sell', 'trade-limit':5,
                             'limit-interest':round(sell_limit_interest_soft,2), 'limit-leverage':10000000,
                             'c-amount':1}, index=[0])
            option_soft_config = pd.concat([option_soft_config, conf_sell_soft])


        elif option['side'] == 'sell':
            buy_limit_interest_soft = find_limit_interest(option,
                                   lookup.loc[:, ~lookup.columns.str.contains('hard')], 'buy')
            conf_buy_soft = pd.DataFrame({'base_symbol':option['symbol_base'], 'base_isin':option['isin_base'], 'oisin':option['isin_option'],
                             'k':option['due_date_price'], 'T':str(option['miladi_due_date']).split(' ')[0],
                             'n-min':round(n_min_value/(option['last_trade_option']*1000)),
                             'lot-size':1, 'side':'buy', 'trade-limit':5,
                             'limit-interest':round(buy_limit_interest_soft,2), 'limit-leverage':0,
                             'c-amount':1}, index=[0])

            option_soft_config = pd.concat([option_soft_config, conf_buy_soft])
    return option_hard_config.reset_index(drop=True), option_soft_config.reset_index(drop=True)


def generate_proper_excel(df, base_isin):
    base_df = df[df['base_isin']==base_isin]
    sheet_1 = pd.DataFrame({'source':['isin', 'near-rlc-novelty', 'far-rlc-novelty'],
                                    'tse':[base_isin, 300, 300]})
    sheet_2 = base_df.loc[:,~base_df.columns.isin(['base_isin', 'base_symbol'])].T.reset_index()
    sheet_2.columns = sheet_2.iloc[0]
    sheet_2 = sheet_2.iloc[1:]
    return sheet_1, sheet_2


def excel_writer(df, name, num_of_cols):
    df_buy = df[df['side']=='buy']
    df_sell = df[df['side']=='sell']
    for base_isin in df_buy['base_isin'].unique():
 
        base_symbol = df_buy[df_buy['base_isin']==base_isin]['base_symbol'].iloc[0]
        sheet_1, sheet_2 = generate_proper_excel(df_buy, base_isin)
        if (sheet_2.shape[1]-1 <= num_of_cols) or (num_of_cols == -1):
            with pd.ExcelWriter(f"./configs/{base_symbol}_{name}_buy_{datetime.datetime.now()}.xlsx") as writer:
                sheet_1.to_excel(writer, sheet_name="general_config", index=False)
                sheet_2.to_excel(writer, sheet_name="inventory_config", index=False)
        else:
            i = 1
            sheet_2_limited = pd.DataFrame()
            writer_counter = 0
            while i <= sheet_2.shape[1]-1:
                sheet_2_limited = pd.concat([sheet_2_limited, sheet_2.iloc[:, i]])
                if (sheet_2_limited.shape[1] == num_of_cols) or (i==sheet_2.shape[1]-1):
                    sheet_2_limited = pd.concat([sheet_2.iloc[:, 0], sheet_2_limited])
                    writer_counter += 1
                    with pd.ExcelWriter(f"./configs/{base_symbol}_{writer_counter}_{name}_buy_{datetime.datetime.now()}.xlsx") as writer:
                            sheet_2_limited.to_excel(writer, sheet_name="general_config", index=False)
                            sheet_2.to_excel(writer, sheet_name="inventory_config", index=False)
                    sheet_2_limited = pd.DataFrame()
                i += 1

                
    for base_isin in df_sell['base_isin'].unique():
 
        base_symbol = df_sell[df_sell['base_isin']==base_isin]['base_symbol'].iloc[0]
        sheet_1, sheet_2 = generate_proper_excel(df_sell, base_isin)
        if (sheet_2.shape[1]-1 <= num_of_cols) or (num_of_cols == -1):
            with pd.ExcelWriter(f"./configs/{base_symbol}_{name}_sell_{datetime.datetime.now()}.xlsx") as writer:
                sheet_1.to_excel(writer, sheet_name="general_config", index=False)
                sheet_2.to_excel(writer, sheet_name="inventory_config", index=False)
        else:
            i = 1
            sheet_2_limited = pd.DataFrame()
            writer_counter = 0
            while i <= sheet_2.shape[1]-1:
                sheet_2_limited = pd.concat([sheet_2_limited, sheet_2.iloc[:, i]])
                if (sheet_2_limited.shape[1] == num_of_cols) or (i==sheet_2.shape[1]-1):
                    sheet_2_limited = pd.concat([sheet_2.iloc[:, 0], sheet_2_limited])
                    writer_counter += 1
                    with pd.ExcelWriter(f"./configs/{base_symbol}_{writer_counter}_{name}_sell_{datetime.datetime.now()}.xlsx") as writer:
                            sheet_2_limited.to_excel(writer, sheet_name="general_config", index=False)
                            sheet_2.to_excel(writer, sheet_name="inventory_config", index=False)
                    sheet_2_limited = pd.DataFrame()
                i += 1

                
def find_limit_interest(option, lookup, side):
    lookup.columns = ['r_min', 'r_max', 'min_leverage', 'max_leverage', 'min_dtm', 'max_dtm']
    
    option_area = lookup[(option['leverage_last_trade']<lookup['max_leverage'])&
                            (option['leverage_last_trade']>=lookup['min_leverage'])&
                            (option['days_until_due']>=lookup['min_dtm'])&
                            (option['days_until_due']<lookup['max_dtm'])]
    limit_interest = None
    if option_area.shape[0] == 1:
        if side == 'buy':
            limit_interest = option_area['r_min'].iloc[0]

        elif side == 'sell':
            limit_interest = option_area['r_max'].iloc[0]

    return limit_interest


def drop_duplicate_configs(configs_df, path_to_previous):
    previous_configs = pd.read_csv(path_to_previous).to_dict('records')
    current_configs = configs_df.to_dict('records')

    new_configs = []

    for conf in current_configs:
        if conf not in previous_configs:
            new_configs.append(conf)

    return pd.DataFrame(new_configs)

