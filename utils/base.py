import numpy as np
import pandas as pd
import datetime
import re
import requests
import concurrent.futures
import time
import pytz
import psycopg2
import pandas.io.sql as sqlio
import jdatetime
import json 
from requests_toolbelt import MultipartEncoder
import jdatetime
from bs4 import BeautifulSoup
import mysql.connector
import json

f = open('~/configs/ipAddress.json')
data = json.load(f)
ipAddress= data['ip']

def convert_fa_numbers(input_str):
    """
    This function convert Persian numbers to English numbers.
   
    Keyword arguments:
    input_str -- It should be string
    Returns: English numbers
    """
    mapping = {
        '۰': '0',
        '۱': '1',
        '۲': '2',
        '۳': '3',
        '۴': '4',
        '۵': '5',
        '۶': '6',
        '۷': '7',
        '۸': '8',
        '۹': '9',
        '.': '.',
    }
    return _multiple_replace(mapping, input_str)

def convert_ar_characters(input_str):
    """
    Converts Arabic chars to related Persian unicode char
    :param input_str: String contains Arabic chars
    :return: New str with converted arabic chars
    """
    mapping = {
        'ك': 'ک',
        'دِ': 'د',
        'بِ': 'ب',
        'زِ': 'ز',
        'ذِ': 'ذ',
        'شِ': 'ش',
        'سِ': 'س',
        'ى': 'ی',
        'ي': 'ی',
        
    }
    return _multiple_replace(mapping, input_str)

def convert_ar_numbers(input_str):
    """
    Converts Arabic numbers to Persian numbers
    :param input_str: String contains Arabic numbers
    :return: New str and replaces arabic number with persian numbers
    """
    mapping = {
        '١': '۱',  # Arabic 1 is 0x661 and Persian one is 0x6f1
        '٢': '۲',  # More info https://goo.gl/SPiBtn
        '٣': '۳',
        '٤': '۴',
        '٥': '۵',
        '٦': '۶',
        '٧': '۷',
        '٨': '۸',
        '٩': '۹',
        '٠': '۰',
    }
    return _multiple_replace(mapping, input_str)

def convert_en_numbers(input_str):
        mapping = {
            '0': '۰',
            '1': '۱',
            '2': '۲',
            '3': '۳',
            '4': '۴',
            '5': '۵',
            '6': '۶',
            '7': '۷',
            '8': '۸',
            '9': '۹',
            '.': '.',
        }
        return _multiple_replace(mapping, input_str)

def readable(n):
    human_readable =''
    # n = abs(float(n))
    abs_n = abs(n)
    if abs_n >= 1e7 and abs_n<= 1e10 :
        round_number = n/1e7
        human_readable = '{:,.0f}{}'.format(round_number,   ' میلیون تومان ')
    elif abs_n>1e10:
        round_number = n/1e10
        
        human_readable = '{:,.2f}{}'.format(round_number, ' میلیارد تومان ')
    else:
        round_number = n/10
        human_readable = '{:,.1f}{}'.format(round_number, 'تومان')
        
    return convert_en_numbers(human_readable)

def _multiple_replace(mapping, text):
    """
    Internal function for replace all mapping keys for a input string
    :param mapping: replacing mapping keys
    :param text: user input string
    :return: New string with converted mapping keys to values
    """
    pattern = "|".join(map(re.escape, mapping.keys()))
    return re.sub(pattern, lambda m: mapping[m.group()], str(text))

def Market_with_askbid_option():
    count = 0 
    while count<5:
        url = 'http://old.tsetmc.com/tsev2/data/MarketWatchInit.aspx?h=0&r=0'
        data = requests.get(url, timeout=12)
        content = data.content.decode('utf-8')
        parts = content.split('@')
        if data.status_code != 200 or len(content.split('@')[2])<400:
            count+=1
            time.sleep(1)
        if count ==5:
            raise Exception('ohoh')
        if data.status_code == 200 and len(content.split('@')[2]) > 400:
            break
    parts = content.split('@')
    inst_price = parts[2].split(';')
    market_me = {}
    # Add the Trade and other stuff to dataframe--------
    for item in inst_price:
        item=item.split(',')
        market_me[item[0]]= dict(id=item[0],isin=item[1],symbol=item[2],
                              name=item[3],open_price=float(item[5]),close_price=float(item[6]),
                              last_trade=float(item[7]),count=item[8],volume=float(item[9]),
                              value=float(item[10]),low_price=float(item[11]),
                              high_price=float(item[12]),yesterday_price=int(item[13]),
                              table_id=item[17],group_id=item[18],max_allowed_price=float(item[19]),
                              min_allowed_price=float(item[20]),last_ret=(float(item[7])-float(item[13]))/float(item[13]),
                              close_ret=(float(item[6])-float(item[13]))/float(item[13]),
                              number_of_shares=float(item[21]), market_cap=int(item[21])*int(item[6]))
    # Add the Ask-Bid price Vol tu dataframe --------
    for item in parts[3].split(';'):
        try:
            item=item.split(',')
            if item[1] == '1':
                market_me[item[0]]['bid_price_1']=  float(item[4])
                market_me[item[0]]['bid_vol_1']=  float(item[6])
                
                market_me[item[0]]['ask_price_1']=  float(item[5])
                market_me[item[0]]['ask_vol_1']=  float(item[7])

            if item[1] == '2':
                market_me[item[0]]['bid_price_2']=  float(item[4])
                market_me[item[0]]['bid_vol_2']=  float(item[6])
                
                market_me[item[0]]['ask_price_2']=  float(item[5])
                market_me[item[0]]['ask_vol_2']=  float(item[7])

            if item[1] == '3':
                market_me[item[0]]['bid_price_3']=  float(item[4])
                market_me[item[0]]['bid_vol_3']=  float(item[6])
                
                market_me[item[0]]['ask_price_3']=  float(item[5])
                market_me[item[0]]['ask_vol_3']=  float(item[7])

            if item[1] == '4':
                market_me[item[0]]['bid_price_4']=  float(item[4])
                market_me[item[0]]['bid_vol_4']=  float(item[6])
                
                market_me[item[0]]['ask_price_4']=  float(item[5])
                market_me[item[0]]['ask_vol_4']=  float(item[7])

            if item[1] == '5':
                market_me[item[0]]['bid_price_5']=  float(item[4])
                market_me[item[0]]['bid_vol_5']=  float(item[6])
                
                market_me[item[0]]['ask_price_5']=  float(item[5])
                market_me[item[0]]['ask_vol_5']=  float(item[7])
        except:
            pass

    df = pd.DataFrame(market_me).T
    df['ask_value_1'] = df['ask_vol_1'] * df['ask_price_1']
    df['ask_value_2'] = df['ask_vol_2'] * df['ask_price_2']
    df['ask_value_3'] = df['ask_vol_3'] * df['ask_price_3']
    df['ask_value_4'] = df['ask_vol_4'] * df['ask_price_4']
    df['ask_value_5'] = df['ask_vol_5'] * df['ask_price_5']
    df['bid_value_1'] = df['bid_vol_1'] * df['bid_price_1']
    df['bid_value_2'] = df['bid_vol_2'] * df['bid_price_2']
    df['bid_value_3'] = df['bid_vol_3'] * df['bid_price_3']
    df['bid_value_4'] = df['bid_vol_4'] * df['bid_price_4']
    df['bid_value_5'] = df['bid_vol_5'] * df['bid_price_5']
    df['time'] = datetime.datetime.now()
    df['date'] = datetime.date.today()
    df['jalali_date'] = str(jdatetime.date.today())
    df['symbol'] = df['symbol'].map(lambda x: convert_ar_characters(x))
    df['symbol'] = df['symbol'].map(lambda x: x.replace(' ' , '_'))

    options = df[df['name'].str.startswith('اختيارخ')].reset_index(drop=True).copy()
    option_informations = pd.DataFrame(list(options['name'].apply(extract_due_informations)))
    options = pd.concat([options, option_informations], axis=1)
    options['base_symbol'] = options['base_symbol'].replace({'ص.دارا':'دارا_یکم', 'حافرین':'حآفرین', 'های':'های_وب'})
    options = options.merge(df[['last_trade', 'symbol', 'id', 'isin','min_allowed_price']], right_on='symbol',
                            left_on='base_symbol', suffixes=['_option', '_base'], how='left')                                

    df = df[~df['id'].isin(options['id_option'])]
    df['name'] = df['name'].apply(lambda x : convert_ar_characters(x))
    return options.reset_index(drop=True), df.reset_index(drop=True)


def extract_call_options_features(options):
    options['in_the_money_percentage'] = 1 - (options['due_date_price'] /options['last_trade_base'])
    options['ask_price_1_w_o_c'] = options['ask_price_1']
    options['bid_price_1_w_o_c'] = options['bid_price_1']

    options['ask_price_1'] = options.apply(lambda x: x['ask_price_1']*(1-0.00103)\
                                                if x['ask_price_1']!=0 else np.nan ,axis=1)

    options['bid_price_1'] = options.apply(lambda x: x['bid_price_1']*(1+0.00103)\
                                                    if x['bid_price_1']!=0 else np.nan ,axis=1)                                                

    options = options.dropna(subset=['ask_price_1', 'bid_price_1'])
    options['leverage_ask_price_1'] = options.apply(lambda x: x['last_trade_base']/x['ask_price_1']\
                                                    if x['ask_price_1']!=0 else np.nan ,axis=1)
    options['leverage_bid_price_1'] = options.apply(lambda x: x['last_trade_base']/x['bid_price_1']\
                                                    if x['bid_price_1']!=0 else np.nan ,axis=1)

    options['leverage_last_trade'] = options.apply(lambda x: x['last_trade_base']/x['last_trade_option']\
                                                    if x['last_trade_option']!=0 else np.nan ,axis=1)

    options['interest_rate_last_trade'] = ((options['due_date_price']/(options['last_trade_base']\
                        -options['last_trade_option']))**(round(365/options['days_until_due'], 4)))-1


    options['interest_rate_ask_price_1'] = ((options['due_date_price']/(options['last_trade_base']\
                        -options['ask_price_1']))**(round(365/options['days_until_due'], 4)))-1

    options['interest_rate_bid_price_1'] = ((options['due_date_price']/(options['last_trade_base']\
                        -options['bid_price_1']))**(round(365/options['days_until_due'], 4)))-1
    
    return options.reset_index(drop=True)

def extract_due_informations(name):
    name = name.split('-')
    due_date_price = int(name[1])
    base_symbol = convert_ar_characters(name[0].split(' ')[1]).replace(' ' , '_')
    due_date = name[2].strip()
    if '/' in due_date:
        splited_due_date = due_date.split('/')
        if len(splited_due_date[0]) == 2:
            splited_due_date[0] = '14' + splited_due_date[0]
        due_date = jdatetime.datetime.strptime('-'.join(splited_due_date), '%Y-%m-%d')
    else:
        due_date = jdatetime.datetime.strptime(due_date, '%Y%m%d')

    return {'base_symbol':base_symbol, 'jalali_due_date':str(due_date),
            'days_until_due':(due_date - jdatetime.datetime.now()).days,
            'miladi_due_date':due_date.togregorian(), 'due_date_price':due_date_price}

def find_proper_symbols(options, monthly_trade_statistics, min_days_until_due=26):
    monthly_trade_statistics = monthly_trade_statistics[monthly_trade_statistics['value']>1e9]
    options = options[options['id_option'].isin(monthly_trade_statistics['id'])]
    options = options[options['days_until_due']>=min_days_until_due]
    valid_base_symbols = pd.Series(options['base_symbol'].unique()).apply\
                        (lambda x: x if options[options['base_symbol']==x]\
                                        ['symbol_option'].unique().shape[0]>1 else None).dropna()
    options = options[options['base_symbol'].isin(valid_base_symbols)]
    return options.reset_index(drop=True)


def fetch_real_time_last_snapshot():
    i = 0 
    while i < 10:
        try:
            options_snapshot, market_snapshot = Market_with_askbid_option()
            # time.sleep(1)
            break
        except Exception as e:
            time.sleep(i*0.7)
            i+=1
    return options_snapshot, market_snapshot

# def option_details(option_id):
    
    
def telegram_msg(msg , chat_id = '-1001654392363'):
    headers = {'Content-type': 'application/json'}
    payload = {"bot-name" : "hermes" , 
              "chat-id" : chat_id,
              "message" : msg,
              "parse-mode" : "html"}
    r = requests.post(f'http://{ipAddress}/send-message', 
                      headers = headers,
                      data=json.dumps(payload), timeout=10)

    if r.status_code != 200:
        print('------------------------')
        print(r.status_code)
        print('len: ', len(msg))
        print('------------------------')
        raise Exception(f'Error while sending to telegram, status_code is {r.status_code}')
    
    
import numpy as np
from scipy.stats import norm

N_prime = norm.pdf
N = norm.cdf

class BlackScholes:
    """ 
    Class to calculate (European) call and put option prices through the Black-Scholes formula 
    without dividends
    
    :param S: Price of underlying stock
    :param K: Strike price
    :param T: Time till expiration (in years)
    :param r: Risk-free interest rate (0.05 indicates 5%)
    :param sigma: Volatility (standard deviation) of stock (0.15 indicates 15%)
    """
    @staticmethod
    def _d1(S, K, T, r, sigma):
        return (1 / (sigma * np.sqrt(T))) * (np.log(S/K) + (r + sigma**2 / 2) * T)
    
    def _d2(self, S, K, T, r, sigma):
        return self._d1(S, K, T, r, sigma) - sigma * np.sqrt(T)
    
    def call_price(self, S, K, T, r, sigma):
        """ Main method for calculating price of a call option """
        d1 = self._d1(S, K, T, r, sigma)
        d2 = self._d2(S, K, T, r, sigma)
        return norm.cdf(d1) * S - norm.cdf(d2) * K * np.exp(-r*T)
    
    def put_price(self, S, K, T, r, sigma):
        """ Main method for calculating price of a put option """
        d1 = self._d1(S, K, T, r, sigma)
        d2 = self._d2(S, K, T, r, sigma)
        return norm.cdf(-d2) * K * np.exp(-r*T) - norm.cdf(-d1) * S
    
    def call_in_the_money(self, S, K, T, r, sigma):
        """ 
        Calculate probability that call option will be in the money at
        maturity according to Black-Scholes.
        """
        d2 = self._d2(S, K, T, r, sigma)
        return norm.cdf(d2)
    
    def put_in_the_money(self, S, K, T, r, sigma):
        """ 
        Calculate probability that put option will be in the money at
        maturity according to Black-Scholes.
        """
        d2 = self._d2(S, K, T, r, sigma)
        return 1 - norm.cdf(d2)
    
    
def black_scholes_call(S, K, T, r, sigma):
    '''

    :param S: Asset price
    :param K: Strike price
    :param T: Time to maturity
    :param r: risk-free rate (treasury bills)
    :param sigma: volatility
    :return: call price
    '''

    ###standard black-scholes formula
    d1 = (np.log(S / K) + (r + sigma ** 2 / 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)

    call = S * N(d1) -  N(d2)* K * np.exp(-r * T)
    return call

def vega(S, K, T, r, sigma):
    '''

    :param S: Asset price
    :param K: Strike price
    :param T: Time to Maturity
    :param r: risk-free rate (treasury bills)
    :param sigma: volatility
    :return: partial derivative w.r.t volatility
    '''

    ### calculating d1 from black scholes
    d1 = (np.log(S / K) + (r + sigma ** 2 / 2) * T) / sigma * np.sqrt(T)

    #see hull derivatives chapter on greeks for reference
    vega = S * N_prime(d1) * np.sqrt(T)
    return vega



def implied_volatility_call(C, S, K, T, r, tol=0.0001,
                            max_iterations=100):
    '''

    :param C: Observed call price
    :param S: Asset price
    :param K: Strike Price
    :param T: Time to Maturity
    :param r: riskfree rate
    :param tol: error tolerance in result
    :param max_iterations: max iterations to update vol
    :return: implied volatility in percent
    '''


    ### assigning initial volatility estimate for input in Newton_rap procedure
    sigma = 0.3
    
    for i in range(max_iterations):

        ### calculate difference between blackscholes price and market price with
        ### iteratively updated volality estimate
        diff = black_scholes_call(S, K, T, r, sigma) - C

        ###break if difference is less than specified tolerance level
        if abs(diff) < tol:
            print(f'found on {i}th iteration')
            print(f'difference is equal to {diff}')
            break

        ### use newton rapshon to update the estimate
        sigma = sigma - diff / vega(S, K, T, r, sigma)

    return sigma



class OptionStrategies:
    @staticmethod
    def short_straddle(S, K, T, r, sigma):
        call = BlackScholes().call_price(S, K, T, r, sigma)
        put = BlackScholes().put_price(S, K, T, r, sigma)
        return - put - call
    
    @staticmethod
    def long_straddle(S, K, T, r, sigma):
        call = BlackScholes().call_price(S, K, T, r, sigma)
        put = BlackScholes().put_price(S, K, T, r, sigma)
        return put + call
    
    @staticmethod
    def short_strangle(S, K1, K2, T, r, sigma):
        assert K1 < K2, f"Please make sure that K1 < K2. Now K1={K1}, K2={K2}"
        put = BlackScholes().put_price(S, K1, T, r, sigma)
        call = BlackScholes().call_price(S, K2, T, r, sigma)
        return - put - call
    
    @staticmethod
    def long_strangle(S, K1, K2, T, r, sigma):
        assert K1 < K2, f"Please make sure that K1 < K2. Now K1={K1}, K2={K2}"
        put = BlackScholes().put_price(S, K1, T, r, sigma)
        call = BlackScholes().call_price(S, K2, T, r, sigma)
        return put + call
    
    @staticmethod
    def short_put_butterfly(S, K1, K2, K3, T, r, sigma):
        assert K1 < K2 < K3, f"Please make sure that K1 < K2 < K3. Now K1={K1}, K2={K2}, K3={K3}"
        put1 = BlackScholes().put_price(S, K1, T, r, sigma)
        put2 = BlackScholes().put_price(S, K2, T, r, sigma)
        put3 = BlackScholes().put_price(S, K3, T, r, sigma)
        return - put1 + 2 * put2 - put3
    
    @staticmethod
    def long_call_butterfly(S, K1, K2, K3, T, r, sigma):
        assert K1 < K2 < K3, f"Please make sure that K1 < K2 < K3. Now K1={K1}, K2={K2}, K3={K3}"
        call1 = BlackScholes().call_price(S, K1, T, r, sigma)
        call2 = BlackScholes().call_price(S, K2, T, r, sigma)
        call3 = BlackScholes().call_price(S, K3, T, r, sigma)
        return call1 - 2 * call2 + call3
    
    @staticmethod
    def short_iron_condor(S, K1, K2, K3, K4, T, r, sigma):
        assert K1 < K2 < K3 < K4, f"Please make sure that K1 < K2 < K3 < K4. Now K1={K1}, K2={K2}, K3={K3}, K4={K4}"
        put1 = BlackScholes().put_price(S, K1, T, r, sigma)
        put2 = BlackScholes().put_price(S, K2, T, r, sigma)
        call1 = BlackScholes().call_price(S, K3, T, r, sigma)
        call2 = BlackScholes().call_price(S, K4, T, r, sigma)
        return put1 - put2 - call1 + call2
    
    @staticmethod
    def long_iron_condor(S, K1, K2, K3, K4, T, r, sigma):
        assert K1 < K2 < K3 < K4, f"Please make sure that K1 < K2 < K3 < K4. Now K1={K1}, K2={K2}, K3={K3}, K4={K4}"
        put1 = BlackScholes().put_price(S, K1, T, r, sigma)
        put2 = BlackScholes().put_price(S, K2, T, r, sigma)
        call1 = BlackScholes().call_price(S, K3, T, r, sigma)
        call2 = BlackScholes().call_price(S, K4, T, r, sigma)

        return - put1 + put2 + call1 - call2
    
    
    
def call_implied_volatility(price, S, K, T, r):
    """ Calculate implied volatility of a call option up to 2 decimals of precision. """
    sigma = 0.0001
    while sigma < 1:
        d1 = BlackScholes()._d1(S, K, T, r, sigma)
        d2 = BlackScholes()._d2(S, K, T, r, sigma)
        price_implied = S * norm.cdf(d1) - K * np.exp(-r*T) * norm.cdf(d2)
        if price - price_implied < 0.0001:
            return sigma
        sigma += 0.0001
    return "Not Found"

def put_implied_volatility(price, S, K, T, r):
    """ Calculate implied volatility of a put option up to 2 decimals of precision. """
    sigma = 0.0001
    while sigma < 1:
        call = BlackScholes().call_price(S, K, T, r, sigma)
        price_implied = K * np.exp(-r*T) - S + call
        if price - price_implied < 0.0001:
            return sigma
        sigma += 0.0001
    return "Not Found"