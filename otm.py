#%%
import datetime
from persiantools.jdatetime import JalaliDate
import pandas as pd
import numpy as np
import psycopg2
from utils.portfolio import *
from utils.base import *
from utils.report import *
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from utils.config import *
from utils.instance import *
import os
import warnings
warnings.filterwarnings('ignore')

#%%
def getclosingPriceDaily(id_ ):

    url = f'http://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceDailyList/{id_}/0'

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.55 Safari/537.36 Edg/96.0.1054.43'}
    r = requests.get(url, headers= headers, timeout=10)
    data= r.content.decode('utf-8')
    data= json.loads(data)
    closingPriceDaily = pd.DataFrame(data['closingPriceDaily'])
    closingPriceDaily = closingPriceDaily[closingPriceDaily['qTotCap']!=0]
    closingPriceDaily['dEven'] =closingPriceDaily['dEven'].astype('str')
    closingPriceDaily['date'] = closingPriceDaily['dEven'].apply(lambda x: datetime.datetime.strptime(x, '%Y%m%d') )
    closingPriceDaily['date'] = pd.to_datetime(closingPriceDaily['date'])
    closingPriceDaily['date_jalali'] = closingPriceDaily['date'].apply(lambda x: JalaliDate.to_jalali(x)) 
    closingPriceDaily['return'] = (closingPriceDaily['pClosing']/closingPriceDaily['priceYesterday'])-1
    closingPriceDaily =closingPriceDaily.set_index('date',drop=True)
    return closingPriceDaily.iloc[:-5]


# %%
id_= '35366681030756042'
closingPriceDaily = getclosingPriceDaily(id_)
print(closingPriceDaily['return'].std() *  np.sqrt(252) , '')


#%%
df1,stocks = Market_with_askbid_option()
#%%
stocks = stocks[(stocks['isin'].str.startswith('IRO')) &
                (~stocks['name'].str.contains('اختيار'))&
                (~stocks['name'].str.contains('تسهيلات '))&
                (~stocks['symbol'].str.endswith('2'))]
stocks.sort_values('market_cap', ascending=False, inplace=True)
stocks['date'] = pd.to_datetime(stocks['date'])
# %%
volatilities={}
for id_ in stocks.head(40).id.to_list():
    closingPriceDaily = getclosingPriceDaily(id_)
    openingDays = closingPriceDaily['priceMin'].resample('Y').count().mean()
    vol = closingPriceDaily['return'].std() *  np.sqrt(np.ceil(openingDays))
    sym = stocks['symbol'][stocks['id'] == id_].iloc[0]
    volatilities[sym] = vol
    print(f'volatility is : {vol} for {sym}')
    
    
# %%
id_= '2400322364771558'
closingPriceDaily = getclosingPriceDaily(id_)
print(closingPriceDaily['return'].std() *  np.sqrt(252) , '')

px.line(closingPriceDaily, x='date' , y='return')
# %%
closingPriceDaily[closingPriceDaily['dEven']=='20200415']
# %%
