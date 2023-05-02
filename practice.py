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
#%%

options, market = fetch_real_time_last_snapshot()
options = extract_call_options_features(options)
# %%
