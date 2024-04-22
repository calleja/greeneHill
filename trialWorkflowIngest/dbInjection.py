import os
import pandas as pd
#import matplotlib.pyplot as plt
import numpy as np
import re
import datetime
import itertools
#from datetimerange import DateTimeRange
import pickle
import sqlalchemy

user = 'root'
password = 'baeldung'
host = '172.17.0.2'
port = 3306
database = 'membership'

url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(user, password, host, port, database)

engine = sqlalchemy.create_engine(url)

with engine.connect() as conn:
    shop.loc[:,['Target_Name', 'Target_Email', 'Activity_Type', 'Subject', 'Activity_Date','ingest_date']].to_sql('shop_log', con=engine, if_exists='replace', index=False)