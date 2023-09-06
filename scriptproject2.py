import sys
import os
import requests
import bs4
import numpy as np
import pandas as pd
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark import SparkConf
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
conf = SparkConf()
conf.setMaster('local')
conf.setAppName('spark-test')
sc = SparkContext(conf=conf)


sqlContext = SQLContext(sc)
sqlContext = SQLContext(sc)
spark = sqlContext
sql = spark.sql


current_date = datetime.now()
day = current_date.day
year = current_date.year
mon = current_date.month
if day < 10:
    day = '0' + str(day)
if mon < 10:
    mon = '0' + str(mon)


url1 = 'http://www.cbr.ru/scripts/XML_daily.asp?date_req={0}/{1}/{2}'.format(day,mon,year)


url_code1 = requests.get(url1)
soup1 = bs4.BeautifulSoup(url_code1.text, 'lxml')

cur_date1 = url1[-10:]

name1 = soup1.find_all('name')
value1 = soup1.find_all('value')
charcode1 = soup1.find_all('charcode')
nominal1 = soup1.find_all('nominal')
numcode1 = soup1.find_all('numcode')


curencies1 = []
for i in range(0,len(name1)):
    row = [cur_date1,
           name1[i].get_text(),
           charcode1[i].get_text(),
           value1[i].get_text().replace(',','.'),
           nominal1[i].get_text(),
           numcode1[i].get_text(),
           ]
    curencies1.append(row)


df1 = pd.DataFrame(curencies1,
                 columns=['date', 'name', 'charcode','value', 'nominal', 'numcode'],
                 dtype = int)

df1 = df1.astype({'nominal': np.int64,
               'numcode': np.int64,
               'value': np.float64}
               )


df_spark = spark.createDataFrame(df1)

sql('create schema if not exists project_currency_rates_student12')

(df_spark.write
         .option("path", "/user/hive/warehouse/project_currency_rates_student12.db/testing_currencies_cbr")
         .mode("append").format('parquet').saveAsTable("project_currency_rates_student12.testing_currencies_cbr"))