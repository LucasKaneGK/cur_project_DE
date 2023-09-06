import sys
import os
import requests
import bs4
import numpy as np
import pandas as pd
from datetime import datetime
import psycopg2

DB_HOST = '87.242.126.7'
DB_USER = 'student12'
DB_USER_PASSWORD = 'student12_password'
DB_NAME = 'sql_ex_for_student12'

conn = psycopg2.connect(host=DB_HOST, user=DB_USER, password=DB_USER_PASSWORD, dbname=DB_NAME)

cursor = conn.cursor()


current_date = datetime.now()
day = current_date.day
year = current_date.year
mon = current_date.month
if day < 10:
    day = '0' + str(day)
if mon < 10:
    mon = '0' + str(mon)
    
url1 = 'http://www.cbr.ru/scripts/XML_daily.asp?date_req={0}/{1}/{2}'.format(day,mon,year)

day = url1[-10:-8]
month = url1[-7:-5]
year = url1[-4:]
cur_date1 = year+'-'+month+'-'+day

url_code1 = requests.get(url1)
soup1 = bs4.BeautifulSoup(url_code1.text, 'lxml')


name1 = soup1.find_all('name')
value1 = soup1.find_all('value')
charcode1 = soup1.find_all('charcode')
nominal1 = soup1.find_all('nominal')
numcode1 = soup1.find_all('numcode')


currencies1 = []
for i in range(0,len(name1)):
    row = [cur_date1,
           name1[i].get_text(),
           charcode1[i].get_text(),
           float(value1[i].get_text().replace(',','.')),
           int(nominal1[i].get_text()),
           int(numcode1[i].get_text()),
           ]
    currencies1.append(row)



for i in currencies1:
    cursor.execute("""insert into cur_r values ('{0}', '{1}', '{2}', {3}, {4}, {5})""".format(i[0],
                                                                                               i[1],
                                                                                               i[2],
                                                                                               i[3],
                                                                                               i[4],
                                                                                               i[5]))

conn.commit()
conn.close()