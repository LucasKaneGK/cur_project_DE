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
    
url2 = 'https://www.cbr.ru/scripts/xml_metall.asp?date_req1={0}/{1}/{2}&date_req2={0}/{1}/{2}'.format(day,mon,year)

day = url2[-10:-8]
month = url2[-7:-5]
year = url2[-4:]
cur_date2 = year+'-'+month+'-'+day

url_code2 = requests.get(url2)
soup2 = bs4.BeautifulSoup(url_code2.text, 'lxml')

buy = soup2.find_all('buy')
sell = soup2.find_all('sell')

metals = []
met = ['Gold', 'Silver', 'Platinum','Palladium']
for i in range(0,len(buy)):
    row = [cur_date2,
           met[i],
           float(buy[i].get_text().replace(',','.')),
           float(sell[i].get_text().replace(',','.'))
           ]
    metals.append(row)
    
g1 = metals[0][2]
g2 = metals[0][3]

s1 = metals[1][2]
s2 = metals[1][3]

p1 = metals[2][2]
p2 = metals[2][3]

pa1 = metals[3][2]
pa2 = metals[3][3]

cursor.execute("""insert into m_cur values ('{0}', 'Gold', {1}, {2}),
                                                   ('{0}', 'Silver', {3}, {4}),
                                                   ('{0}', 'Platinum', {5}, {6}),
                                                   ('{0}', 'Palladium', {7}, {8})""".format(cur_date2, g1, g2,s1,s2,p1,p1,pa1,pa2))
               

               
conn.commit()

conn.close()
