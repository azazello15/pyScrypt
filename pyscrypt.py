"""импортирую библиотеки"""
import httplib2
import os

from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

import pandas as pd
import matplotlib.pyplot as plt

import requests
from bs4 import BeautifulSoup

from sqlalchemy import create_engine

'''определяю функцию для сбора данных с гугл таблицы'''


def get_table():
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    secret_file = os.path.join(os.getcwd(), 'test-table-363807-16fe79488155.json')
    creds_service = ServiceAccountCredentials.from_json_keyfile_name(secret_file, scopes).authorize(httplib2.Http())
    return build('sheets', 'v4', http=creds_service)


service = get_table()
sheet = service.spreadsheets()
sheet_id = '1G4E-pPgEudCPvh-kRSdlXwF0ESkJv6fIFeenoaceQ6w'
resp = sheet.values().get(spreadsheetId=sheet_id, range='Лист1').execute()
df = pd.DataFrame(resp['values'], columns=['№', 'заказ №', 'стоимость,$', 'срок поставки']) #Собираю данные в Датафрейм
table = df[1:]

'''Произвожу парсинг страницы с курсом валют по ЦБ РФ для того что бы определить стоимость в рублях'''

url = 'https://www.banki.ru/products/currency/'
headers = {
    'accept': '*/*',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'
}
r = requests.get(url=url, headers=headers)
soup = BeautifulSoup(r.content, 'lxml')
currencies = soup.find('tr', class_='cb-current-rates__list__item')
currency = currencies.find_all('td')
cur = currency[1].text.replace(',', '.').strip()
cur_list = []
for i in table['стоимость,$']:
    price = float(i) * float(cur)
    cur_list.append(price)

'''Добавляю полученный столбец в основной Датафрейм и вывожу в csv таблицу'''

table.insert(3, 'стоимость, руб', cur_list, True)

'''Произвожу анализ полученных данных и построим зависимости'''
fig = plt.figure(figsize=(10, 6))
plt.xlabel('стоимость')
plt.ylabel('срок поставки')
plt.title('График')
plt.plot(table['стоимость,$'], table['срок поставки'])
plt.show()

'''Импортирую данные в базу данных'''
connect = create_engine('postgresql+psycopg2://' + 'postgres' + ':' + 'password' + '@ip' + ':' + str(5432) + '/' + 'main')
pd.io.sql.to_sql(table, 'm_patient', connect, schema='public')
