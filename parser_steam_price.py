import requests
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import pandas as pd
import time
from psycopg2 import sql
import psycopg2
import asyncio
import aiohttp

def try_exc(func,):
    def call_func(*args):
        try:
            conn = psycopg2.connect("dbname=Steam_Prices user=postgres password=WatEx2252")#Вместо звёздочек сваоя база данных.
            conn.autocommit=True
            cur = conn.cursor()
            return func(*args)
        except (Exception) as error:
            print("Ошибка при работе с PostgreSQL", error)
        except MySQLdb.IntegrityError:
            pass
        if conn:
            cur.close()
            conn.close()
            print("Соединение с PostgreSQL закрыто")
    return call_func

class steam_csgo:
    def __init__(self)->None:
        self.cookie={'steamLoginSecure':"76561198413746598%7C%7C07DF3F438641F1E694D391626C014948C45DADAE6"}
        self.column=['value_count','cost','auto_cost','name_item','game','category']

    def parser(self):
        html=None
        data_gen=[]
        for count in range(187):
            URL_TEMPLATE="https://steamcommunity.com/market/search/render/?query=&start={}&count=100&search_descriptions=0&sort_column=popular&sort_dir=desc&appid=730".format(count)
            while (html==None):
                html = requests.get(URL_TEMPLATE, headers={'User-Agent': UserAgent().chrome}, cookies=self.cookie)
                html_text=html.text.replace("\\n", "").replace("\\t", "").replace("\\r", "").replace('\/', '/').replace('\\"','\"')
                soup = BeautifulSoup (html_text, 'html5lib')
                if html.status_code!=200:
                    print(html)
                    print('sleep time 2 minuts')
                    time.sleep(120)
                    html=None
            time.sleep(1)
            for i in soup.find_all('div', class_='market_listing_row market_recent_listing_row market_listing_searchresult'):
                swap=i.get_text('>').split(sep='>')
                swap.remove('Starting at:')
                swap[0]=int(swap[0].replace(',',''))
                swap[1]=float(swap[1].replace(',','.').replace(' pуб.', '').replace(' USD', '').replace('$',''))
                swap[2]=float(swap[2].replace(',','.').replace(' pуб.', '').replace(' USD', '').replace('$',''))
                swap.append(swap[3][swap[3].find('('):swap[3].find(')')+1])
                swap[3]=swap[3].replace("'",'')
                if swap[-1]=='':
                    swap[-1]=None
                data_gen.append(swap)
            self.data=pd.DataFrame(data=data_gen, columns=self.column)
        return self.data

    @try_exc
    def sql_insert(self):
        conn = psycopg2.connect("dbname=Steam_Prices user=postgres password=WatEx2252")#Вместо звёздочек сваоя база данных.
        cur = conn.cursor()
        for i in range(len(self.data)):
                insert=sql.SQL("INSERT INTO steam_parser(value_count, price, price_auto, name_weapon, game, category) VALUES ({}, {}, {}, '{}', '{}', '{}') ON conflict (name_weapon) do nothing;".format(*self.data.loc[i]))
                cur.execute(insert)

    @try_exc
    def sql_update(self):
        conn = psycopg2.connect("dbname=Steam_Prices user=postgres password=WatEx2252")#Вместо звёздочек сваоя база данных.
        cur = conn.cursor()
        for i in range(len(self.data)):
            update=sql.SQL("UPDATE steam_parser SET  value_count={}, price={}, price_auto={}, name_weapon='{}', game='{}', category='{}' WHERE name_weapon = '{}'".format(*self.data.loc[i],self.data.loc[i,'name_item']))
            cur.execute(update)
            conn.commit()
        print("Обновление данных завершено")

    @try_exc
    def sql_select(self):
        conn = psycopg2.connect("dbname=Steam_Prices user=postgres password=WatEx2252")#Вместо звёздочек сваоя база данных.
        cur = conn.cursor()
        select = """SELECT * FROM steam_parser"""
        cur.execute(select)
        self.data=pd.DataFrame(cur.fetchall(), columns=self.column)
        return self.data

    def in_excel(self, name='steam_db.xlsx', index_f=False):
        self.data.to_excel(name, index=index_f)

    def in_csv(self, name='steam_db.csv', index_f=False):
        self.data.to_csv(name, index=index_f)
