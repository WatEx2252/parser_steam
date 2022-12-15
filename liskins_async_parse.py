import pandas as pd
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from psycopg2 import sql
import psycopg2

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


class parser_liskins():
    def __init__(self):
        self.data=[]
        self.pd_data=[]
        self.URL_TEMPLATE='https://lis-skins.ru/market/csgo/?page='


    async def parser(self,session,url,page_id):
        url=url+str(page_id)
        async with session.get(url) as resp:
            resp_text= await resp.text()
            self.data_in_html(resp_text)


    def data_in_html(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        swap=[]
        f_soup=soup.find('div', class_='skins-market-skins-list')
        for i in f_soup:
            try:
                count+=1
                swap.append(i.find('div', class_='name-inner').string.replace("'",'')+" "+i.find('div', class_='name-exterior').string)
                try:
                    swap.append(i.find('div', class_='name-exterior').string)
                except:
                    swap.append(None)
                swap.append(i.find('div', class_='price').text.replace(',', '').replace(' ', ''))
                try:
                    swap.append(i.find('div', class_='similar-count').text.replace('x', '').replace(' ', ''))
                except:
                    swap.append(1)
                try:
                    swap.append(i.find('div', class_='info-item hold').string.replace('\t','').replace('\n',''))
                except:
                    swap.append(0)
                self.data.append(swap)
                swap=[]
            except Exception as e:
                swap=[]

    async def run(self):
        if type(self.data)!=type(list()):
            self.data=[]
        tasks=[]
        async with aiohttp.ClientSession() as session:
            for page_id in range(65):
                task=asyncio.create_task(self.parser(session, self.URL_TEMPLATE, page_id))
                tasks.append(task)
            await asyncio.gather(*tasks)
        self.pd_data=pd.DataFrame(data=self.data, columns=['name_weapon', 'exterier', 'cost', 'count', 'hold'])
        self.pd_data['cost']=self.pd_data['cost'].astype('float')
        self.pd_data['count']=self.pd_data['count'].astype('int')

    @try_exc
    def sql_update(self):
        conn = psycopg2.connect("dbname=Steam_Prices user=postgres password=WatEx2252")#Вместо звёздочек сваоя база данных.
        cur = conn.cursor()
        for i in range(len(self.pd_data)):
            load=0
            update=sql.SQL("""UPDATE liskins SET name_weapon='{}',exterier='{}', price={}, counter={}, hold_item='{}' WHERE name_weapon = '{}';""".format(*self.pd_data.loc[i], self.pd_data.loc[i,'name_weapon']))
            cur.execute(update)
            conn.commit()
