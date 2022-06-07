import psycopg2
import parser_var1
import pandas as pd
import operator
from psycopg2 import sql
import market_csgo_prices

#Декоратор для обработки ошибок с базой данных.
def try_exc(func,):
    def call_func(*args):
        try:
            conn = psycopg2.connect("dbname=***** user=postgres password=*****")#Вместо звёздочек сваоя база данных.
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


#Вставка всех пропущенных данных
@try_exc
def insert_db_steam(data):
    conn = psycopg2.connect("dbname=***** user=postgres password=*****")#Вместо звёздочек сваоя база данных.
    conn.autocommit=True
    cur = conn.cursor()
    load=0
    for i in range(len(data)):
            insert=sql.SQL("INSERT INTO steam_parser(value_count, price, price_auto, name_weapon, game, category) VALUES ({}, {}, {}, '{}', '{}', '{}') ON conflict (name_weapon) do nothing;".format(*data.loc[i]))
            cur.execute(insert)
            if load+1<=round((i/(len(data)))*100):
                load=round((i/(len(data)))*100)
                if round(load/20)==load/20:
                    print(str(load)+'%')
    print("сканирование закончено!!!")




#парсинг данных и получение их в главном файле
def insert_db_pd_steam():
    index=['value_count','cost','auto_cost','name_item','game','category']
    data=pd.DataFrame(columns=index)
    for j in range(187):
        if data.shape[0]==0:
            data=pd.DataFrame(data=parser_var1.find_name_price(j),columns=index)
        else:
            data=pd.concat([data,parser_var1.find_name_price(j)],ignore_index=True)
        print(str(round((100/187)*j,1))+'%')
    return data
#Сохранение данных в 3х типов файлов и обработка ошибок.
@try_exc
def save_data_steam(data, form='excel', file_name='df_steam',index_s=False):
    tag=['.xlsx','.csv']
    if form=='sql':
        conn = psycopg2.connect("dbname=***** user=postgres password=*****")#Вместо звёздочек сваоя база данных.
        cur = conn.cursor()
        print('Вствка всех пропущенных значений')
        insert_db_steam(data)
        load=0
        print('Начато обновление данных')
        for i in range(len(data)):
            update=sql.SQL("UPDATE steam_parser SET  value_count={}, price={}, price_auto={}, name_weapon='{}', game='{}', category='{}' WHERE name_weapon = '{}'".format(*data.loc[i],data.loc[i,'name_item']))
            cur.execute(update)
            conn.commit()
            if load+1<=round((i/(len(data)))*100):
                load=round((i/(len(data)))*100)
                if round(load/20)==load/20:
                    print(str(load)+'%')
    else:
        try:
            if form=='excel':
                data.to_excel(file_name+tag[0],index=index_s)
            elif form=='csv':
                data.to_csv(file_name+tag[0],index=index_s)
            else:
                print('Некорректный формат файла.')
        except (Exception) as error:
            print("Ошибка при работе с фалами pandas:", error)
#Получение и вывод данных с market.csgo,ru
@try_exc
def save_market_csgo_prices(form='excel', file_name='df_steam_market',in_db=False, index_s=False):
    conn = psycopg2.connect("dbname=***** user=postgres password=*****")#Вместо звёздочек сваоя база данных.
    cur = conn.cursor()
    tag=['.xlsx','.csv']
    if in_db==False:
        data_marker=market_csgo_prices.pars_marker_csgo()
    elif in_db==True:
        select = """"Select * FROM market_csgo_prices WHERE price != NULL"""
        data_marker=data_marketcur.execute(select)
    load=0
    if form=='sql':
        if in_db==False:
            for i in range(len(data_marker)):
                update=sql.SQL("UPDATE market_csgo_prices SET name_weapon='{}', volume={}, price={} WHERE name_weapon = '{}';".format(*data_marker.loc[i],data_marker.loc[i,'market_hash_name']))
                cur.execute(update)
                conn.commit()
                if load+1<=round((i/(len(data_marker)))*100):
                    load=round((i/(len(data_marker)))*100)
                    if round(load/20)==load/20:
                        print(str(load)+'%')
        else:
            print("Данные взяты из базы данных. Обновлени не нужно.")
    else:
        try:
            if form=='excel':
                data_marker.to_excel(file_name+tag[0],index=index_s)
            elif form=='csv':
                data_marker.to_csv(file_name+tag[0],index=index_s)
            else:
                print('Некорректный формат файла.')
        except (Exception) as error:
            print("Ошибка при работе с фалами pandas:", error)
