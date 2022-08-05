import pandas as pd
import sql

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

class sql:
    def __init__(self):
        ### БД steam
        self.select_steam="SELECT * FROM steam_parser"
        ### БД market csgo
        self.select_market_csgo="Select * FROM market_csgo_prices WHERE price IS NOT NULL;"
        ### БД dmarket
        ### Взаимодействие БД
        self.join_steam_market="""SELECT * FROM steam_parser
                                JOIN LEFT steam_parser.id=market_csgo_prices.id;"""
    @try_exc
    def get_sql(get,data):
        conn = psycopg2.connect("dbname=Steam_Prices user=postgres password=WatEx2252")#Вместо звёздочек сваоя база данных.
        cur = conn.cursor()
        get_con=sql.SQL(get)
        self.data=pd.DataFrame(cur.execute(get_con))
        return self.data
