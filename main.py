import pandas as pd
from psycopg2 import sql

import market_csgo_prices
import parser_steam_price


class bot:
    def __init__(self):
        market_price=market_csgo_prices.market_csgo()
        steam_price=parser_steam_price.steam_csgo()
        self.data_market_csgo=market_price.sql_select()
        self.data_steam_csgo=steam_price.sql_select()


    def profit(self):
        data=pd.concat([self.data_market_csgo, self.data_steam_csgo])
        return data

    def refrash__db
