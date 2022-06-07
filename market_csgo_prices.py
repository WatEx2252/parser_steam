import requests
import json
import pandas as pd


def pars_marker_csgo():
    URL_TEMPLATE='https://market.csgo.com/api/v2/prices/RUB.json'
    get_json=requests.get(URL_TEMPLATE)
    prices=get_json.json()
    data=pd.DataFrame(prices['items'])
    data['market_hash_name']=data['market_hash_name'].str.replace("'","").replace('\\n','')
    data
    return data
