import requests
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import pandas as pd
import time

def strip_irrelevant_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    for tag in soup.select(','.join([
        f'{t}[nonce]' for t in ('script', 'style')])):
        del tag['nonce']
    return soup.prettify()

def find_name_price(count=0):
    index=['value_count','cost','auto_cost','name_item','game','category']
    data_gen=[]
    cookie={'steamLoginSecure':"76561198413746598%7C%7C17369E9904D511B2675154A471F827E55A7820E8"}
    html='null'
    num_ip=0
    URL_TEMPLATE="https://steamcommunity.com/market/search/render/?query=&start={}&count=100&search_descriptions=1&sort_column=popular&sort_dir=desc&appid=730&category_730_ItemSet%5B%5D=any&category_730_ProPlayer%5B%5D=any&category_730_StickerCapsule%5B%5D=any&category_730_TournamentTeam%5B%5D=any&category_730_Weapon%5B%5D=any".format(count*100)
    
    while (html=='null'):
        html = requests.get(URL_TEMPLATE, headers={'User-Agent': UserAgent().chrome}, cookies=cookie)
        html_text=html.text.replace("\\n", "").replace("\\t", "").replace("\\r", "").replace('\/', '/').replace('\\"','\"')
        soup = BeautifulSoup (html_text, 'html5lib')
        if html.status_code==429:
            print(html)
            print('sleep time 2 minuts')
            time.sleep(120)
            html='null'
        time.sleep(1)
            
    for i in soup.find_all('div', class_='market_listing_row market_recent_listing_row market_listing_searchresult'): 
        swap=i.get_text('>').split(sep='>')
        swap.remove('Starting at:')
        swap[0]=int(swap[0].replace(',',''))
        swap[1]=float(swap[1].replace(',','.').replace(' pуб.', ''))
        swap[2]=float(swap[2].replace(',','.').replace(' pуб.', ''))
        swap.append(swap[3][swap[3].find('('):swap[3].find(')')+1])
        swap[3]=swap[3].replace("'",'')
        if swap[-1]=='':
            swap[-1]=None
        data_gen.append(swap)
    data=pd.DataFrame(data=data_gen, columns=index)
    return data


