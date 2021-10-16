import pandas as pd
import ast
import requests
import json
from binance.client import Client
import coinmarketcapapi
from bs4 import BeautifulSoup
from time import sleep
import numpy as np
from pybithumb import Bithumb
import time

pd.options.display.float_format = '{:.5f}'.format

def get_usdt_kr():
    api = "5fba6c30-0548-444a-8102-0df73458b870"
    # cmc = coinmarketcapapi.CoinMarketCapAPI(api)
    exchange_rate_url = "https://search.naver.com/search.naver?where=nexearch&sm=top_hty&fbm=1&ie=utf8&query=%ED%99%98%EC%9C%A8"
    resp = requests.get(exchange_rate_url)
    soup = BeautifulSoup(resp.text, "html.parser")
    # tool = cmc.tools_priceconversion(amount=1, symbol='USDT', convert='USD')
    # 현재 환율
    for rate_tlt in soup.find_all("div", class_="rate_tlt"):
        for strong in rate_tlt.find_all("strong"):
            dollar = float(strong.getText().replace(",", ""))
    # usdt usd 가격 -> kr 패치
    # USDT_kr_price = tool.data['quote']['USD']['price'] * dollar
    USDT_kr_price = dollar
    return USDT_kr_price

def get_bnc_upbit_intersection(upbit_market_all):
    bnc_market_all = client.futures_ticker()

    bnc_market_name = [
        market_info["symbol"][:-4]
        for market_info in bnc_market_all
        if market_info["symbol"][-4:] == "USDT"
    ]

    # 양대마켓에 둘다 있는 코인 (binance는 뒤에 USDT 붙이고, upbit은 앞에 KRW- 를 붙여서 사용하면 된다.)
    intersection = list(set(bnc_market_name) & set(upbit_market_all))

    return intersection


def get_binance_perp_ask(symbol):
    asks_list = client.futures_order_book(symbol=symbol)['bids']
    asks_dict = {'symbol': symbol.split('USDT')[0], 'size': [], 'price': [], 'each_asks_total_price': [],
                 'stack_size': [], 'stack_price': [], 'stack_avg_price': []}
    for i in asks_list:
        asks_dict['stack_size'].append(sum(asks_dict['size']) + float(i[1]))
        asks_dict['stack_price'].append(sum(asks_dict['each_asks_total_price']) + float(i[0]) * float(i[1]))
        asks_dict['size'].append(float(i[1]))
        asks_dict['price'].append(float(i[0]))
        asks_dict['each_asks_total_price'].append(float(i[0]) * float(i[1]))
        asks_dict['stack_avg_price'].append(sum(asks_dict['each_asks_total_price']) / sum(asks_dict['size']))

        if asks_dict['stack_price'][-1] > 10000:
            break

    return asks_dict


def get_binance_perp_df(intersection):
    binance_result = list(map(get_binance_perp_ask, [i + 'USDT' for i in intersection]))
    bnc_df = pd.DataFrame(binance_result)
    return bnc_df


def get_ftx_upbit_intersection(upbit_market_all):
    markets = requests.get('https://ftx.com/api/futures').json()
    df = pd.DataFrame(markets['result'])
    df.set_index('name', inplace=True)
    ftx_market_all = [i.split('-PERP')[0] for i in df[df['type'] == 'perpetual'].index]

    # 양대마켓에 둘다 있는 코인 (binance는 뒤에 USDT 붙이고, upbit은 앞에 KRW- 를 붙여서 사용하면 된다.)
    intersection = list(set(ftx_market_all) & set(upbit_market_all))

    return intersection


def get_ftx_perp_ask(symbol):
    asks_list = requests.get('https://ftx.com/api/markets/' + symbol + '-PERP/orderbook').json()['result']['bids']
    asks_dict = {'symbol': symbol.split('USDT')[0], 'size': [], 'price': [], 'each_asks_total_price': [],
                 'stack_size': [], 'stack_price': [], 'stack_avg_price': []}
    for i in asks_list:
        asks_dict['stack_size'].append(sum(asks_dict['size']) + float(i[1]))
        asks_dict['stack_price'].append(sum(asks_dict['each_asks_total_price']) + float(i[0]) * float(i[1]))
        asks_dict['size'].append(float(i[1]))
        asks_dict['price'].append(float(i[0]))
        asks_dict['each_asks_total_price'].append(float(i[0]) * float(i[1]))
        asks_dict['stack_avg_price'].append(sum(asks_dict['each_asks_total_price']) / sum(asks_dict['size']))

        if asks_dict['stack_price'][-1] > 1000:
            break

    return asks_dict


def get_ftx_perp_df(intersection):
    ftx_result = list(map(get_ftx_perp_ask, [i for i in intersection]))
    ftx_df = pd.DataFrame(ftx_result)
    return ftx_df

def get_okex_upbit_intersection(upbit_market_all):

    okex_list = requests.get('https://www.okex.com/api/swap/v3/instruments').json()
    okex_list = [i['instrument_id'].split('-')[0] for i in okex_list if 'USDT' in i['instrument_id']]

    # 양대마켓에 둘다 있는 코인 (binance는 뒤에 USDT 붙이고, upbit은 앞에 KRW- 를 붙여서 사용하면 된다.)
    intersection = list(set(okex_list) & set(upbit_market_all))

    return intersection

def get_okex_perp_ask(symbol):
    ########################
    symbol_info = {}
    for i in requests.get('https://www.okex.com/api/swap/v3/instruments').json():
        if 'USDT' in i['instrument_id']:
            symbol_info[i['underlying_index']] = i['contract_val']
    ########################

    asks_list = \
    requests.get('https://www.okex.com/api/swap/v3/instruments/' + symbol + '-USDT-SWAP/depth?size=20').json()['asks']
    asks_dict = {'symbol': symbol.split('USDT')[0], 'size': [], 'price': [], 'each_asks_total_price': [],
                 'stack_size': [], 'stack_price': [], 'stack_avg_price': []}

    for i in asks_list:
        asks_dict['stack_size'].append(sum(asks_dict['size']) + float(i[1]) * float(symbol_info[symbol]))
        asks_dict['stack_price'].append(
            sum(asks_dict['each_asks_total_price']) + float(i[0]) * float(i[1]) * float(symbol_info[symbol]))
        asks_dict['size'].append(float(i[1]) * float(symbol_info[symbol]))
        asks_dict['price'].append(float(i[0]))
        asks_dict['each_asks_total_price'].append(float(i[0]) * float(i[1]) * float(symbol_info[symbol]))
        asks_dict['stack_avg_price'].append(sum(asks_dict['each_asks_total_price']) / sum(asks_dict['size']))

        if asks_dict['stack_price'][-1] > 1000:
            break

    return asks_dict

def get_okex_perp_df(intersection):
    okex_result = list(map(get_okex_perp_ask, [i for i in intersection]))
    okex_df = pd.DataFrame(okex_result)
    return okex_df

def get_kimp_df(foreign_df, upbit_df, intersection):
    kimp_list = []
    usdt_krw = get_usdt_kr()
    for i in intersection:
        kimp_list.append(
            get_kimp_cal(i, foreign_df[foreign_df['symbol'] == i], upbit_df[upbit_df['symbol'] == i], usdt_krw))

    result = pd.DataFrame(kimp_list)
    return result


def get_upbit_asks(symbol):
    url = "https://api.upbit.com/v1/orderbook"
    querystring = {"markets": symbol}
    headers = {"Accept": "application/json"}
    response = requests.request("GET", url, headers=headers, params=querystring)
    result = response.json()

    bids_dict = {'symbol': symbol.split('KRW-')[-1], 'size': [], 'price': [], 'each_bids_total_price': [],
                 'stack_size': [], 'stack_price': [], 'stack_avg_price': []}

    for i in result[0]['orderbook_units']:
        bids_dict['stack_size'].append(sum(bids_dict['size']) + float(i['ask_size']))
        bids_dict['stack_price'].append(
            sum(bids_dict['each_bids_total_price']) + float(i['ask_price'] * float(i['ask_size'])))
        bids_dict['size'].append(float(i['ask_size']))
        bids_dict['price'].append(float(i['ask_price']))
        bids_dict['each_bids_total_price'].append(float(i['ask_size']) * float(i['ask_price']))
        bids_dict['stack_avg_price'].append(sum(bids_dict['each_bids_total_price']) / sum(bids_dict['size']))

        if bids_dict['stack_price'][-1] > 10000000:
            break

    return bids_dict

def get_bithumb_asks(symbol):
    result=requests.get('https://api.bithumb.com/public/orderbook/'+symbol+'_KRW').json()['data']['asks']

    bids_dict = {'symbol': symbol, 'size': [], 'price': [], 'each_bids_total_price': [],
                 'stack_size': [], 'stack_price': [], 'stack_avg_price': []}

    for i in result:
        bids_dict['stack_size'].append(sum(bids_dict['size']) + float(i['quantity']))
        bids_dict['stack_price'].append(
            sum(bids_dict['each_bids_total_price']) + (float(i['price']) * float(i['quantity'])))
        bids_dict['size'].append(float(i['quantity']))
        bids_dict['price'].append(float(i['price']))
        bids_dict['each_bids_total_price'].append(float(i['quantity']) * float(i['price']))
        bids_dict['stack_avg_price'].append(sum(bids_dict['each_bids_total_price']) / sum(bids_dict['size']))

        if bids_dict['stack_price'][-1] > 10000000:
            break

    return bids_dict

def get_kimp_cal(symbol, cur_bnc_df, cur_upbit_df, usdt_krw):
    cur_limit = 1000000

    kimp_dict = {'symbol': symbol, 'limit': cur_limit, 'bnc_size': 0, 'upbit_size': 0,
                 'bnc_avg_price': 0, 'upbit_avg_price': 0, 'kimp_by_avg': 0}

    for index, cur_stack_price in enumerate(cur_bnc_df['stack_price'].values[0]):

        if cur_stack_price * float(usdt_krw) > cur_limit:
            gap = cur_stack_price - (cur_limit / float(usdt_krw))  # usdt gap
            gap_size = gap / float(cur_bnc_df['price'].values[0][index])  # size by usdt
            limit_size = float(cur_bnc_df['stack_size'].values[0][index]) - gap_size  # total size by limit
            cur_avg_krw_price = cur_limit / limit_size
            kimp_dict['bnc_size'] = limit_size
            kimp_dict['bnc_avg_price'] = cur_avg_krw_price
            break

    for index, cur_stack_price in enumerate(cur_upbit_df['stack_price'].values[0]):

        if cur_stack_price > cur_limit:
            gap = cur_stack_price - (cur_limit)
            gap_size = gap / float(cur_upbit_df['price'].values[0][index])
            limit_size = float(cur_upbit_df['stack_size'].values[0][index]) - gap_size
            cur_avg_krw_price = cur_limit / limit_size
            kimp_dict['upbit_size'] = limit_size
            kimp_dict['upbit_avg_price'] = cur_avg_krw_price
            break

    kimp_dict['kimp_by_avg'] = round(
        (kimp_dict['upbit_avg_price'] - kimp_dict['bnc_avg_price']) / kimp_dict['upbit_avg_price'] * 100, 2)

    return kimp_dict


def get_upbit_asks_df(intersection):
    upbit_list = []

    for i in intersection:
        res = get_upbit_asks('KRW-' + i)
        upbit_list.append(res)
        sleep(0.1)  # upbit request 제한
    upbit_df = pd.DataFrame(upbit_list)
    return upbit_df

def get_bithumb_asks_df(intersection):
    bithumb_list = []

    for i in intersection:
        res = get_bithumb_asks(i)
        bithumb_list.append(res)
        sleep(0.1)  # upbit request 제한
    bithumb_df = pd.DataFrame(bithumb_list)
    return bithumb_df


################################################################

def get_gap_bnc_df():
    url = "https://api.upbit.com/v1/market/all"
    querystring = {"isDetails": "false"}
    headers = {"Accept": "application/json"}
    response = requests.request("GET", url, headers=headers, params=querystring)
    upbit_market_all = response.json()
    upbit_market_all = [
        market_info["market"].replace("KRW-", "")
        for market_info in upbit_market_all
        if market_info["market"].split("-")[0] == "KRW"
    ]

    global client
    client = Client()
    intersection=get_bnc_upbit_intersection(upbit_market_all)
    # intersection=get_ftx_upbit_intersection()
    bnc_df=get_binance_perp_df(intersection)
    upbit_df=get_upbit_asks_df(intersection)
    bnc_df=get_kimp_df(bnc_df,upbit_df,intersection)
    bnc_df['gap']=(np.array(bnc_df['upbit_avg_price'])/np.array(bnc_df['bnc_avg_price']))/(np.array(bnc_df[bnc_df['symbol']=='BTC']['upbit_avg_price'])/np.array(bnc_df[bnc_df['symbol']=='BTC']['bnc_avg_price']))
    now = time.localtime()
    bnc_df['time'] = time.strftime('%X', now)
    bnc_df=round(bnc_df,4)

    return bnc_df

def get_gap_ftx_df():
    url = "https://api.upbit.com/v1/market/all"
    querystring = {"isDetails": "false"}
    headers = {"Accept": "application/json"}
    response = requests.request("GET", url, headers=headers, params=querystring)
    upbit_market_all = response.json()
    upbit_market_all = [
        market_info["market"].replace("KRW-", "")
        for market_info in upbit_market_all
        if market_info["market"].split("-")[0] == "KRW"
    ]

    global client
    client = Client()
    intersection=get_ftx_upbit_intersection(upbit_market_all)
    ftx_df=get_ftx_perp_df(intersection)
    upbit_df=get_upbit_asks_df(intersection)
    ftx_df=get_kimp_df(ftx_df,upbit_df,intersection)
    ftx_df['gap']=(np.array(ftx_df['upbit_avg_price'])/np.array(ftx_df['bnc_avg_price']))/(np.array(ftx_df[ftx_df['symbol']=='BTC']['upbit_avg_price'])/np.array(ftx_df[ftx_df['symbol']=='BTC']['bnc_avg_price']))
    now = time.localtime()
    ftx_df['time'] = time.strftime('%X', now)
    ftx_df=round(ftx_df,4)

    return ftx_df

def get_gap_okex_df():
    url = "https://api.upbit.com/v1/market/all"
    querystring = {"isDetails": "false"}
    headers = {"Accept": "application/json"}
    response = requests.request("GET", url, headers=headers, params=querystring)
    upbit_market_all = response.json()
    upbit_market_all = [
        market_info["market"].replace("KRW-", "")
        for market_info in upbit_market_all
        if market_info["market"].split("-")[0] == "KRW"
    ]

    global client
    client = Client()
    intersection=get_okex_upbit_intersection(upbit_market_all)
    okex_df=get_okex_perp_df(intersection)
    upbit_df=get_upbit_asks_df(intersection)
    okex_df=get_kimp_df(okex_df,upbit_df,intersection)
    okex_df['gap']=(np.array(okex_df['upbit_avg_price'])/np.array(okex_df['bnc_avg_price']))/(np.array(okex_df[okex_df['symbol']=='BTC']['upbit_avg_price'])/np.array(okex_df[okex_df['symbol']=='BTC']['bnc_avg_price']))
    now = time.localtime()
    okex_df['time'] = time.strftime('%X', now)
    okex_df=round(okex_df,4)

    return okex_df

######################################################################

def get_gap_bnc_df_bithumb():
    bithumb_market_all=Bithumb.get_tickers()

    global client
    client = Client()
    intersection=get_bnc_upbit_intersection(bithumb_market_all)
    # intersection=get_ftx_upbit_intersection()
    bnc_df=get_binance_perp_df(intersection)
    bithumb_df=get_bithumb_asks_df(intersection)
    bnc_df=get_kimp_df(bnc_df,bithumb_df,intersection)
    bnc_df['gap']=(np.array(bnc_df['upbit_avg_price'])/np.array(bnc_df['bnc_avg_price']))/(np.array(bnc_df[bnc_df['symbol']=='BTC']['upbit_avg_price'])/np.array(bnc_df[bnc_df['symbol']=='BTC']['bnc_avg_price']))

    now = time.localtime()
    bnc_df['time']=time.strftime('%X', now)

    bnc_df=round(bnc_df,4)

    return bnc_df

def get_gap_ftx_df_bithumb():
    bithumb_market_all=Bithumb.get_tickers()

    global client
    client = Client()
    intersection=get_ftx_upbit_intersection(bithumb_market_all)
    ftx_df=get_ftx_perp_df(intersection)
    bithumb_df=get_bithumb_asks_df(intersection)
    ftx_df=get_kimp_df(ftx_df,bithumb_df,intersection)
    ftx_df['gap']=(np.array(ftx_df['upbit_avg_price'])/np.array(ftx_df['bnc_avg_price']))/(np.array(ftx_df[ftx_df['symbol']=='BTC']['upbit_avg_price'])/np.array(ftx_df[ftx_df['symbol']=='BTC']['bnc_avg_price']))
    now = time.localtime()
    ftx_df['time'] = time.strftime('%X', now)
    ftx_df=round(ftx_df,4)

    return ftx_df

def get_gap_okex_df_bithumb():
    bithumb_market_all=Bithumb.get_tickers()

    global client
    client = Client()
    intersection=get_okex_upbit_intersection(bithumb_market_all)
    okex_df=get_okex_perp_df(intersection)
    bithumb_df=get_bithumb_asks_df(intersection)
    okex_df=get_kimp_df(okex_df,bithumb_df,intersection)
    okex_df['gap']=(np.array(okex_df['upbit_avg_price'])/np.array(okex_df['bnc_avg_price']))/(np.array(okex_df[okex_df['symbol']=='BTC']['upbit_avg_price'])/np.array(okex_df[okex_df['symbol']=='BTC']['bnc_avg_price']))
    now = time.localtime()
    okex_df['time'] = time.strftime('%X', now)
    okex_df=round(okex_df,4)

    return okex_df

######################################################################

def merge_table():
    a = get_gap_bnc_df()
    b = get_gap_ftx_df()
    c = get_gap_okex_df()
    d = get_gap_bnc_df_bithumb()
    e = get_gap_ftx_df_bithumb()
    f = get_gap_okex_df_bithumb()
    a['foreign_exchange'] = ['Binance'] * len(a)
    b['foreign_exchange'] = ['FTX'] * len(b)
    c['foreign_exchange'] = ['OKEX'] * len(c)
    d['foreign_exchange'] = ['Binance'] * len(d)
    e['foreign_exchange'] = ['FTX'] * len(e)
    f['foreign_exchange'] = ['OKEX'] * len(f)

    a['domestic_exchange'] = ['Upbit'] * len(a)
    b['domestic_exchange'] = ['Upbit'] * len(b)
    c['domestic_exchange'] = ['Upbit'] * len(c)
    d['domestic_exchange'] = ['Bithumb'] * len(d)
    e['domestic_exchange'] = ['Bithumb'] * len(e)
    f['domestic_exchange'] = ['Bithumb'] * len(f)

    merge_df=pd.concat([a,b,c,d,e,f])
    merge_df.columns=['symbol', 'limit', 'foreign_size', 'domestic_size',
                 'foreign_avg_price','domestic_avg_price', 'kimp_by_avg', 'gap',
                              'time','foreign_exchange','domestic_exchange']
    return merge_df