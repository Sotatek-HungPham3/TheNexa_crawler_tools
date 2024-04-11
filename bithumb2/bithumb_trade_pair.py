import requests
import json

headers = {"accept": "application/json"}


def get_all_pair_krw():
    url = "https://api.bithumb.com/public/ticker/ALL_KRW"
    response = requests.get(url, headers=headers)
    result_json = json.loads(response.text)
    if (result_json['status'] == "0000" and result_json['data']):
        del result_json['data']['date']
        keysList = result_json['data'].keys()
        return keysList
    return []


def get_all_pair_btc():
    url = "https://api.bithumb.com/public/ticker/ALL_BTC"
    response = requests.get(url, headers=headers)
    result_json = json.loads(response.text)
    if (result_json['status'] == "0000" and result_json['data']):
        del result_json['data']['date']
        keysList = result_json['data'].keys()
        return keysList
    return []


def get_all_pairs():
    tokenKrw = get_all_pair_krw()
    tokenBtc = get_all_pair_btc()
    pairs = []
    for token in tokenKrw:
        pair = {
            'order_currency': token,
            'payment_currency': 'KRW'
        }
        pairs.append(pair)
    for token in tokenBtc:
        pair = {
            'order_currency': token,
            'payment_currency': 'BTC'
        }
        pairs.append(pair)

    return pairs;
