import hashlib
import json
from binance.client import Client
import requests
import hmac
from base_call_api import *



class CrawlBinance:
    API_KEY = 'hA4Z4878Ek3GI2dnBLOXSw8PzvfB062cuv5wTRWomwvEEDGzMTps66YFf2jVXAWy'
    SECRET_KEY = 'MzWCjMcSy5VIo6TAhtI8z2K2yjyQzoabv3hbnkFkYhNK09vsCWjMHWQQZ98Rw5cV'

    def crawl_get_deposit_history(self, start_time, end_time):
        print(f'[Start time]: ' + str(start_time))
        print(f'[End time]: ' + str(end_time))
        client = Client(api_key=self.API_KEY, api_secret=self.SECRET_KEY)
        data = client.get_deposit_history(startTime=start_time, endTime=end_time, recvWindow=60000, status=1)
        print(f'[Data]: ' + str(json.dumps(data)))
        return data

    def crawl_get_deposit_history_v2(self, offset, limit, start_time, end_time):
        print(f'[Offset]: ' + str(offset))
        print(f'[Limit]: ' + str(limit))
        client = Client(api_key=self.API_KEY, api_secret=self.SECRET_KEY)
        data = client.get_deposit_history(offset=offset,limit=limit, startTime=start_time, endTime=end_time , recvWindow=60000, status=1)
        print(f'[Data]: ' + str(json.dumps(data)))
        return data

    def crawl_get_withdraw_history(self, start_time, end_time):
        print(f'[Start time]: ' + str(start_time))
        print(f'[End time]: ' + str(end_time))
        client = Client(api_key=self.API_KEY, api_secret=self.SECRET_KEY)
        data = client.get_withdraw_history(startTime=start_time, endTime=end_time, recvWindow=60000)
        print(f'[Data]: ' + str(json.dumps(data)))
        return data

    def crawl_get_withdraw_history_v2(self, start_time, end_time, offset, limit):
        print(f'[Offset]: ' + str(offset))
        print(f'[Limit]: ' + str(limit))
        client = Client(api_key=self.API_KEY, api_secret=self.SECRET_KEY)
        data = client.get_withdraw_history(startTime=start_time, endTime=end_time, offset=offset, limit=limit ,recvWindow=60000)
        print(f'[Data]: ' + str(json.dumps(data)))
        return data

    def crawl_pairs(self):
        url = 'https://api.binance.com/api/v3/exchangeInfo?permissions=SPOT'
        r = requests.get(url)

        return r.json()

    def crawl_trades(self, symbol, limit, last_id):
        client = Client(api_key=self.API_KEY, api_secret=self.SECRET_KEY)
        if last_id != None:
            data = client.get_my_trades(symbol=symbol, fromId=last_id, limit=limit, recvWindow=60000)
            return data

        data = client.get_my_trades(symbol=symbol, limit=limit, recvWindow=60000)
        return data

    def crawl_swap(self, start_time, end_time):
        print(f'[START TIME] ' + str(start_time))
        print(f'[END TIME] ' + str(end_time))
        url = '/sapi/v1/bswap/swap'
        params = {
            "startTime": start_time,
            "endTime": end_time,
            "limit": 100,
            "recvWindow": 60000
        }
        r = send_signed_request("GET", url, params)
        print(json.dumps(r))
        return r

    def crawl_swap_v2(self, start_time, end_time, offset, limit):
        print(f'[Offset] ' + str(offset))
        print(f'[Limit] ' + str(limit))
        url = '/sapi/v1/bswap/swap'
        params = {
            "startTime": start_time,
            "endTime": end_time,
            "limit": limit,
            "offset": offset,
            "recvWindow": 60000
        }
        r = send_signed_request("GET", url, params)
        print(json.dumps(r))
        return r
