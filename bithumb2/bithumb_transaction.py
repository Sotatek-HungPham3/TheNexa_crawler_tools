#! /usr/bin/env python
# XCoin API-call sample script (for Python 3.X)
#
# @author	btckorea
# @date	2017-04-11
#
#
# First, Build and install pycurl with the following commands::
# (if necessary, become root)
#
# https://pypi.python.org/pypi/pycurl/7.43.0#downloads
#
# tar xvfz pycurl-7.43.0.tar.gz
# cd pycurl-7.43.0
# python setup.py --libcurl-dll=libcurl.so install
# python setup.py --with-openssl install
# python setup.py install

import time
import json
import mysql.connector
import traceback
from xcoin_api_client import *
from bithumb_trade_pair import *
from common import *

#CONSTS
USER_ID = 200
#END CONSTS

# Replace with your actual API keys
api_key = "58d7b5ef0175d0342ed84b5f9315e86c"
api_secret = "c804d60dc9796ddead1386094885e828"
USER_ID
api = XCoinAPI(api_key, api_secret);

# MySQL database connection details (replace with yours)
mydb = mysql.connector.connect(
    host="127.0.0.1",
    port="3307",
    user="crawler",
    password="1",
    database="crawler"
)
mycursor = mydb.cursor()

sql_insert_transaction = """
INSERT INTO bithumb_transactions (user_id, search, transfer_date, order_currency, payment_currency,
units, price, amount, fee_currency, fee, order_balance, payment_balance)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Get all pair in bithumb
pairs = get_all_pairs()
lst_tx = {}
# main
for pair in pairs:
    print("[Pairs]: " + pair.get('order_currency') + '/' + pair.get('payment_currency'))
    # get transactions
    rgParams = {
        'endpoint': '/info/user_transactions',
        "count": 50,
        "searchGb": 0,
        "order_currency": pair.get('order_currency'),
        "payment_currency": pair.get('payment_currency')
    }
    # Flag to track if there are more transaction to fetch
    has_more = True
    offset = 0  # Start from offset 0
    sleep_time = 0.2  # Adjust sleep time as needed (seconds)
    transaction_count = 0  # Initialize transaction count
    while has_more:
        try:
            print(offset)
            rgParams['offset'] = offset
            result = api.xcoinApiCall(rgParams['endpoint'], rgParams)
            if result["status"] != "0000" or not result["data"]:
                has_more = False
                break
            print(result)
            for data in result["data"]:
                transaction_count += 1
                print(f'Transaction count {transaction_count}')
                with open('transaction_list.json', 'a') as f:
                    json.dump(data, f, ensure_ascii=False)
                    f.write('\n')
                transaction_data = (
                    USER_ID,  # fake user id
                    data["search"],
                    data["transfer_date"],
                    data["order_currency"],
                    data["payment_currency"],
                    data["units"],
                    data["price"],
                    data["amount"],
                    data["fee_currency"],
                    data["fee"],
                    data["order_balance"],
                    data["payment_balance"]
                )
                key = set_key_trade(data['search'], data['transfer_date'], data['order_currency'], data['payment_currency'], data['order_balance'], data['payment_balance'])
                check_tx = check_duplicate(key, lst_tx)
                if check_tx == True:
                    lst_tx[key] = True
                    mycursor.execute(sql_insert_transaction, transaction_data)
                    mydb.commit()
                else:
                    print(key)
            # Sleep to respect API rate limit
            time.sleep(sleep_time)
        except Exception as x:
            print(traceback.format_exc())
        offset += 1


