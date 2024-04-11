import time
import json
import mysql.connector
import traceback
from pybit.unified_trading import HTTP

ACCESS_TOKEN = 'ab2aea5e-152c-4875-bc34-061093b82b75'
SECRET_KEY = bytes('bbee8463-c685-4c02-ad64-f9cf0d834095', 'utf-8')


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
INSERT INTO bybit_deposit_offchain (user_id, tx_pri_id, amount, type, coin, 
address, status, createdTime, txID)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Flag to track if there are more transaction to fetch
has_more = True
to_id = None  # Start from offset 0
sleep_time = 0.5  # Adjust sleep time as needed (seconds)
transaction_count = 0  # Initialize transaction count
cursor=None
orderId=None
session = HTTP(
    testnet=False,
    api_key="0JI7AEdW5jLRNQnyW0",
    api_secret="nJ6YK3RtkhwxZz2zEyG6Ta6jUuuDegPxplDr",
)
while has_more:
    try :
     
        data =session.get_internal_deposit_records(
            cursor=cursor
            limit=100
        )
        print(data)
        for trade in data['result']['rows']:
            transaction_count +=1
            print(f'Trade count {transaction_count}')
            with open('deposit_list.json', 'a') as f:
                json.dump(trade, f, ensure_ascii=False)
                f.write('\n')
            trade_data = (
                10, # fake user id
                trade["coin"],
                trade["chain"],
                trade["amount"],
                trade["txID"],
                trade["status"],
                trade["toAddress"],
                trade["tag"],
                trade["depositFee"],
                trade["successAt"],
                trade["confirmations"],
                trade["txIndex"],
                trade["blockHash"],
                trade["batchReleaseLimit"],
                trade["depositType"],
            )
            mycursor.execute(sql_insert_transaction, trade_data)
            mydb.commit()
        # get last Id from response to get next list of trade
        cursor = data['result']['nextPageCursor']
        if not cursor:
            break
        # Sleep to respect API rate limit
        time.sleep(sleep_time)
    except Exception as x:
        has_more = False
        print(traceback.format_exc())