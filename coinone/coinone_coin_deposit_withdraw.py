from coinone_base import *
import time
import json
import mysql.connector
import traceback
from datetime import date, datetime

ACCESS_TOKEN = '1e46f9f2-1f1f-4948-8a01-1c2f01eb9f56'
SECRET_KEY = bytes('4a173fca-b934-486f-af2e-4fb8550f29af', 'utf-8')

coinone = CoinOne(ACCESS_TOKEN, SECRET_KEY)

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
INSERT INTO coinone_coin_deposit_withdraws (user_id, uuid, currency, txid, type, 
from_address, from_secondary_address, to_address, to_secondary_address,
confirmations, amount, fee, status, created_at, `date`)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Flag to track if there are more transaction to fetch
has_more = True
sleep_time = 0.5  # Adjust sleep time as needed (seconds)
transaction_count = 0  # Initialize transaction count
time_end = 1712620800000
current_time = round(time.time() * 1000)
offset = (24 * 60 * 60 * 1000) * 90
from_ts = current_time
to_ts = current_time
action = '/v2.1/transaction/coin/history'

while from_ts > time_end:
    try :
        from_ts = from_ts - offset
        if from_ts < time_end:
            from_ts = time_end
        to_id = None  # Start from offset 0
        print(f'[START] ', str(from_ts))
        while True:
            payload = {
                'from_ts': from_ts,
                'to_ts': to_ts,
                'to_id': to_id, # id where to history prior to this id. Get the latest history with id = None
                'is_deposit': None, # get both deposit and withdraw
                'size': 2 # get Max=100 transaction
            }
            res = coinone.get_response(action,payload)
            data = json.loads(res)
            if 'transactions' in data and len(data['transactions']) > 0:
                for transaction in data['transactions']:
                    transaction_count +=1
                    print(f'Transaction count {transaction_count}')
                    with open('transaction_krw_list.json', 'a') as f:
                        json.dump(transaction, f, ensure_ascii=False)
                        f.write('\n')
                    timestampSecond = int(transaction["created_at"]) / 1000
                    dobj = datetime.fromtimestamp(timestampSecond)
                    tx_time_formatted = dobj.date()
                    transaction_data = (
                        1, # fake user id
                        transaction["id"],
                        transaction["currency"],
                        transaction["txid"],
                        transaction["type"],
                        transaction["from_address"],
                        transaction["from_secondary_address"],
                        transaction["to_address"],
                        transaction["to_secondary_address"],
                        transaction["confirmations"],
                        transaction["amount"],
                        transaction["fee"],
                        transaction["status"],
                        transaction["created_at"],
                        tx_time_formatted
                    )
                    mycursor.execute(sql_insert_transaction, transaction_data)
                    mydb.commit()
            # get last Id from response to get next list of transaction
                to_id = data['transactions'][-1]['id']
            else:
                break
        to_ts = from_ts - 1
    except Exception as x:
        has_more = False
        print(traceback.format_exc())