from coinone_base import *
import time
import json
import mysql.connector
import traceback
from pybit.unified_trading import HTTP
from datetime import datetime,timedelta


ACCESS_TOKEN = 'ab2aea5e-152c-4875-bc34-061093b82b75'
SECRET_KEY = bytes('bbee8463-c685-4c02-ad64-f9cf0d834095', 'utf-8')

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
INSERT INTO bybit_withdraw (user_id, coin, chain, amount, txID, 
status, toAddress, tag, withdrawFee,createTime,updateTime,withdrawId,withdrawType )
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s)
"""

# Flag to track if there are more transaction to fetch
has_more = True
to_id = None  # Start from offset 0
sleep_time = 0.5  # Adjust sleep time as needed (seconds)
transaction_count = 0  # Initialize transaction count
cursor=None
orderId=None
# Lấy thời gian hiện tại
current_time = datetime.now()

# Chuyển đổi thời gian hiện tại sang milliseconds
current_time_ms = int(current_time.timestamp() * 1000)

# Khởi tạo giá trị cho 'to' và 'from' theo milliseconds
to_ts = current_time_ms
from_ts = current_time_ms - (30 * 24 * 60 * 60 * 1000)  # 90 ngày trước
start_ts = datetime(2022, 5, 1).timestamp() * 1000
endTime = datetime(2022, 5, 30).timestamp() * 1000
session = HTTP(
    testnet=False,
    api_key="1Gluy6GE6BfIveSBOZ",
    api_secret="2pinNuvPz6QaPTgkzZKzD82YXQm222N731yW",
)
while has_more:
    try :
     
        data =session.get_withdrawal_records(
        withdrawType=2,
        # cursor=cursor,
        limit=50,
        startTime=start_ts,
        endTime=endTime
)
        print(data)
        for trade in data['result']['rows']:
            transaction_count +=1
            print(f'Trade count {transaction_count}')
            with open('deposit_list.json', 'a') as f:
                json.dump(trade, f, ensure_ascii=False)
                f.write('\n')
            trade_data = (
                1, # fake user id
                trade["coin"],
                trade["chain"],
                trade["amount"],
                trade["txID"],
                trade["status"],
                trade["toAddress"],
                trade["tag"],
                trade["withdrawFee"],
                trade["createTime"],
                trade["updateTime"],
                trade["withdrawId"],
                trade["withdrawType"],
            )
            mycursor.execute(sql_insert_transaction, trade_data)
            mydb.commit()
        # get last Id from response to get next list of trade
        cursor = data['result']['nextPageCursor']
        print(cursor)
        if not cursor:
            break
        # Sleep to respect API rate limit
        time.sleep(sleep_time)
    except Exception as x:
        has_more = False
        print(traceback.format_exc())