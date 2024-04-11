from coinone_base import *
import time
import json
import mysql.connector
import traceback
from datetime import datetime,timedelta

ACCESS_TOKEN = '5d9dd25f-5323-4287-af26-fead54fa1a9e'
SECRET_KEY = bytes('b2d7f716-3057-41a2-8bcc-7ab2a1376463', 'utf-8')

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

sql_insert_trade = """
INSERT INTO coinone_trades (user_id, trade_id, order_id, quote_currency, target_currency, 
order_type, is_ask, is_maker, price,
qty, timestamp, fee_rate, fee, fee_currency, `date`)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Hàm chuyển đổi từ milliseconds sang chuỗi yyyy-mm-dd
def ms_to_date(ms):
    # Chuyển đổi milliseconds sang đối tượng datetime
    date_time = datetime.fromtimestamp(ms / 1000)
    # Trả về chuỗi có định dạng yyyy-mm-dd
    return date_time.strftime('%Y-%m-%d')
# Flag to track if there are more trade to fetch
sleep_time = 0.5  # Adjust sleep time as needed (seconds)
trade_count = 0  # Initialize trade count
distance_days = 90
# Lấy thời gian hiện tại
current_time = datetime.now()

# Chuyển đổi thời gian hiện tại sang milliseconds
current_time_ms = int(current_time.timestamp() * 1000)

time_end = 1409533200000
current_time = round(time.time() * 1000)
offset = (24 * 60 * 60 * 1000) * 90
from_ts = current_time
to_ts = current_time
to_id = None  # Start from offset 0
action='/v2.1/order/completed_orders/all'

while from_ts > time_end:
    from_ts = from_ts - offset
    if from_ts < time_end:
        from_ts = time_end
    to_id = None  # Start from offset 0
    print(f'[START] ', str(from_ts))
    while True:
        payload = {
                'to_trade_id': to_id, # id where to history prior to this id. Get the latest history with id = None
                'size': 100,
                'from_ts': from_ts, #1704162762000,
                'to_ts': to_ts #1711860994000,# get Max=100 trade
        }
        res = coinone.get_response(action,payload)
        data = json.loads(res)
        print(data)
        if 'completed_orders' in data and len(data['completed_orders']) > 0:
            for trade in data['completed_orders']:
                trade_count +=1
                timestampSecond = int(trade["timestamp"]) / 1000
                dobj = datetime.fromtimestamp(timestampSecond)
                tx_time_formatted = dobj.date()
                print(f'Trade count {trade_count}')
                with open('trade_list.json', 'a') as f:
                    json.dump(trade, f, ensure_ascii=False)
                    f.write('\n')
                trade_data = (
                        10, # fake user id
                        trade["trade_id"],
                        trade["order_id"],
                        trade["quote_currency"],
                        trade["target_currency"],
                        trade["order_type"],
                        trade["is_ask"],
                        trade["is_maker"],
                        trade["price"],
                        trade["qty"],
                        trade["timestamp"],
                        trade["fee_rate"],
                        trade["fee"],
                        trade["fee_currency"],
                        tx_time_formatted
                    )
                mycursor.execute(sql_insert_trade, trade_data)
                mydb.commit()
                # get last Id from response to get next list of trade
            to_id = data['completed_orders'][-1]['trade_id']
        else:
            break
    to_ts = from_ts - 1