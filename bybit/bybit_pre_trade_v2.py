from coinone_base import *
import time
import json
import mysql.connector
import traceback
from datetime import datetime,timedelta
from pybit.unified_trading import HTTP


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
INSERT INTO bybit_pre_trade (user_id, symbol, orderId,orderLinkId, side, orderPrice, 
orderQty, leavesQty, orderType, stopOrderType,execFee,execId,execPrice,execQty,execType,execValue,execTime,isMaker,feeRate,tradeIv,markIv,markPrice,indexPrice,underlyingPrice,blockTradeId)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
"""
session = HTTP(
    testnet=False,
    api_key="1Gluy6GE6BfIveSBOZ",
    api_secret="2pinNuvPz6QaPTgkzZKzD82YXQm222N731yW",
)
# Hàm chuyển đổi từ milliseconds sang chuỗi yyyy-mm-dd
def ms_to_date(ms):
    # Chuyển đổi milliseconds sang đối tượng datetime
    date_time = datetime.fromtimestamp(ms / 1000)
    # Trả về chuỗi có định dạng yyyy-mm-dd
    return date_time.strftime('%Y-%m-%d')
# Flag to track if there are more trade to fetch
sleep_time = 0.5  # Adjust sleep time as needed (seconds)
transaction_count = 0  # Initialize trade count
distance_days = 7
# Lấy thời gian hiện tại
current_time = datetime.now()

# Chuyển đổi thời gian hiện tại sang milliseconds
current_time_ms = int(current_time.timestamp() * 1000)

# Khởi tạo giá trị cho 'to' và 'from' theo milliseconds
to_ts = current_time_ms
from_ts = current_time_ms - (distance_days * 24 * 60 * 60 * 1000)  # 90 ngày trước
start_ts = datetime(2023, 1, 1).timestamp() * 1000
has_more=True

while has_more:
    cursor = None  # Start from offset 0
    has_more_trade_id=True
    print(ms_to_date(from_ts), ms_to_date(to_ts))
    if from_ts < datetime(2017, 1, 1).timestamp() * 1000 and to_ts < datetime(2017, 1, 1).timestamp()*1000:
        has_more=False
        break
    if from_ts < datetime(2017, 1, 1).timestamp() * 1000 and to_ts > datetime(2017, 1, 1).timestamp()*1000:
        from_ts = datetime(2017, 1, 1).timestamp() * 1000
   
    while has_more_trade_id:
        try :
          
            data =session.get_pre_upgrade_trade_history(
                category="spot",
                cursor=cursor,
                startTime=from_ts,
                endTime=to_ts
            )
            print(data)
            # if not data or data['result'] != 'success' or not data['completed_orders']:
            #     print(data)
            #     has_more_trade_id = False
            #     to_ts = from_ts
            #     from_ts = from_ts - (distance_days * 24 * 60 * 60 * 1000)
            #     break
            for trade in data['result']['list']:
                transaction_count +=1
                print(f'Trade count {transaction_count}')
                with open('pre_trade.json', 'a') as f:
                    json.dump(trade, f, ensure_ascii=False)
                    f.write('\n')
                trade_data = (
                    1, # fake user id
                    trade["symbol"],
                    trade["orderId"],
                    trade["orderLinkId"],
                    trade["side"],
                    trade["orderPrice"],
                    trade["orderQty"],
                    trade["leavesQty"],
                    trade["orderType"],
                    trade["stopOrderType"],
                    trade["execFee"],
                    trade["execId"],
                    trade["execPrice"],
                    trade["execQty"],
                    trade["execType"],
                    trade["execValue"],
                    trade["execTime"],
                    trade["isMaker"],
                    trade["feeRate"],
                    trade["tradeIv"],
                    trade["markIv"],
                    trade["markPrice"],
                    trade["indexPrice"],
                    trade["underlyingPrice"],
                    trade["blockTradeId"]
                )
                # mycursor.execute(sql_insert_trade, trade_data)
                # mydb.commit()
            # get last Id from response to get next list of trade
            cursor = data['result']['nextPageCursor']
            print(cursor)
            if not cursor:
                has_more_trade_id = False
                to_ts = from_ts
                from_ts = from_ts - (distance_days * 24 * 60 * 60 * 1000)
                break
           
            # Sleep to respect API rate limit
            time.sleep(sleep_time)
        except Exception as x:
            has_more = False
            print(traceback.format_exc())