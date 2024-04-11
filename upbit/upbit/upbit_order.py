import time
import traceback

import mysql.connector
import pyupbit
import json

from upbit_order_list import get_all_order

access_key = "nHPmoSxSQuyxAvnkx7x3fgWB3VQJcQ0Rz31pEiZx"
secret_key = "U7ihxL0s4JtuIM4eYGfRQ1Yvrm1XQcL9yUnix2CP"
upbit = pyupbit.Upbit(access_key, secret_key)

# MySQL database connection details (replace with yours)
mydb = mysql.connector.connect(
    host="127.0.0.1",
    port="3307",
    user="crawler",
    password="1",
    database="crawler"
)
mycursor = mydb.cursor()

sql_insert_order = """
INSERT INTO upbit_orders_testing (uuid, user_id, side, ord_type, price, state, market, created_at, volume, 
remaining_volume, reserved_fee, remaining_fee, paid_fee, locked, executed_volume, trades_count, is_has_trade)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

sql_insert_transaction = """
INSERT INTO upbit_transactions_testing (order_uuid, user_id, uuid, market, price, volume, fee, funds, created_at, side)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


# Flag to track if there are more orders to fetch
has_more = True
page = 1  # Start from page 1
sleep_time = 0.1  # Adjust sleep time as needed (seconds)
order_count = 0  # Initialize order count
while has_more:
    try:
        # Get orders for the current page
        # state=["done", "cancel"]
        # state=["wait", "watch"]
        orders = get_all_order(state=["done", "cancel"], page=page)
        # Check if there are any orders on this page
        if not orders:
            has_more = False
            break

        # Sleep to respect API rate limit
        # time.sleep(sleep_time)

        # Process the orders (get order detail then store in database)
        for order in orders:
            order_count +=1
            print(f'Order count {order_count}')
            # get order detail
            order_detail = upbit.get_order(order['uuid'])
            with open('order_list.json', 'a') as f:
                json.dump(order_detail, f, ensure_ascii=False)
                f.write('\n')
            is_has_trade = True
            if not order_detail['trades']:
                is_has_trade = False
            order_data = (
                order_detail["uuid"],
                13, # fake user id for user has more than 13K
                order_detail["side"],
                order_detail["ord_type"],
                order_detail["price"] if order_detail.get("price") is not None else None,
                order_detail["state"],
                order_detail["market"],
                order_detail["created_at"],
                order_detail["volume"] if order_detail.get("volume") is not None else None,
                order_detail["remaining_volume"] if order_detail.get("remaining_volume") is not None else None,
                order_detail["reserved_fee"] if order_detail.get("reserved_fee") is not None else None,
                order_detail["remaining_fee"] if order_detail.get("remaining_fee") is not None else None,
                order_detail["paid_fee"],
                order_detail["locked"],
                order_detail["executed_volume"],
                order_detail["trades_count"],
                is_has_trade
            )
            mycursor.execute(sql_insert_order, order_data)
            mydb.commit()

            # If has order has trades then save each trade
            if order_detail['trades']:
                # Insert individual trades as transactions
                for trade in order_detail['trades']:  # Handle potential missing "trades" list
                    trade_fee = float(trade["volume"]) * float(order_detail["paid_fee"]) / float(order_detail["executed_volume"])
                    transaction_data = (
                        order_detail["uuid"],  # Link to parent order
                        13,   # fake user id for user has more than 13K
                        trade["uuid"],
                        trade["market"],
                        trade["price"] if trade.get("price") is not None else None,
                        trade["volume"],
                        trade_fee,
                        trade["funds"],
                        trade["created_at"],
                        order_detail["side"]# Determine transaction type (buy or sell)
                    )
                    mycursor.execute(sql_insert_transaction, transaction_data)
                    mydb.commit()
            elif order_detail['trades_count'] != 0:
                transaction_data = (
                    order_detail["uuid"],  # Link to parent order
                    2,   # fake user id for user has more than 13K
                    order_detail["uuid"],
                    order_detail["market"],
                    order_detail["price"] if order_detail.get("price") is not None else None,
                    order_detail["executed_volume"],
                    order_detail["paid_fee"],
                    float(order_detail["executed_volume"]) * float(order_detail["price"]),
                    order_detail["created_at"],
                    order_detail["side"]# Determine transaction type (buy or sell)
                )
                mycursor.execute(sql_insert_transaction, transaction_data)
                mydb.commit()
            # Sleep to respect API rate limit
            time.sleep(sleep_time)
    except Exception as x:
        print(traceback.format_exc())
        has_more = False
        mycursor.close()
        mydb.close()
    # finally:
    #     # Close the cursor and connection
    #     mycursor.close()
    #     mydb.close()
    # Increment page number for the next iteration
    page += 1
mycursor.close()
mydb.close()



