import time
import traceback

import mysql.connector
import json


from upbit_withdrawal_list import get_all_withdrawal

# MySQL database connection details (replace with yours)
mydb = mysql.connector.connect(
    host="uat-taas-seoul.crc4caaeourk.ap-northeast-2.rds.amazonaws.com",
    port="3306",
    user="dev",
    password="hVs52Unvf99p933V",
    database="uat_taas"
)
mycursor = mydb.cursor()

sql_insert_withdraw = """
INSERT INTO upbit_withdraws_testing (user_id, type, uuid, currency, net_type, txid, state, created_at, done_at, 
amount, fee, transaction_type)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
sleep_time = 0.2  # Adjust sleep time as needed (seconds)
withdraw_count = 0  # Initialize order count

try:
    # Get withdrawal with state
    states = ['WAITING', 'PROCESSING', 'DONE', 'FAILED', 'CANCELLED', 'REJECTED']
    for state in states:
        print(state)
        page = 1  # Start from page 1
        # Flag to track if there are more withdrawal to fetch
        has_more = True
        while has_more:
            withdraws = get_all_withdrawal(state=state, page=page)
            # Check if there are any deposit on this page
            if not withdraws:
                has_more = False
                break
            for withdraw in withdraws:
                withdraw_count +=1
                print(f'Withdraw count {withdraw_count}')
                # with open('withdraw_13.json', 'a') as f:
                #     json.dump(withdraw, f, ensure_ascii=False)
                #     f.write('\n')
                withdraw_data = (
                    30, # fake user id for user has more than 13K
                    withdraw["type"],
                    withdraw["uuid"],
                    withdraw["currency"],
                    withdraw["net_type"],
                    withdraw["txid"],
                    withdraw["state"],
                    withdraw["created_at"],
                    withdraw["done_at"],
                    withdraw["amount"],
                    withdraw["fee"],
                    withdraw["transaction_type"]
                )
                print(sql_insert_withdraw%withdraw_data)
                mycursor.execute(sql_insert_withdraw, withdraw_data)
                mydb.commit()
            # Sleep to respect API rate limit
            time.sleep(sleep_time)
            # Increment page number for the next iteration
            page += 1
        print(f'End for state {state}')
except Exception as x:
    print(traceback.format_exc())
finally:
    # Close the cursor and connection
    mycursor.close()
    mydb.close()
