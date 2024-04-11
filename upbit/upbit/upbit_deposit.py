import time
import traceback

import mysql.connector
import json


from upbit_deposit_list import get_all_deposit

# MySQL database connection details (replace with yours)
mydb = mysql.connector.connect(
    host="uat-taas-seoul.crc4caaeourk.ap-northeast-2.rds.amazonaws.com",
    port="3306",
    user="uat_taas",
    password="dev",
    database="hVs52Unvf99p933V"
)
mycursor = mydb.cursor()

sql_insert_deposit = """
INSERT INTO upbit_deposits_testing (user_id, type, uuid, currency, net_type, txid, state, created_at, done_at, 
amount, fee, transaction_type)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
sleep_time = 0.2  # Adjust sleep time as needed (seconds)
deposit_count = 0  # Initialize deposit count

try:
    # Get deposit with state
    states = ['PROCESSING', 'ACCEPTED', 'CANCELLED', 'REJECTED', 'TRAVEL_RULE_SUSPECTED', 'REFUNDING', 'REFUNDED']
    for state in states:
        print(state)
        page = 1  # Start from page 1
        # Flag to track if there are more deposit to fetch
        has_more = True
        while has_more:
            deposits = get_all_deposit(state=state, page=page)
            # Check if there are any deposit on this page
            if not deposits:
                has_more = False
                break
            for deposit in deposits:
                print(deposit)
                deposit_count +=1
                print(f'Deposit count {deposit_count}')
                with open('deposit_13.json', 'a') as f:
                    json.dump(deposit, f, ensure_ascii=False)
                    f.write('\n')
                deposit_data = (
                    13, # fake user id for user has more than 13K
                    deposit["type"],
                    deposit["uuid"],
                    deposit["currency"],
                    deposit["net_type"],
                    deposit["txid"],
                    deposit["state"],
                    deposit["created_at"],
                    deposit["done_at"],
                    deposit["amount"],
                    deposit["fee"],
                    deposit["transaction_type"]
                )
                print(sql_insert_deposit%deposit_data)
                mycursor.execute(sql_insert_deposit, deposit_data)
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
