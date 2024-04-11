from datetime import date, datetime

import mysql.connector
import traceback

# Connect to your MySQL database
mydb = mysql.connector.connect(
    host="127.0.0.1",
    port="3307",
    user="crawler",
    password="1",
    database="crawler"
)

mycursor = mydb.cursor()

# Define the SQL query to read data
sql = "SELECT * FROM coinone_trades"
def ms_to_date(ms):
    # Chuyển đổi milliseconds sang đối tượng datetime
    date_time = datetime.fromtimestamp(ms / 1000)
    # Trả về chuỗi có định dạng yyyy-mm-dd
    return date_time.strftime('%Y-%m-%d')
try:
    # Execute the query
    mycursor.execute(sql)

    # Fetch all results
    rows = mycursor.fetchall()

    # Insert data into upbit_summary_transactions, applying logic
    for row in rows:
        id,trade_id, user_id, order_id, quote_currency, target_currency,order_type, is_ask, is_maker,price, qty, timestamp,fee_rate,fee,fee_currency = row

        # Extract date from created_at
        if timestamp:
            tx_time_formatted = ms_to_date(timestamp)  # Extract only date part
            #  convert to timestamp format
            tx_time = timestamp
        else:
            tx_time_formatted = None  # Set to None if created_at is missing
            tx_time = None  # Set to None if created_at is missing
        # Define logic mapping
       
        if is_ask: # sell
            in_token = quote_currency
            out_token = target_currency
            fee_token = fee_currency
            in_amount = float(qty)*float(price)
            out_amount = qty
            fee_amount = fee
        else: #  bid or buy
            in_token = target_currency
            out_token = quote_currency
            fee_token = fee_currency
            in_amount = qty
            out_amount = float(qty)*float(price)
            fee_amount = fee
        # Construct values for the INSERT query
        values = (
            2,  # Fixed type for trading
            10,  # Fixed user_id
            None,  # uuid
            id,  # ref_id (using uuid for now, adjust if needed)
            # order_uuid,  # order_id
            in_token,  # in_token
            out_token,  # out_token
            fee_token,  # fee_token
            in_amount,  # in_amount
            out_amount,  # out_amount
            fee_amount,  # fee_amount
            None,  # tx_hash
            None,  # network
            tx_time,  # tx_time
            tx_time_formatted,  # tx_time_formatted
            None,  # address
            None,  # address_tag
        )

        # Insert values into the table
        sql = "INSERT INTO coinone_summary_transactions (type, user_id, uuid, ref_id, in_token, out_token, fee_token, in_amount, out_amount, fee_amount, tx_hash, network, tx_time, tx_time_formatted, address, address_tag) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        mycursor.execute(sql, values)
        mydb.commit()  # Commit changes to the database

except mysql.connector.Error as err:
    print(traceback.format_exc())
    print("Error reading data:", err)

finally:
    # Close the cursor and connection
    mycursor.close()
    mydb.close()

print("Data mapping complete.")
