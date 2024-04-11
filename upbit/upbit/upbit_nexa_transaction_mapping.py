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
sql = "SELECT * FROM upbit_transactions"

try:
    # Execute the query
    mycursor.execute(sql)

    # Fetch all results
    rows = mycursor.fetchall()

    # Insert data into upbit_summary_transactions, applying logic
    for row in rows:
        transaction_id, uuid, user_id, order_uuid, market, price, volume, fee, funds, created_at, side = row

        # Extract date from created_at
        if created_at:
            tx_time_formatted = date.fromisoformat(created_at.strftime('%Y-%m-%d'))  # Extract only date part
            #  convert to timestamp format
            tx_time = created_at.timestamp()
        else:
            tx_time_formatted = None  # Set to None if created_at is missing
            tx_time = None  # Set to None if created_at is missing
        # Define logic mapping
        symbols = market.split('-')
        if side == 'ask' : # sell
            in_token = symbols[0]
            out_token = symbols[1]
            fee_token = symbols[0]
            in_amount = funds
            out_amount = volume
            fee_amount = fee
        else: #  bid or buy
            in_token = symbols[1]
            out_token = symbols[0]
            fee_token = symbols[0]
            in_amount = volume
            out_amount = funds
            fee_amount = fee
        # Construct values for the INSERT query
        values = (
            2,  # Fixed type for trading
            2,  # Fixed user_id
            None,  # uuid
            uuid,  # ref_id (using uuid for now, adjust if needed)
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
        sql = "INSERT INTO upbit_summary_transactions (type, user_id, uuid, ref_id, in_token, out_token, fee_token, in_amount, out_amount, fee_amount, tx_hash, network, tx_time, tx_time_formatted, address, address_tag) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
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
