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
sql = "SELECT * FROM upbit_deposits WHERE state ='ACCEPTED'"

try:
    # Execute the query
    mycursor.execute(sql)

    # Fetch all results
    rows = mycursor.fetchall()

    # Insert data into upbit_summary_transactions, applying logic
    for row in rows:
        id, user_id, type, uuid, currency, net_type, txid, state, created_at, done_at, amount, fee, transaction_type = row

        # Extract date from done_at timestamp
        if done_at:
            tx_time_formatted = date.fromisoformat(done_at.strftime('%Y-%m-%d'))  # Extract only date part
            #  convert to timestamp format
            tx_time = done_at.timestamp()
        else:
            tx_time_formatted = None  # Set to None if done_at is missing
        print(tx_time)
        print(net_type)
        # Construct values for the INSERT query
        values = (
            0,  # Fixed type for depost
            2,  # Fixed user_id
            None,  # uuid
            uuid,  # ref_id (using uuid for now, adjust if needed)
            None,  # order_id
            currency,  # in_token
            None,  # out_token
            currency,  # fee_token
            amount,  # in_amount
            fee,  # fee_amount
            txid,  # tx_hash
            net_type,  # network
            tx_time,  # tx_time
            tx_time_formatted,  # tx_time_formatted
            None,  # address
            None,  # address_tag
        )

        # Insert values into the table
        sql = "INSERT INTO upbit_summary_transactions (type, user_id, uuid, ref_id, order_id, in_token, out_token, fee_token, in_amount, fee_amount, tx_hash, network, tx_time, tx_time_formatted, address, address_tag) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
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
