from datetime import date, datetime

import mysql.connector
import traceback

# CONST
USER_ID = 50
# END CONST

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
sql = (
    "SELECT user_id, search, transfer_date, order_currency, payment_currency, units, price, amount, fee_currency, fee, "
    "order_balance, payment_balance FROM bithumb_transactions where user_id = 50")

try:
    # Execute the query
    mycursor.execute(sql)

    # Fetch all results
    rows = mycursor.fetchall()

    # Insert data into upbit_summary_transactions, applying logic
    for row in rows:
        user_id, search, transfer_date, order_currency, payment_currency, units, price, amount, fee_currency, fee, order_balance, payment_balance = row
        if order_currency == 'P' or payment_currency == 'P':
            # skip to reward record
            continue
        if search == 3 or search == 9:
            # skip withdrawing or depositing KRW
            continue
        # format and convert units and fee
        f_fee = float(fee.replace(',',''))
        f_units = float(units.replace('+','').replace('-',''))
        f_price = float(abs(price))
        if transfer_date:
            timestampSecond = transfer_date / 1000000
            dobj = datetime.fromtimestamp(timestampSecond)
            tx_time_formatted = dobj.date()
            tx_time = transfer_date / 1000
        else:
            tx_time_formatted = None  # Set to None if created_at is missing
            tx_time = None  # Set to None if created_at is missing
        if search == 5: # withdraw
            out_token = order_currency
            fee_token = fee_currency
            fee_amount = f_fee
            in_token = ''
            in_amount = 0
            if order_currency == 'KRW':
                out_amount = f_price - f_fee
            else:
                out_amount = f_units - f_fee
        elif search == 2: # sell
            in_token = payment_currency
            out_token = order_currency
            fee_token = fee_currency
            out_amount = f_units
            in_amount = f_units * f_price
            fee_amount = f_fee
        elif search == 4: # deposit
            in_token = order_currency
            fee_token = fee_currency
            fee_amount = f_fee
            out_token = ''
            out_amount = 0
            if order_currency == 'KRW':
                in_amount = f_price
            else:
                in_amount = f_units
        elif search == 1: # buy
            in_token = order_currency
            out_token = payment_currency
            fee_token = fee_currency
            in_amount = f_units
            out_amount = f_units * f_price
            fee_amount = f_fee

        # deposit
        if search == 4:
            type = 0
        # withdraw
        elif search == 5:
            type = 1
        # trade
        elif search == 1 or search == 2:
            type = 2

        values = (
            type,  # Fixed type for trading
            USER_ID,  # Fixed user_id
            0,  # uuid
            None,  # ref_id (using uuid for now, adjust if needed)
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
        sql = "INSERT INTO bithumb_summary_transactions (type, user_id, uuid, ref_id, in_token, out_token, fee_token, in_amount, out_amount, fee_amount, tx_hash, network, tx_time, tx_time_formatted, address, address_tag) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
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
