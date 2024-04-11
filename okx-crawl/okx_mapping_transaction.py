from datetime import date, datetime
import mysql.connector
# CONST
USER_ID = 1
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
    "SELECT user_id, in_token, out_token, in_amount, out_amount, fee, fee_currency, `timestamp`, type FROM okx_transactions")

try:
    # Execute the query
    mycursor.execute(sql)

    # Fetch all results
    rows = mycursor.fetchall()
    print(rows)
    # Insert data into upbit_summary_transactions, applying logic
    for row in rows:
        user_id, in_token, out_token, in_amount, out_amount, fee, fee_currency, timestamp, type = row
        if timestamp:
            timestampSecond = int(timestamp) / 1000
            dobj = datetime.fromtimestamp(timestampSecond)
            tx_time_formatted = dobj.date()
            #  convert to timestamp format
            tx_time = int(timestamp)
        else:
            tx_time_formatted = None  # Set to None if created_at is missing
            tx_time = None  # Set to None if created_at is missing

        typeInDb = 2 # default trade
        if type == 'DEPOSIT':
            typeInDb = 0
        elif type == 'WITHDRAW':
            typeInDb = 1
        elif type == 'SWAP':
            typeInDb = 3

        in_amount = in_amount if in_token != fee_currency else float(in_amount) - float(fee)

        values = (
            USER_ID,
            in_token,
            out_token,
            in_amount,
            out_amount,
            fee,
            fee_currency,
            typeInDb,
            tx_time,
            tx_time_formatted
        )

        # Insert values into the table
        sql = "INSERT INTO okx_summary_transactions (user_id, in_token, out_token, in_amount, out_amount, fee_amount, fee_token, type, tx_time, tx_time_formatted) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        mycursor.execute(sql, values)
        mydb.commit()  # Commit changes to the database


except mysql.connector.Error as err:
    print("Error reading data:", err)

finally:
    # Close the cursor and connection
    mycursor.close()
    mydb.close()

print("Data mapping complete.")
