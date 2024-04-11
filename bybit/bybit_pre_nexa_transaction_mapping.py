from datetime import date, datetime
from pybit.unified_trading import HTTP
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
session = HTTP(testnet=False)
mycursor = mydb.cursor()

# Define the SQL query to read data
sql = "SELECT * FROM bybit_pre_trade order by execTime asc"
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
        id,user_id, symbol, orderId,orderLinkId, side, orderPrice, orderQty, leavesQty, orderType, stopOrderType,execFee,execId,execPrice,execQty,execType,execValue,execTime,isMaker,feeRate,tradeIv,markIv,markPrice,indexPrice,underlyingPrice,blockTradeId =row

        # Extract date from created_at
        if execTime:
            tx_time_formatted = ms_to_date(execTime)  # Extract only date part
            #  convert to timestamp format
            tx_time = execTime
        else:
            tx_time_formatted = None  # Set to None if created_at is missing
            tx_time = None  # Set to None if created_at is missing
        # Define logic mapping
        symbolData = session.get_instruments_info(
            category="spot",
            symbol=symbol,
        )
        baseCoin = symbolData['result']['list'][0]['baseCoin']
        quoteCoin = symbolData['result']['list'][0]['quoteCoin']
        if not execValue:
            # inAmount= float(execPrice)*float(execQty) - float(execFee)
            inAmount= float(execPrice)*float(execQty) - float(execFee)
            outAmount= float(execPrice)*float(execQty)
        else:
            inAmount = float(execValue) - float(execFee)
            # inAmount = float(execValue)
            outAmount= float(execValue)
        if side == 'Sell': # sell
            in_token =  quoteCoin
            out_token = baseCoin
            fee_token = quoteCoin
            in_amount = inAmount
            out_amount = execQty
            fee_amount = execFee
        else: #  bid or buy
            in_token = baseCoin
            out_token = quoteCoin
            fee_token = baseCoin
            in_amount = float(execQty) - float(execFee)
            # in_amount = float(execQty)
            out_amount = outAmount
            fee_amount = execFee
        # Construct values for the INSERT query
        values = (
            2,  # Fixed type for trading
            2,  # Fixed user_id
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
        sql = "INSERT INTO bybit_transactions (type, user_id, uuid, ref_id, in_token, out_token, fee_token, in_amount, out_amount, fee_amount, tx_hash, network, tx_time, tx_time_formatted, address, address_tag) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
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
