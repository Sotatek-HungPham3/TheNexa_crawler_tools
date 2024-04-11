from datetime import date, datetime
import mysql.connector
from binance.helpers import *
from datetime import datetime

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

print(f'[PROCESS GET EXCHANGE PAIRS] ====================== ')
# Get all pair
sql = "select pair, coin, currency from exchange_pairs where type = 'binance'"
# Execute the query
mycursor.execute(sql)
# Fetch all results
rows = mycursor.fetchall()
# Create an empty dictionary to store the results
symbol_map = {}
for row in rows:
    pair, coin, currency = row
    symbol_map[pair] = {
        'coin': coin,
        'currency': currency
    }

# print(f'[PROCESS MAP DEPOSIT] ====================== ')
# # Define the SQL query to read data DEPOSIT
# sqlDeposit = (
#     "SELECT amount, coin, network, status, address, addressTag, txId, insertTime, transferType FROM binance_crawl_deposit WHERE status = 1")
# # Execute the query
# mycursor.execute(sqlDeposit)
# # Fetch all results
# depositTxs = mycursor.fetchall()
# for row in depositTxs:
#     amount, coin, network, status, address, addressTag, txId, insertTime, transferType = row
#     timestampSecond = int(insertTime) / 1000
#     dobj = datetime.fromtimestamp(timestampSecond)
#     tx_time_formatted = dobj.date()
#     values = (
#         USER_ID,
#         0,
#         coin,
#         amount,
#         coin,
#         0,
#         insertTime,
#         tx_time_formatted,
#         txId,
#         network,
#         address,
#         addressTag
#     )
#     # Insert values into the table
#     sql = ("INSERT INTO binance_summary_transactions (user_id, `type`, in_token, in_amount, fee_token, "
#            "fee_amount, tx_time, tx_time_formatted, tx_hash, network, address, address_tag) "
#            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
#     mycursor.execute(sql, values)
#     mydb.commit()  # Commit changes to the database
#
# print(f'[PROCESS MAP WITHDRAW] ====================== ')
# # Define the SQL query to read data DEPOSIT
# sqlWithdraw = (
#     "SELECT amount, transactionFee, coin, status, address, txId, applyTime, network, transferType, completeTime FROM binance_crawl_withdraw WHERE status = 6")
# # Execute the query
# mycursor.execute(sqlWithdraw)
# # Fetch all results
# withdrawTxs = mycursor.fetchall()
# for row in withdrawTxs:
#     amount, transactionFee, coin, status, address, txId, applyTime, network, transferType, completeTime = row
#     dt = datetime.strftime(completeTime, "%Y-%m-%d")
#     values = (
#         USER_ID,
#         1,
#         coin,
#         amount,
#         coin,
#         transactionFee,
#         date_to_milliseconds(str(completeTime)),
#         dt,
#         txId,
#         network,
#         address,
#     )
#     # Insert values into the table
#     sql = ("INSERT INTO binance_summary_transactions (user_id, `type`, out_token, out_amount, fee_token, "
#            "fee_amount, tx_time, tx_time_formatted, tx_hash, network, address) "
#            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
#     mycursor.execute(sql, values)
#     mydb.commit()  # Commit changes to the database
#
# print(f'[PROCESS MAP TRADE] ====================== ')
# # Define the SQL query to read data DEPOSIT
# sqlTrade = (
#     "SELECT symbol, orderId, qty, quoteQty, commission, commissionAsset, `time`, isBuyer, isMaker, isBestMatch FROM binance_trade_transactions")
# # Execute the query
# mycursor.execute(sqlTrade)
# # Fetch all results
# tradeTxs = mycursor.fetchall()
# for row in tradeTxs:
#     symbol, orderId, qty, quoteQty, commission, commissionAsset, time, isBuyer, isMaker, isBestMatch = row
#     pair = symbol_map[symbol]
#     timestampSecond = int(time) / 1000
#     dobj = datetime.fromtimestamp(timestampSecond)
#     tx_time_formatted = dobj.date()
#     if bool(isBuyer) == True:
#         in_token = pair['coin']
#         in_amount = qty
#     else:
#         in_token = pair['currency']
#         in_amount = quoteQty
#     if bool(isBuyer) == True:
#         out_token = pair['currency']
#         out_amount = quoteQty
#     else:
#         out_token = pair['coin']
#         out_amount = qty
#     fee_token = commissionAsset
#     fee_amount = commission
#     if in_token == fee_token:
#         in_amount = float(in_amount) - float(fee_amount)
#     values = (
#         USER_ID,
#         3,
#         in_token,
#         out_token,
#         in_amount,
#         out_amount,
#         fee_token,
#         fee_amount,
#         time,
#         tx_time_formatted,
#         orderId,
#     )
#     # Insert values into the table
#     sql = ("INSERT INTO binance_summary_transactions (user_id, `type`, in_token, out_token, in_amount, out_amount, fee_token, "
#            "fee_amount, tx_time, tx_time_formatted, order_id) "
#            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
#     mycursor.execute(sql, values)
#     mydb.commit()  # Commit changes to the database

print(f'[PROCESS MAP SWAP] ====================== ')
# Define the SQL query to read data SWAP
sqlSwap = (
    "SELECT swapId, swapTime, status, baseQty, quoteQty, quoteAsset, baseAsset, price, fee FROM binance_crawl_swap WHERE status = 1")
# Execute the query
mycursor.execute(sqlSwap)
# Fetch all results
swapTxs = mycursor.fetchall()
for row in swapTxs:
    swapId, swapTime, status, baseQty, quoteQty, quoteAsset, baseAsset, price, fee = row
    timestampSecond = int(swapTime) / 1000
    dobj = datetime.fromtimestamp(timestampSecond)
    tx_time_formatted = dobj.date()
    #  convert to timestamp format
    tx_time = int(swapTime)
    values = (
        USER_ID,
        3,
        baseAsset,
        quoteAsset,
        baseQty,
        quoteQty,
        quoteAsset,
        fee,
        tx_time,
        tx_time_formatted
    )
    # Insert values into the table
    sql = (
        "INSERT INTO binance_summary_transactions (user_id, `type`, in_token, out_token, in_amount, out_amount, fee_token, "
        "fee_amount, tx_time, tx_time_formatted) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    mycursor.execute(sql, values)
    mydb.commit()  # Commit changes to the database