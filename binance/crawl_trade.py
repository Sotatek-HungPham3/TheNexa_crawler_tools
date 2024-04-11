import json

from base_binance import *
import mysql.connector
import time

mydb = mysql.connector.connect(
    host="127.0.0.1",
    port="3307",
    user="crawler",
    password="1",
    database="crawler"
)
mycursor = mydb.cursor()

USER_ID = 1000

# Init crawler
crawler = CrawlBinance()

# Get all pair
sql = "select pair from exchange_pairs where type = 'binance'"
# Execute the query
mycursor.execute(sql)
# Fetch all results
rows = mycursor.fetchall()

for row in rows:
    pair = row
    print(f'[PROCESS]: ' + str(pair[0]))
    last_id = 1
    while True:
        results = crawler.crawl_trades(symbol=pair[0], limit=1000, last_id=last_id)
        print(f'[LENGTH DATA:]', str(len(results)))
        if len(results) == 0:
            break
        for tx in results:
            values = (
                USER_ID,
                tx['symbol'],
                tx['id'],
                tx['orderId'],
                tx['orderListId'],
                tx['price'],
                tx['qty'],
                tx['quoteQty'],
                tx['commission'],
                tx['commissionAsset'],
                tx['time'],
                tx['isBuyer'],
                tx['isMaker'],
                tx['isBestMatch'],
            )
            # Insert values into the table
            sql = (
                "INSERT INTO binance_trade_transactions (user_id, symbol, tradeId, orderId, orderListId, `price`, qty, quoteQty, commission, commissionAsset,"
                " `time`, isBuyer, isMaker, isBestMatch)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
            mycursor.execute(sql, values)
            mydb.commit()
            last_id = int(tx['id']) + 1


