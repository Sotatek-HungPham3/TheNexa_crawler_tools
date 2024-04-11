from base_binance import *
import time
import mysql.connector

mydb = mysql.connector.connect(
    host="127.0.0.1",
    port="3307",
    user="crawler",
    password="1",
    database="crawler"
)
mycursor = mydb.cursor()

# Const
USER_ID = 100
START_TIME = 1498867200000
TIME_OFFSET = (24 * 60 * 60 * 1000) * 90
# End Const

# Init crawler
crawler = CrawlBinance()

start_time = START_TIME
current_time = round(time.time() * 1000)

while start_time < current_time:
    end_time = start_time + TIME_OFFSET
    if end_time > current_time:
        end_time = current_time

    results = crawler.crawl_get_withdraw_history(start_time=start_time, end_time=end_time)
    for tx in results:
        values = (
            USER_ID,
            tx['id'],
            tx['amount'],
            tx['transactionFee'],
            tx['coin'],
            tx['status'],
            tx['address'],
            tx['txId'] if 'txId' in tx else None,
            tx['applyTime'],
            tx['network'] if 'network' in tx else None,
            tx['transferType'] if 'transferType' in tx else None,
            tx['withdrawOrderId'] if 'withdrawOrderId' in tx else None,
            tx['info'] if 'info' in tx else None,
            tx['confirmNo'] if 'confirmNo' in tx else None,
            tx['walletType'] if 'walletType' in tx else None,
            tx['txKey'] if 'txKey' in tx else None,
            tx['completeTime'] if 'completeTime' in tx else None,
        )
        # Insert values into the table
        sql = (
            "INSERT INTO binance_crawl_withdraw (user_id, withdrawId, amount, transactionFee, coin, `status`, "
            "address, txId, applyTime, network, transferType, withdrawOrderId, info, confirmNo, walletType, txKey, completeTime)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        mycursor.execute(sql, values)
        mydb.commit()

    start_time = end_time
