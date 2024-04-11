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
USER_ID = 1000
# START_TIME = 1498867200000
START_TIME = 1712624400000
TIME_OFFSET = (24 * 60 * 60 * 1000) * 90
# End Const

# Init crawler
crawler = CrawlBinance()
limit = 1000
start_time = START_TIME
current_time = round(time.time() * 1000)

while start_time < current_time:
    end_time = start_time + TIME_OFFSET
    if end_time > current_time:
        end_time = current_time
    print(f'[Start time]: ' + str(start_time))
    print(f'[End time]: ' + str(end_time))
    offset = 0
    while True:
        results = crawler.crawl_get_deposit_history_v2(offset=offset, limit=limit, start_time=start_time,end_time=end_time)
        if len(results) == 0:
            break
        for tx in results:
            values = (
                USER_ID,
                tx['id'],
                tx['amount'],
                tx['coin'],
                tx['network'],
                tx['status'],
                tx['address'],
                tx['addressTag'],
                tx['txId'],
                tx['insertTime'],
                tx['transferType'] if 'transferType' in tx else None,
                tx['confirmTimes'] if 'confirmTimes' in tx else None,
                tx['unlockConfirm'] if 'unlockConfirm' in tx else None,
                tx['walletType'] if 'walletType' in tx else None,
            )
            # Insert values into the table
            sql = (
                "INSERT INTO binance_crawl_deposit (user_id, deposit_id, amount, coin, network, `status`, address, addressTag, txId, insertTime, transferType, confirmTimes, unlockConfirm, walletType)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
            mycursor.execute(sql, values)
            mydb.commit()
        offset = offset + limit
    start_time = end_time
