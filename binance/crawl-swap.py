import time

from base_binance import *
import mysql.connector

mydb = mysql.connector.connect(
    host="127.0.0.1",
    port="3307",
    user="crawler",
    password="1",
    database="crawler"
)
mycursor = mydb.cursor()

crawler = CrawlBinance()

USER_ID = 100

START_TIME = 1498867200000
TIME_OFFSET = (24 * 60 * 60 * 1000) * 90

start_time = START_TIME
current_time = round(time.time() * 1000)

while start_time < current_time:
    end_time = start_time + TIME_OFFSET
    if end_time > current_time:
        end_time = current_time
    results = crawler.crawl_swap(start_time, end_time)
    for tx in results:
        values = (
            USER_ID,
            tx['swapId'],
            tx['swapTime'],
            tx['status'],
            tx['quoteAsset'],
            tx['baseAsset'],
            tx['quoteQty'],
            tx['price'],
            tx['fee']
        )
        # Insert values into the table
        sql = (
            "INSERT INTO binance_crawl_swap (user_id, swapId, swapTime, `status`, quoteAsset, baseAsset"
            ", quoteQty, price, fee)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
        mycursor.execute(sql, values)
        mydb.commit()
    start_time = end_time

