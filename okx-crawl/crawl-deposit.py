from datetime import date, datetime, timedelta
from base_crawl_okx import *
import mysql.connector
from dateutil.relativedelta import relativedelta

mydb = mysql.connector.connect(
    host="127.0.0.1",
    port="3307",
    user="crawler",
    password="1",
    database="crawler"
)
mycursor = mydb.cursor()

# CONST
USER_ID = 1001
START_TIME = "1704412800000"  # 01-07-2017 milisecond
END_TIME = str(round(datetime.now().timestamp() * 1000))  # current time
LIMIT = "2"
# END CONST

# INIT CRAWLER
crawler = CrawlOkx()
# END INIT

current_date = datetime.now()
end_date_time = current_date - relativedelta(months=3)

# print(round(current_date.timestamp() * 1000))
# print(round(end_date_time.timestamp() * 1000))

start_time = round(end_date_time.timestamp() * 1000)
end_time = round(current_date.timestamp() * 1000)
print(f'[START TIME] ', start_time)
print(f'[END TIME] ', end_time)

while True:
    depositTxs = crawler.crawl_deposit(start_time=start_time, end_time=end_time, limit=LIMIT)
    if len(depositTxs) == 0:
        break
    for tx in depositTxs:
        values = (
            USER_ID,
            tx['ccy'],
            tx['chain'],
            tx['amt'],
            tx['from'],
            tx['to'],
            tx['areaCodeFrom'],
            tx['txId'],
            tx['ts'],
            tx['state'],
            tx['depId'],
            tx['fromWdId'],
            tx['actualDepBlkConfirm'],
        )
        # Insert values into the table
        sql = (
            "INSERT INTO okx_deposit_transactions (user_id, currency, `chain`, amount, `from`, `to`, areaCodeFrom"
            ", txId, `timestamp`, `state`, depositId, fromWdId, actualDepBlkConfirm)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        mycursor.execute(sql, values)
        mydb.commit()
        end_time = int(tx['ts'])


