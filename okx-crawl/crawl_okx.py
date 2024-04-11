import json
from datetime import date, datetime
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
LIMIT = "100"
# END CONST
billId = 0
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
    tradeTxs = crawler.crawl_trade("SPOT", billId, limit=LIMIT, begin=start_time, end=end_time)
    print(json.dumps(tradeTxs))
    if len(tradeTxs) == 0:
        break
    for tx in tradeTxs:
        convert_time = datetime.fromtimestamp(int(tx['ts']) / 1000).strftime("%Y-%m-%d %H:%M:%S")
        values = (
            USER_ID,
            tx['instType'],
            tx['tradeId'],
            tx['ordId'],
            tx['clOrdId'],
            tx['billId'],
            tx['tag'],
            tx['fillPx'],
            tx['fillSz'],
            tx['txId'] if 'txId' in tx else None,
            tx['fillIdxPx'],
            tx['fillPnl'],
            tx['fillPxVol'],
            tx['fillPxUsd'],
            tx['fillMarkVol'],
            tx['fillFwdPx'],
            tx['fillMarkPx'],
            tx['side'],
            tx['posSide'],
            tx['execType'],
            tx['feeCcy'],
            abs(float(tx['fee'])),
            tx['ts'],
            convert_time,
            tx['fillTime'],
        )

        # Insert values into the table
        sql = (
            "INSERT INTO okx_trade_transactions (user_id, instType, `tradeId`, ordId, `clOrdId`, `billId`, tag"
            ", fillPx, `fillSz`, `txId`, fillIdxPx, fillPnl, fillPxVol, fillPxUsd, fillMarkVol, fillFwdPx, fillMarkPx, `side`, "
            "posSide, execType, feeCcy, fee, ts, convert_time, fillTime)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        mycursor.execute(sql, values)
        mydb.commit()
    billId = int(tradeTxs[0]['billId'])
