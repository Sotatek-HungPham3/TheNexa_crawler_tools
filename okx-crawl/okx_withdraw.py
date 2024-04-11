import json
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
LIMIT = "100"
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
    depositTxs = crawler.crawl_withdraw(start_time=start_time, end_time=end_time, limit=LIMIT)
    print(json.dumps(depositTxs))
    if len(depositTxs) == 0:
        break
    for tx in depositTxs:
        values = (
            USER_ID,
            tx['ccy'],
            tx['chain'],
            tx['nonTradableAsset'],
            tx['amt'],
            tx['ts'],
            tx['from'],
            tx['areaCodeFrom'],
            tx['to'],
            tx['areaCodeTo'],
            tx['tag'] if 'tag' in tx else None,
            tx['pmtId'] if 'pmtId' in tx else None,
            tx['memo'] if 'memo' in tx else None,
            tx['addrEx'] if 'addrEx' in tx else None,
            tx['txId'],
            tx['fee'],
            tx['feeCcy'],
            tx['state'],
            tx['wdId'],
            tx['clientId'],
        )
        # Insert values into the table
        sql = (
            "INSERT INTO okx_withdraw_transactions (user_id, currency, `chain`, nonTradableAsset, amount, ts, `from`"
            ", areaCodeFrom, `to`, areaCodeTo, tag, pmtId, memo, addrEx, txId, fee, feeCcy, state, wdId, clientId)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        mycursor.execute(sql, values)
        mydb.commit()
        end_time = int(tx['ts'])
