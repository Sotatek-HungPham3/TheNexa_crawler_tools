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
USER_ID = 1
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

start_time_deposit = round(end_date_time.timestamp() * 1000)
end_time_deposit = round(current_date.timestamp() * 1000)
print(f'[START TIME DEPOSIT] ', start_time_deposit)
print(f'[END TIME DEPOSIT] ', end_time_deposit)

# PROCESS DEPOSIT
while True:
    depositTxs = crawler.crawl_deposit(start_time=start_time_deposit, end_time=end_time_deposit, limit=LIMIT)
    if len(depositTxs) == 0:
        break
    for tx in depositTxs:
        convert_time = datetime.fromtimestamp(int(tx['ts']) / 1000).strftime("%Y-%m-%d %H:%M:%S")
        values = (
            USER_ID,
            tx['ccy'],
            tx['amt'],
            tx['from'],
            tx['to'],
            tx['txId'],
            tx['ts'],
            convert_time,
            tx['state'],
            "DEPOSIT"
        )

        # Insert values into the table
        sql = ("INSERT INTO okx_transactions (user_id, in_token, in_amount, `from`, `to`, tx_id, `timestamp`, convert_time, state, type)"
               " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        mycursor.execute(sql, values)
        mydb.commit()
        end_time_deposit = int(tx['ts'])


start_time_withdraw = round(end_date_time.timestamp() * 1000)
end_time_withdraw = round(current_date.timestamp() * 1000)
print(f'[START TIME WITHDRAW] ', start_time_withdraw)
print(f'[END TIME WITHDRAW] ', end_time_withdraw)

# PROCESS WITHDRAW
while True:
    withdrawTxs = crawler.crawl_withdraw(start_time=start_time_withdraw, end_time=end_time_withdraw, limit=LIMIT)
    if len(withdrawTxs) == 0:
        break
    for tx in withdrawTxs:
        convert_time = datetime.fromtimestamp(int(tx['ts']) / 1000).strftime("%Y-%m-%d %H:%M:%S")
        values = (
            USER_ID,
            tx['ccy'],
            tx['amt'],
            tx['fee'],
            tx['feeCcy'],
            tx['from'],
            tx['to'],
            tx['txId'],
            tx['ts'],
            convert_time,
            tx['state'],
            "WITHDRAW"
        )
        # Insert values into the table
        sql = (
            "INSERT INTO okx_transactions (user_id, out_token, out_amount, fee, fee_currency, `from`, `to`, tx_id, `timestamp`, convert_time, state, type )"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        mycursor.execute(sql, values)
        mydb.commit()
        end_time_withdraw = int(tx['ts'])


start_time_swap = round(end_date_time.timestamp() * 1000)
end_time_swap = round(current_date.timestamp() * 1000)
print(f'[START TIME SWAP] ', start_time_swap)
print(f'[END TIME SWAP] ', end_time_swap)

# PROCESS SWAP
while True:
    swapTxs = crawler.crawl_swap(start_time=start_time_swap, end_time=end_time_swap, limit=LIMIT)
    if len(swapTxs) == 0:
        break
    for tx in swapTxs:
        convert_time = datetime.fromtimestamp(int(tx['ts']) / 1000).strftime("%Y-%m-%d %H:%M:%S")
        if tx['side'] == 'sell':
            in_token = tx['quoteCcy']
            out_token = tx['baseCcy']
            in_amount = tx['fillQuoteSz']
            out_amount = tx['fillBaseSz']
        else:
            in_token = tx['baseCcy']
            out_token = tx['quoteCcy']
            in_amount = tx['fillBaseSz']
            out_amount = tx['fillQuoteSz']
        if tx['state'] == 'fullyFilled':
            state = 1  # success
        else:
            state = 0  # fail
        values = (
            USER_ID,
            in_token,
            out_token,
            in_amount,
            out_amount,
            tx['ts'],
            convert_time,
            state,
            "SWAP"
        )

        # Insert values into the table
        sql = (
            "INSERT INTO okx_transactions (user_id, in_token, out_token, in_amount, out_amount, `timestamp`, convert_time, state, type )"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
        mycursor.execute(sql, values)
        mydb.commit()
        end_time_swap = int(tx['ts'])


start_time_trade = round(end_date_time.timestamp() * 1000)
end_time_trade = round(current_date.timestamp() * 1000)
print(f'[START TIME] ', start_time_trade)
print(f'[END TIME] ', end_time_trade)
billId = 0

# PROCESS TRADE
while True:
    tradeTxs = crawler.crawl_trade("SPOT", billId, limit=LIMIT, begin=start_time_trade, end=end_time_trade)
    if len(tradeTxs) == 0:
        break
    for tx in tradeTxs:
        convert_time = datetime.fromtimestamp(int(tx['ts']) / 1000).strftime("%Y-%m-%d %H:%M:%S")
        pairs = str(tx['instId']).split('-')
        if tx['side'] == 'buy':
            in_token = pairs[0]
            out_token = pairs[1]
            in_amount = tx['fillSz']
            out_amount = float(tx['fillSz']) * float(tx['fillPx'])
        else:
            in_token = pairs[1]
            out_token = pairs[0]
            in_amount = float(tx['fillSz']) * float(tx['fillPx'])
            out_amount = tx['fillSz']
        fee = 0 - float(tx['fee'])
        values = (
            1,
            in_token,
            out_token,
            in_amount,
            out_amount,
            fee,
            tx['feeCcy'],
            tx['ts'],
            convert_time,
            1,
            "TRADE"
        )
        # Insert values into the table
        sql = (
            "INSERT INTO okx_transactions (user_id, in_token, out_token, in_amount, out_amount, fee, fee_currency,`timestamp`, convert_time, state, type )"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        mycursor.execute(sql, values)
        mydb.commit()
    billId = int(tradeTxs[0]['billId'])
