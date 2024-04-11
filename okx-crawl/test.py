import datetime
import json

from base_crawl_okx import *

START_TIME = "1498867200000" # 01-07-2017 milisecond
END_TIME = str(round(datetime.datetime.now().timestamp() * 1000)) # current time

crawler = CrawlOkx()
tradeTxs = crawler.crawl_trade("SPOT")

for tx in tradeTxs:
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
    values = (
        1,
        in_token,
        out_token,
        in_amount,
        out_amount,
        tx['fee'],
        tx['feeCcy'],
        tx['ts'],
        1,
        "TRADE"
    )
    # Insert values into the table
    sql = (
        "INSERT INTO okx_transactions (user_id, in_token, out_token, in_amount, out_amount, fee, fee_currency,`timestamp`, state, type )"
        " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
