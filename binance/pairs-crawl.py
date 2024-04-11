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

# get ref id
sql = "SELECT symbol, ref_id FROM masterdata WHERE exchange ='binance'"
mycursor.execute(sql)

# Create an empty dictionary to store the results
symbol_ref_id_map = {}

# Iterate through the results and populate the dictionary
for row in mycursor:
    symbol, ref_id = row
    symbol_ref_id_map[symbol] = ref_id
# end get refid

data = crawler.crawl_pairs()
symbols = data['symbols']
for symbol in symbols:
    print(f'[Symbols]' + symbol['symbol'])
    if (symbol['baseAsset'] in symbol_ref_id_map):
        coin_ref_id = symbol_ref_id_map[symbol['baseAsset']]
    else:
        coin_ref_id = None
        print(f'[Coin]' + str(symbol['baseAsset']))
    if (symbol['quoteAsset'] in symbol_ref_id_map):
        currency_ref_id = symbol_ref_id_map[symbol['quoteAsset']]
    else:
        currency_ref_id = None
        print(f'[Currency]' + str(symbol['quoteAsset']))
    values = (
        'binance',
        symbol['baseAsset'],
        symbol['quoteAsset'],
        1,
        symbol['symbol'],
        coin_ref_id,
        currency_ref_id
    )
    # Insert values into the table
    sql = (
        "INSERT INTO exchange_pairs (type, coin, currency, is_active, pair, coin_ref_id, currency_ref_id)"
        " VALUES (%s, %s, %s, %s, %s, %s, %s)")
    mycursor.execute(sql, values)
    mydb.commit()
