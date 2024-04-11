import pandas as pd
import pymysql

# Connect to MySQL database
connection = pymysql.connect(  host="127.0.0.1",
    port=3307,
    user="crawler",
    password="1",
    database="crawler")

# Query to select data from MySQL table
query = "SELECT * FROM coinone_summary_transactions where user_id=10 order by tx_time asc"

# Read data into pandas dataframe
df = pd.read_sql(query, connection)

# Close MySQL connection
connection.close()

# Export dataframe to Excel
df.to_excel('coinone_summary_transaction_sage.xlsx', index=False)