import mysql.connector
import traceback

# Connect to your MySQL database
mydb = mysql.connector.connect(
    host="127.0.0.1",
    port="3307",
    user="crawler",
    password="1",
    database="crawler"
)

mycursor = mydb.cursor()

try:
    # Read "symbol" and "ref_id" from the table
    sql = "SELECT symbol, ref_id FROM masterdata WHERE exchange ='okx'"
    mycursor.execute(sql)

    # Create an empty dictionary to store the results
    symbol_ref_id_map = {}

    # Iterate through the results and populate the dictionary
    for row in mycursor:
        symbol, ref_id = row
        symbol_ref_id_map[symbol] = ref_id

    # Read data from upbit_transactions
    sql = "SELECT id, in_token, out_token, fee_token FROM okx_summary_transactions"
    mycursor.execute(sql)
    rows = mycursor.fetchall()
    # Insert data into upbit_summary_transactions, applying logic
    for row in rows:
        id, in_token, out_token, fee_token = row
        # get token ref id on Map
        in_token_ref_id, out_token_ref_id, fee_token_ref_id = None, None, None
        if bool(in_token):
            if in_token in symbol_ref_id_map:
                in_token_ref_id = symbol_ref_id_map[in_token]
            else:
                in_token_ref_id = None
                print(in_token)
        if bool(out_token):
            if out_token in symbol_ref_id_map:
                out_token_ref_id = symbol_ref_id_map[out_token]
            else:
                out_token_ref_id = None
                print(out_token)
        # if bool(fee_token):
        #     fee_token_ref_id = symbol_ref_id_map[fee_token]
        # print(f'{in_token} has ref id is {in_token_ref_id}')
        # print(f'{out_token} has ref id is {out_token_ref_id}')
        # print(f'{fee_token} has ref id is {fee_token_ref_id}')
        sql = """
            UPDATE okx_summary_transactions
            SET in_token_ref_id = %s,
                out_token_ref_id = %s,
                fee_token_ref_id = %s
            WHERE id = %s
        """
        values = (in_token_ref_id, out_token_ref_id, fee_token_ref_id, id)
        # Execute UPDATE query
        mycursor.execute(sql, values)
        mydb.commit()  # Commit changes to the database

except mysql.connector.Error as err:
    print(traceback.format_exc())
    print("Error reading data:", err)

finally:
    # Close the cursor and connection
    mycursor.close()
    mydb.close()
