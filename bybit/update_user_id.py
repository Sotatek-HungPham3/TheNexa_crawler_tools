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
user_id=3
id=2
mycursor = mydb.cursor()
sql = """
            UPDATE bybit_transactions
            SET user_id = %s,
            WHERE id = %s
        """
        values = (user_id, id)
        # Execute UPDATE query
        mycursor.execute(sql, values)
        mydb.commit()  # Commit changes to the database