# Scan_Data_Collection/db_config.py
# Same Azure MySQL as price_and_deals_update - no MongoDB
import mysql.connector
from mysql.connector import Error

DB_HOST = "rtngateway-mysqldbserver.mysql.database.azure.com"
DB_USER = "mysqldbuser"
DB_PASSWORD = "nezSyr-facfy9-RTNpropay"
DB_NAME = "live_rtn_stopnsave"


def get_mysql_connection():
    """Return a connection to Azure MySQL. Use this for all reads and writes; no MongoDB."""
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
        )
        return conn
    except Error as e:
        print(f"MySQL connection error: {e}")
        return None
