import mysql.connector
DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "pycharmuser"
DB_PASSWORD = "12345"
DB_NAME = "headhunter"
connection = mysql.connector.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME
)
cursor = connection.cursor(buffered=True)
