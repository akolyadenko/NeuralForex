import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="trd",
    user="trd")

cur = conn.cursor()
cur.execute('select ARRAY[1, 2, 3]::bigint[]')

for row in cur.fetchall():
    print(row[0])

