import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="trd",
    user="trd")

cur = conn.cursor()
cur.execute('drop table if exists eurusd_data')
cur.execute('''
    create table if not exists eurusd_data (
        year bigint,
        month bigint,
        day bigint,
        hour bigint,
        minute bigint,
        open real,
        high real,
        low real,
        close real,
        volume real
    )
''')

header = True
for s in open('/usr/proj/trd/eurusd.csv'):
    if header:
        header = False
        continue
    tokens = s.split(',')
    datetime_tokens = tokens[0].split(' ')
    date_tokens = datetime_tokens[0].split('.')
    day = int(date_tokens[0])
    month = int(date_tokens[1])
    year = int(date_tokens[2])
    time_tokens = datetime_tokens[1].split(':')
    hour = int(time_tokens[0])
    min = int(time_tokens[1])

    open = float(tokens[1])
    high = float(tokens[2])
    low = float(tokens[3])
    close = float(tokens[4])
    volume = float(tokens[5])

    cur.execute("""
        insert into eurusd_data(
            year,
            month,
            day,
            hour,
            minute,
            open,
            high,
            low,
            close,
            volume
        ) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (year, month, day, hour, min, open, high, low, close, volume))

conn.commit()
conn.close()
