#!/usr/bin/python2.7

# Testing connection to DB
# Ditched sqlalchemy, this seemed simpler

import pymysql

conn = pymysql.connect(host='83.212.84.190', db="foursquare", user="foursquare", passwd="foursquare", unix_socket="/var/run/mysqld/mysqld.sock", port=3306)

cur = conn.cursor()
cur.execute("SELECT user FROM checkins LIMIT 2")

for row in cur:
    print(row)

cur.close()
conn.close()