#!/usr/bin/env python

# imports
import MySQLdb

# DB Constants.
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_DABA = 'mailer'
DB_TABL = 'mailing_list'

# Samples Count
REQ_NUM_SAMPLES = 50

# Server Connection to MySQL
DB_SERVER = MySQLdb.connect(host=DB_HOST,
           user=DB_USER,
           passwd=DB_PASS,
           db=DB_DABA)
DB_CONN = DB_SERVER.cursor()

# Building Query.
SQL_QUERY = 'INSERT INTO ' + DB_TABL + \
' (`name`, `mail`, `status`) VALUES '

# For entire data set
for i in range(REQ_NUM_SAMPLES):
    SQL_QUERY += "('name" + str(i) + "', 'mail" + str(i) + \
    "@test.com', '0' )"
    if i != REQ_NUM_SAMPLES - 1:
        SQL_QUERY += ', '

#print command
try:
    DB_CONN.execute(SQL_QUERY)
    DB_SERVER.commit()
except Exception as error:
    print (error)
    DB_SERVER.rollback()

DB_SERVER.close()
