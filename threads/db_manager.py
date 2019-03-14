#!/usr/bin/env python

import MySQLdb

# DB Constants
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_DABA = 'mailer'
DB_TABL = 'mailing_list'


def retrieve_rows():
    # Server Connection to MySQL for Thread.
    db_server = MySQLdb.connect(host=DB_HOST,
                                user=DB_USER,
                                passwd=DB_PASS,
                                db=DB_DABA)
    db_conn = db_server.cursor()


    db_conn.execute(""" SELECT * FROM %s
                        WHERE status=0 """ % DB_TABL)
    rows = db_conn.fetchall()
    return rows

def retrieve_single_row(db_conn):
    db_conn.execute(""" SELECT * FROM %s
                        WHERE status=0
                        LIMIT 1 FOR UPDATE""" % DB_TABL)
    row = db_conn.fetchone()
    return row

def check_row(db_conn, db_server, row_id, thread_name):
    db_conn.execute(""" UPDATE %s 
                        SET status=1,
                        sent_time=NOW(),
                        thread='%s'
                        WHERE id=%d""" % (DB_TABL, thread_name, row_id))
    db_server.commit()

def close_connection(db_server):
    db_server.close()

def get_output_from_db(db_conn, db_server, query):
    '''Returns output from database as list'''
    db_server = MySQLdb.connect(host=DB_HOST,
                                user=DB_USER,
                                passwd=DB_PASS,
                                db=DB_DABA)
    db_conn = db_server.cursor()
    db_conn.execute(query)
    result = db_conn.fetchall()
    db_conn.close()
    return result

def print_results(result):
    '''Prints results to console'''
    for row in result:
        print (row[0], ' : ', row[1], 'Records Processed')

def print_time_performance(db_conn, db_server):
    print('===== Time Performance =====')
    print_results(get_output_from_db(db_conn, db_server, """SELECT sent_time, COUNT( * ) 
                                        FROM  `mailing_list` GROUP BY sent_time"""))
def print_thread_performance(db_conn, db_server):
    print('===== Thread Performance =====')
    print_results(get_output_from_db(db_conn, db_server, """SELECT thread, COUNT( * ) 
                                        FROM  `mailing_list` GROUP BY thread"""))