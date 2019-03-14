#!/usr/bin/env python

# imports
import MySQLdb
import timeit
import mail_manager
import db_manager
import smtplib

# DB Constants
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_DABA = 'mailer'
DB_TABL = 'mailing_list'

# Threading Config
WORKER_THREAD_COUNT = 20


# Server Connection to MySQL for Thread.
db_server = MySQLdb.connect(host=DB_HOST,
                            user=DB_USER,
                            passwd=DB_PASS,
                            db=DB_DABA)
db_conn = db_server.cursor()
start_time = timeit.timeit()

# SMTP Constants
SMTP_SERVER = 'smtp.gmail.com'  # Gmail Server
SMTP_PORT = 587  # Common SMTP port
SMTP_USER = 'sistemasopertivos2019@gmail.com'  # SMTP User
SMTP_PASS = '57600667'  # SMTP password

smtp_session = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
smtp_session = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
smtp_session.ehlo()
smtp_session.starttls()
smtp_session.login(SMTP_USER, SMTP_PASS)

# Looping until we have rows available.
while True:
    # Fetching One row for processing and locking row with
    # MySQL InnoDB FOR UPDATE clause.
    row = db_manager.retrieve_single_row(db_conn)

    # If we have a row, else break.
    if row:
        mail_recipient = row[2]
        # Building headers
        mail_headers = mail_manager.build_headers(mail_recipient)
        # Sending mail
        mail_manager.send_mail(smtp_session, mail_recipient, mail_headers)
        # Updating Table, and releasing row lock.
        db_manager.check_row(db_conn, db_server, row[0], 'main_thread')
    else:
        break
mail_manager.close_connection(smtp_session)
db_manager.close_connection(db_server)

end_time = timeit.timeit()
# Closing connections once the thread is done.

db_manager.print_time_performance(db_conn, db_server)
db_manager.print_thread_performance(db_conn, db_server)


print("Execution time: %d s" % ((end_time-start_time)*1000))