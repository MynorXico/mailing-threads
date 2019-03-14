#!/usr/bin/env python

# imports
import MySQLdb
import smtplib
import threading
import timeit
import mail_manager
import db_manager

# SMTP Constants
SMTP_SERVER = 'smtp.gmail.com'  # Gmail Server
SMTP_PORT = 587  # Common SMTP port
SMTP_USER = 'sistemasopertivos2019@gmail.com'  # SMTP User
SMTP_PASS = '57600667'  # SMTP password

# DB Constants
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_DABA = 'mailer'
DB_TABL = 'mailing_list'

# Threading Config
WORKER_THREAD_COUNT = 16


# Threading Class.
class MailingThread(threading.Thread):
    '''This class would help generate multiple threads.'''
    def __init__(self, name):
        threading.Thread.__init__(self)
        self.name = name

    def run(self):
        smtp_session = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        smtp_session = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        smtp_session.ehlo()
        smtp_session.starttls()
        smtp_session.login(SMTP_USER, SMTP_PASS)

        db_server = MySQLdb.connect(host=DB_HOST,
                                    user=DB_USER,
                                    passwd=DB_PASS,
                                    db=DB_DABA)
        db_conn = db_server.cursor()

        # Looping till we have rows available.
        while True:

            # Fetching One row for processing and locking row with
            # MySQL InnoDB FOR UPDATE clause.
            row = db_manager.retrieve_single_row(db_conn)

            # If we have a row, else break.
            if row:

                # Constructing Mail Headers.
                mail_recipient = row[2]
                mail_headers = mail_manager.build_headers(mail_recipient)
                # Sending mail through already existing server connection.
                mail_manager.send_mail(smtp_session, mail_recipient, mail_headers)

                # Updating Table, and releasing row lock.
                db_manager.check_row(db_conn, db_server, row[0], self.name)

            else:
                break
        # Closing connections once the thread is done.
        mail_manager.close_connection(smtp_session)
        db_manager.close_connection(db_server)

# Python list to maintain all thread instances
ALL_THREADS = []

# Starting all the threads
start_time = timeit.timeit()
for i in range(WORKER_THREAD_COUNT):
    thread = MailingThread('thread_%d' % i)
    thread.start()
    ALL_THREADS.append(thread)

# Waiting for all threads to finish
for thread in ALL_THREADS:
    thread.join()

end_time = timeit.timeit()

db_server = MySQLdb.connect(host=DB_HOST,
                                    user=DB_USER,
                                    passwd=DB_PASS,
                                    db=DB_DABA)
db_conn = db_server.cursor()

db_manager.print_time_performance(db_conn, db_server)
db_manager.print_thread_performance(db_conn, db_server)
print("Execution time: %is" % ((end_time-start_time)*1000))