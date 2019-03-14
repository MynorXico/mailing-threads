#!/usr/bin/env python

# imports
import MySQLdb
import smtplib
import timeit

# DB Constants
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_DABA = 'mailer'
DB_TABL = 'mailing_list'

# Threading Config
WORKER_THREAD_COUNT = 20

# SMTP Constants
SMTP_SERVER = 'smtp.gmail.com'  					# e.g: smtp.gmail.com
SMTP_PORT = 587                         				# Common SMTP port
SMTP_USER = 'sistemasopertivos2019@gmail.com'                          	# SMTP User Credentials
SMTP_PASS = '57600667'				                        # SMTP Pass Credentials

# MAIL Constants
MAIL_SENDER = 'sistemasoperativos2019@gmail.com'  	# Sender's email
MAIL_SUBJECT = 'Test'			      		# Subject Line, can be dynamic
MAIL_BODY = 'This is a test.'                		# Mail body, can be dynamic

# Server Connection to MySQL for Thread.
db_server = MySQLdb.connect(host=DB_HOST,
                            user=DB_USER,
                            passwd=DB_PASS,
                            db=DB_DABA)
db_conn = db_server.cursor()

# SMTP Mail server connection for thread.
smtp_session = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
smtp_session.ehlo()
smtp_session.starttls()
smtp_session.login(SMTP_USER, SMTP_PASS)

# Looping till we have rows available.
start_time = timeit.timeit()
while True:

    # Fetching One row for processing and locking row with
    # MySQL InnoDB FOR UPDATE clause.
    db_conn.execute("""SELECT * FROM """ + \
    DB_TABL + """ WHERE status=0 LIMIT 1 FOR UPDATE""")
    row = db_conn.fetchone()
    print(row)
    # If we have a row, else break.
    if row:
        # Constructing Mail Headers.
        mail_recipient = row[2]
        mail_headers = ["From: " + MAIL_SENDER,
                        "Subject: " + MAIL_SUBJECT,
                        "To: " + mail_recipient,
                        "MIME-Version: 1.0",
                        "Content-Type: text/html"]

        # Easier to concat.
        mail_headers = "\r\n".join(mail_headers)

        # Sending mail through already existing server connection.
        smtp_session.sendmail(MAIL_SENDER, mail_recipient, \
                              mail_headers + "\r\n\r\n" + MAIL_BODY)
        # Updating Table, and releasing row lock.
        db_conn.execute("""UPDATE """ + DB_TABL + \
                        """ SET status=1, sent_time=NOW(), thread=\"thread1\"  WHERE id="""+str(row[0]))

        # Commiting change to MySQL server
        db_server.commit()
    else:
        break

end_time = timeit.timeit()
# Closing connections once the thread is done.
db_server.close()
smtp_session.quit()

def get_output_from_db(query):
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

print ('===== Time Performance =====')
print_results(get_output_from_db("SELECT sent_time, COUNT( * ) " \
"FROM  `mailing_list` GROUP BY sent_time"))
print ('===== Thread Performance =====')
print_results(get_output_from_db("SELECT thread, COUNT( * ) "\
"FROM  `mailing_list` GROUP BY thread"))

print("Execution time: %is" % ((end_time-start_time)*1000))