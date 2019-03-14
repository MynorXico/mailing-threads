import queue
import threading
import time
import MySQLdb
import smtplib
import db_manager
import mail_manager
done = 0

n_threads = 6

# DB Constants
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = ''
DB_DABA = 'mailer'
DB_TABL = 'mailing_list'

# SMTP Constants
SMTP_SERVER = 'smtp.gmail.com'  # Gmail Server
SMTP_PORT = 587  # Common SMTP port
SMTP_USER = 'threadsos2019@gmail.com'  # SMTP User
SMTP_PASS = '57600667'  # SMTP password


class MailingThread(threading.Thread):
    def __init__(self, threadName, mailing_q):
        threading.Thread.__init__(self)
        self.threadName = threadName
        self.mailing_q = mailing_q
    def run(self):


        print("Starts thread: %s" % self.threadName)

        # Server Connection to MySQL for Thread.
        db_server = MySQLdb.connect(host=DB_HOST,
                                    user=DB_USER,
                                    passwd=DB_PASS,
                                    db=DB_DABA)
        db_conn = db_server.cursor()
        try:
            smtp_session = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        except:
            return
        smtp_session.ehlo()
        smtp_session.starttls()
        smtp_session.login(SMTP_USER, SMTP_PASS)

        while done == 0:
            mailing_q_lock.acquire()
            if not mails_queue.empty():
                row = mails_queue.get()
                mailing_q_lock.release()
                mail_recipient = row[2]
                mail_headers = mail_manager.build_headers(mail_recipient)
                try:
                    mail_manager.send_mail(smtp_session, mail_recipient, mail_headers)
                    db_manager.check_row(db_conn, db_server, row[0],self.threadName)
                except:
                    mails_queue.put(row)
        db_manager.close_connection(db_server)
        try:
            mail_manager.close_connection(smtp_session)
        except:
            print("GG")
        print("Ends thread: %s" % self.threadName)


mailing_q_lock = threading.Lock()
mails_queue = queue.Queue(100)

threads = []

# Create new threads
for i in range(n_threads):
    thread = MailingThread(i, mails_queue)
    thread.start()
    threads.append(thread)

# Fills the queue
mailing_q_lock.acquire()
rows = db_manager.retrieve_rows()
for row in rows:
    mails_queue.put(row)
mailing_q_lock.release()

# Wait for the queue to be empty
while not mails_queue.empty():
    pass



done = 1
for t in threads:
    print("Joining %s" % str(t.threadName))
    t.join()
db_server = MySQLdb.connect(host=DB_HOST,
                                    user=DB_USER,
                                    passwd=DB_PASS,
                                    db=DB_DABA)
db_conn = db_server.cursor()

db_manager.print_time_performance(db_conn, db_server)
db_manager.print_thread_performance(db_conn, db_server)
