#!/usr/bin/env python


# SMTP Constants
SMTP_SERVER = 'smtp.gmail.com'  # Gmail Server
SMTP_PORT = 587  # Common SMTP port
SMTP_USER = 'sistemasopertivos2019@gmail.com'  # SMTP User
SMTP_PASS = '57600667'  # SMTP password

# MAIL Constants
MAIL_SENDER = 'sistemasoperativos2019@gmail.com'  # Sender's email
MAIL_SUBJECT = 'Test'  # Subject Line, can be dynamic
MAIL_BODY = 'This is a test.'  # Mail body, can be dynamic


def build_headers(mail_recipient):
    mail_headers = ["From: %s" % MAIL_SENDER,
                    "Subject: %s" % MAIL_SUBJECT,
                    "To: %s" % mail_recipient,
                    "MIME-Version: 1.0",
                    "Content-Type: text/html"]
    mail_headers = "\r\n".join(mail_headers)  # Easier to concat
    return mail_headers

def send_mail(smtp_session, mail_recipient, mail_headers):
    # Sending mail through already existing
    smtp_session.sendmail(MAIL_SENDER,
                          mail_recipient,
                          mail_headers + "\r\n\r\n" + MAIL_BODY)
def close_connection(smtp_session):
    smtp_session.quit()