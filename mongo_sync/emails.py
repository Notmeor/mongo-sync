# -*- coding: utf-8 -*-


from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from builtins import str
from past.builtins import basestring

import logging
import os
import smtplib

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.utils import formatdate

from mongo_sync.config import conf


SMTP_MAIL_FROM = conf['email']['smtp_mail_from']
SMTP_HOST = conf['email']['smtp_host']
SMTP_PORT = conf['email']['smtp_port']
SMTP_STARTTLS = conf['email']['smtp_starttls']
SMTP_SSL = conf['email']['smtp_ssl']
SMTP_USER = conf['email']['smtp_user']
SMTP_PASSWORD = conf['email']['smtp_password']


def send_email_smtp(to, subject, html_content=None, files=None, dryrun=False,
                    cc=None, bcc=None, mime_subtype='mixed'):
    """
    Send an email with html content

    >>> send_email('test@example.com', 'foo', '<b>Foo</b> bar', ['/dev/null'], dryrun=True)
    """

    if html_content is None:
        html_content = subject

    to = get_email_address_list(to)

    msg = MIMEMultipart(mime_subtype)
    msg['Subject'] = subject
    msg['From'] = SMTP_MAIL_FROM
    msg['To'] = ", ".join(to)
    recipients = to
    if cc:
        cc = get_email_address_list(cc)
        msg['CC'] = ", ".join(cc)
        recipients = recipients + cc

    if bcc:
        # don't add bcc in header
        bcc = get_email_address_list(bcc)
        recipients = recipients + bcc

    msg['Date'] = formatdate(localtime=True)
    mime_text = MIMEText(html_content, 'html')
    msg.attach(mime_text)

    for fname in files or []:
        basename = os.path.basename(fname)
        with open(fname, "rb") as f:
            part = MIMEApplication(
                f.read(),
                Name=basename
            )
            part['Content-Disposition'] = 'attachment; filename="%s"' % basename
            msg.attach(part)

    send_MIME_email(SMTP_MAIL_FROM, recipients, msg, dryrun)

send_email = send_email_smtp


def send_MIME_email(e_from, e_to, mime_msg, dryrun=False):

    if not dryrun:
        s = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) if SMTP_SSL else smtplib.SMTP(SMTP_HOST, SMTP_PORT)
        if SMTP_STARTTLS:
            s.starttls()
        if SMTP_USER and SMTP_PASSWORD:
            s.login(SMTP_USER, SMTP_PASSWORD)
        logging.info("Sent an alert email to " + str(e_to))
        s.sendmail(e_from, e_to, mime_msg.as_string())
        s.quit()


def get_email_address_list(address_string):
    if isinstance(address_string, basestring):
        if ',' in address_string:
            address_string = address_string.split(',')
        elif ';' in address_string:
            address_string = address_string.split(';')
        else:
            address_string = [address_string]

    return address_string