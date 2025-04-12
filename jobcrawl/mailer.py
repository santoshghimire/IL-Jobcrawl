import smtplib
import logging
import mimetypes
import time
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from jobcrawl import settings
import os
import base64
from mailjet_rest import Client

email_from = settings.EMAIL_FROM
email_to = settings.EMAIL_TO
smtp_server = settings.SMTP_SERVER
smtp_port = settings.SMTP_PORT
username = settings.SMTP_USERNAME
password = settings.SMTP_PASSWORD
mailjet_api_key = settings.MAILJET_API_KEY
mailjet_secret_key = settings.MAILJET_SECRET_KEY


def send_plain_email(subject, body, to=None, multi=False):
    msg = MIMEMultipart()
    msg["From"] = email_from
    if to:
        msg["To"] = to
    else:
        email_to = settings.EMAIL_TO
        email_to = email_to.split(',')[0]
        msg["To"] = email_to
    msg["Subject"] = subject
    msg.preamble = subject
    textpart = MIMEText(body, 'plain')

    msg.attach(textpart)

    server = smtplib.SMTP("{}:{}".format(smtp_server, smtp_port))
    server.starttls()
    server.login(username, password)
    server.sendmail(email_from, email_to.split(","), msg.as_string())
    logging.info('***************************************************')
    logging.info('Email Successfully Sent to {} .'
        'subject={}, body={}'.format(email_to, subject, body))
    logging.info('***************************************************')
    server.quit()



# Helper to encode file to base64
def encode_file(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode()


def send_email_mailjet_attach(subject, body, file_to_send):
    mailjet = Client(auth=(mailjet_api_key, mailjet_secret_key), version='v3.1')
    attachments = []
    for fpath in file_to_send:
        fname = os.path.basename(fpath)
        attachments.append(
            {
                "ContentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "Filename": fname,
                "Base64Content": encode_file(fpath)
            })

    to_emails = [{"Email": i} for i in email_to.split(',')]
    data = {
        'Messages': [
            {
                "From": {"Email": email_from},
                "To": to_emails,
                "Subject": subject,
                "TextPart": body,
                "Attachments": attachments
            }
        ]
    }
    result = mailjet.send.create(data=data)
    logging.info("Mailjet email sent status={}, response={}".format(result.status_code, result.json()))


def send_email(directory, file_name, body, multi=False):
    if multi:
        file_to_send = ["{}/{}".format(directory, i) for i in file_name]
    else:
        file_to_send = ["{}/{}".format(directory, file_name)]
    if multi:
        subject = '{}_Daily-List-Of-Competitor-Jobs.xlsx'.format(
            file_name[0][:10])
    else:
        subject = file_name

    msg = MIMEMultipart()
    msg["From"] = email_from
    msg["To"] = email_to
    msg["Subject"] = subject
    msg.preamble = subject
    textpart = MIMEText(body, 'plain')

    for each_file in file_to_send:
        attachment = get_attachment(each_file)
        attachment.add_header(
            "Content-Disposition", "attachment",
            filename=os.path.basename(each_file)
        )
        msg.attach(attachment)

    msg.attach(textpart)

    try:
        server = smtplib.SMTP("{}:{}".format(smtp_server, smtp_port))
        server.starttls()
        server.login(username, password)
        server.sendmail(email_from, email_to.split(","), msg.as_string())
        logging.info('***************************************************')
        logging.info('Email Successfully Sent via SMTP to {} .'
            'directory={}, file_name={}, body={}'
            ''.format(email_to, directory, file_name, body))
        logging.info('***************************************************')
        server.quit()
        return
    except OSError:
        logging.exception('Sending email with smtp failed. Trying with mailjet')

    try:
        send_email_mailjet_attach(subject, body, file_to_send)
        logging.info('***************************************************')
        logging.info('Email Successfully Sent via Mailjet to {} .'
            'directory={}, file_name={}, body={}'
            ''.format(email_to, directory, file_name, body))
        logging.info('***************************************************')
    except Exception:
        logging.exception('Sending email with mailjet api failed.')


def get_attachment(file_to_send):
    ctype, encoding = mimetypes.guess_type(file_to_send)
    if ctype is None or encoding is not None:
        ctype = "application/octet-stream"

    maintype, subtype = ctype.split("/", 1)

    if maintype == "text":
        fp = open(file_to_send)
        # Note: we should handle calculating the charset
        attachment = MIMEText(fp.read(), _subtype=subtype)
        fp.close()
    elif maintype == "image":
        fp = open(file_to_send, "rb")
        attachment = MIMEImage(fp.read(), _subtype=subtype)
        fp.close()
    elif maintype == "audio":
        fp = open(file_to_send, "rb")
        attachment = MIMEAudio(fp.read(), _subtype=subtype)
        fp.close()
    else:
        fp = open(file_to_send, "rb")
        attachment = MIMEBase(maintype, subtype)
        attachment.set_payload(fp.read())
        fp.close()
        encoders.encode_base64(attachment)
    return attachment
