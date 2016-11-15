import smtplib
import mimetypes
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from jobcrawl import settings
import os

email_from = settings.EMAIL_FROM
email_to = settings.EMAIL_TO
smtp_server = settings.SMTP_SERVER
smtp_port = settings.SMTP_PORT
username = settings.SMTP_USERNAME
password = settings.SMTP_PASSWORD


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

    server = smtplib.SMTP("{}:{}".format(smtp_server, smtp_port))
    server.starttls()
    server.login(username, password)
    server.sendmail(email_from, email_to.split(","), msg.as_string())
    print('***************************************************')
    print('Email Successfully Sent to {} '.format(email_to))
    print('***************************************************')
    server.quit()


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
