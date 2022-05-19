from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from Config import EMAIL_SENDER_NAME, EMAIL_SENDER_ADDRESS, EMAIL_SENDER_PASSWORD

import smtplib


class Email:

    @staticmethod
    def send_mail_with_attachment(receiver, subject, content, attachments):
        try:
            message = MIMEMultipart()
            message['From'] = "{} <{}>".format(EMAIL_SENDER_NAME, EMAIL_SENDER_ADDRESS)
            message['To'] = ' , '.join(receiver)
            message['Subject'] = subject
            message.attach(MIMEText(content, 'html'))

            for file in attachments:
                attachment = open(file, "rb")
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', "attachment; filename= %s" % file[file.rfind('\\') + 1:])
                message.attach(part)

            session = smtplib.SMTP('smtp.gmail.com', 587)
            session.starttls()
            session.login(EMAIL_SENDER_ADDRESS, EMAIL_SENDER_PASSWORD)
            text = message.as_string()
            session.sendmail(EMAIL_SENDER_ADDRESS, receiver, text)
            session.quit()
            print('Mail Sent!!!')
        except Exception as e:
            print(e)
