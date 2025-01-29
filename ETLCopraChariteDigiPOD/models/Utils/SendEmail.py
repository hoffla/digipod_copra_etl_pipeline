import os, requests
import smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

from models.Utils.logger import get_logger

logger = get_logger(__name__)


def send_email_smtp(subject, body, recipients):
    try:
        smtp_host = 'smtp-out.charite.de'
        smtp_port = 587
        smtp_username = os.getenv('EMAIL_USERNAME')
        smtp_password = os.getenv('EMAIL_PASSWORD')
        smtp_sender = os.getenv('EMAIL_SENDER')
        recipients_email = os.getenv(recipients).replace(' ', '').split(',')

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)

            if not all([smtp_username, smtp_password, recipients_email]):
                logger.error("Error: The enviromental variable are not correctly configured.")
                return

            msg = MIMEMultipart()
            msg['From'] = smtp_username
            msg['To'] = os.getenv(recipients)
            msg['Subject'] = subject

            msg.attach(MIMEText(body, 'plain'))

            failed_emails = server.sendmail(smtp_sender, recipients_email, msg.as_string())
            if failed_emails: raise RuntimeError(failed_emails) 
            else: logger.info("Email was successfully sended!")
    except Exception as err:
        logger.error(f'The following email {subject} could not be send. Error: {err}')


def send_mail_intern(subject, body, recipients, mail_attachment=None):
    msg = MIMEMultipart()
    msg['From'] = 'Digi-POD Projekt'
    msg['To'] = os.getenv(recipients).replace(' ', '').split(',')
    msg['Subject'] = subject
    
    msg.attach(MIMEText(body, 'html'))
    
    if mail_attachment and os.path.exists(mail_attachment):
        with open(mail_attachment, 'rb') as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(mail_attachment)}"')
        msg.attach(part)
    
    smtp_host = "mail-cbf.charite.de"
    smtp_port = 25
    
    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.sendmail('noreply_digipod@charite.de', recipients, msg.as_string())
        logger.info("Email enviado com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao enviar o email: {e}")


def send_email(subject, message, recipients):
    response = requests.post(
        f"https://api.mailgun.net/v3/{os.getenv('DOMAIN_NAME')}/messages",
        auth=("api", os.getenv('API_KEY')),
        data={
            "from": f"DigiPOD ETL Pipeline Message <mailgun@{os.getenv('DOMAIN_NAME')}>",
            "to": os.getenv(recipients).replace(' ', '').split(','),
            "subject": subject,
            "text": message},
    )
    logger.info(response)
