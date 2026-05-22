import os

from .config import (
    MAILTRAP_CATEGORY,
    MAILTRAP_SENDER_EMAIL,
    MAILTRAP_SENDER_NAME,
    MAILTRAP_TOKEN,
)


def notifications_enabled():
    return bool(MAILTRAP_TOKEN and MAILTRAP_SENDER_EMAIL)


def send_email(subject, body, recipients):
    if not recipients:
        return {"ok": True, "skipped": True}
    if not notifications_enabled():
        return {"ok": True, "skipped": True}

    try:
        import mailtrap as mt

        mail = mt.Mail(
            sender=mt.Address(email=MAILTRAP_SENDER_EMAIL, name=MAILTRAP_SENDER_NAME),
            to=[mt.Address(email=email) for email in recipients],
            subject=subject,
            text=body,
            category=MAILTRAP_CATEGORY,
        )
        client = mt.MailtrapClient(token=MAILTRAP_TOKEN)
        client.send(mail)
    except Exception as exc:
        return {"ok": False, "err": str(exc)}
    return {"ok": True, "skipped": False}


def parse_email_recipients(value=None):
    raw_value = value if value is not None else os.getenv("AWSQUEUEENGINE_ALERT_TO", "")
    return [email.strip() for email in raw_value.split(",") if email and email.strip()]
