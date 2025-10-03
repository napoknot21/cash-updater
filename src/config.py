from __future__ import annotations

import os
import re
import polars as pl

from dotenv import load_dotenv

load_dotenv()

# Application settings and values
APPLICATION_ID=os.getenv("APPLICATION_ID")
SECRET_VALUE_ID=os.getenv("SECRET_VALUE_ID")

OBJECT_ID=os.getenv("OBJECT_ID")
TENANT_ID=os.getenv("TENANT_ID")
SECRET_ID=os.getenv("SECRET_ID")


# Permissions + Scopes
GRAPH_BASE = "https://graph.microsoft.com/v1.0"
SCOPES = ["https://graph.microsoft.com/.default"]
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"


# Shared emails
SHARED_MAIL_1=os.getenv("SHARED_MAIL_1")
SHARED_MAIL_2=os.getenv("SHARED_MAIL_2")

SHARED_MAILS = [SHARED_MAIL_1, SHARED_MAIL_2]


# Tech credentials
TECH_EMAIL=os.getenv("TECH_EMAIL")
TECH_PASSW=os.getenv("TECH_PASSW")


# Group credentials
GROUP_EMAIL=os.getenv("GROUP_EMAIL")


# Email informtion schema
EMAIL_COLUMNS = {

    "id" : pl.Utf8,
    "subject" : pl.Utf8,
    "from" : pl.Utf8,
    "receivedDateTime" : pl.Utf8,#pl.Datetime,
    "hasAttachments" : pl.Boolean,
    "originEmail" : pl.Utf8

}


# Counterparties

MS = {

    "emails": {e.strip() for e in os.getenv("MS_EMAILS").split(";") if e.strip()},
    "subject": "(?i)" + "|".join(re.escape(w.strip()) for w in os.getenv("MS_SUBJECT_WORDS").split(";") if w.strip()),
    "filenames": {f.strip() for f in os.getenv("MS_FILENAMES").split(";") if f.strip()},

}

GS = {

    "emails": {e.strip() for e in os.getenv("GS_EMAILS").split(";") if e.strip()},
    "subject": "(?i)" + "|".join(re.escape(w.strip()) for w in os.getenv("GS_SUBJECT_WORDS").split(";") if w.strip()),
    "filenames": {f.strip() for f in os.getenv("GS_FILENAMES").split(";") if f.strip()}

}

SAXO = {

    "emails": {e.strip() for e in os.getenv("SAXO_EMAILS").split(";") if e.strip()},
    "subject": "(?i)" + "|".join(re.escape(w.strip()) for w in os.getenv("SAXO_SUBJECT_WORDS").split(";") if w.strip()),
    "filenames": {f.strip() for f in os.getenv("SAXO_FILENAMES").split(";") if f.strip()}

}

EDB = {

    "emails": {e.strip() for e in os.getenv("EDB_EMAILS").split(";") if e.strip()},
    "subject": "(?i)" + "|".join(re.escape(w.strip()) for w in os.getenv("EDB_SUBJECT_WORDS").split(";") if w.strip()),
    "filenames": {f.strip() for f in os.getenv("EDB_FILENAMES").split(";") if f.strip()}

}

UBS = {

    "emails": {e.strip() for e in os.getenv("UBS_EMAILS").split(";") if e.strip()},
    "subject": "(?i)" + "|".join(re.escape(w.strip()) for w in os.getenv("UBS_SUBJECT_WORDS").split(";") if w.strip()),
    "filenames": {f.strip() for f in os.getenv("UBS_FILENAMES").split(";") if f.strip()}

}

COUNTERPARTIES = {

    "MS" : MS,
    "GS" : GS,
    "SAXO" : SAXO,
    "EDB" : EDB,
    "UBS" : UBS

}