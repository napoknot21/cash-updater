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


# Fundations
FUNDATIONS = {

    "HV" : os.getenv("HV"),
    "WR" : os.getenv("WR")

}


# Email informtion schema
EMAIL_COLUMNS = {

    "Id" : pl.Utf8,
    "Subject" : pl.Utf8,
    "From" : pl.Utf8,
    "Received DateTime" : pl.Utf8,#pl.Datetime,
    "Attachments" : pl.Boolean,
    "Shared Email" : pl.Utf8

}


# Cash columns format
CASH_COLUMNS = {

    "Fundation" : pl.Utf8,
    "Account" : pl.Utf8, # "Account Number" : pl.Utf8,
    "Date" : pl.Datetime,
    "Bank" : pl.Utf8,
    "Currency" : pl.Utf8,
    "Type" : pl.Utf8,
    "Amount in CCY" : pl.Float64,
    "Exchange" : pl.Float64,
    "Amount in EUR" : pl.Float64

}


# Collateral columns format
COLLATERAL_COLUMNS = {

    "Fundation" : pl.Utf8,
    "Account" : pl.Utf8, # "Account Number" : pl.Utf8,
    "Date" : pl.Datetime,
    "Bank" : pl.Utf8,
    "Currency" : pl.Utf8,
    "Total" : pl.Float64, #"Total Collateral at Bank" : pl.Float64,
    "IM" : pl.Float64,
    "VM" : pl.Float64,
    "Requirement" : pl.Float64,
    "Net Exess/Deficit" : pl.Float64

}


# Counterparties

MS = {

    "emails": {e.strip() for e in os.getenv("MS_EMAILS").split(";") if e.strip()},
    "subject": os.getenv("MS_SUBJECT_WORDS").strip(),
    "filenames": {f.strip() for f in os.getenv("MS_FILENAMES").split(";") if f.strip()},

}

GS = {

    "emails": {e.strip() for e in os.getenv("GS_EMAILS").split(";") if e.strip()},
    "subject": os.getenv("GS_SUBJECT_WORDS").strip(),
    "filenames": {f.strip() for f in os.getenv("GS_FILENAMES").split(";") if f.strip()}

}


SAXO = {

    "emails": {e.strip() for e in os.getenv("SAXO_EMAILS").split(";") if e.strip()},
    "subject": os.getenv("SAXO_SUBJECT_WORDS").strip(),
    "filenames": {f.strip() for f in os.getenv("SAXO_FILENAMES").split(";") if f.strip()}

}

# EDB
EDB = {

    "emails": {e.strip() for e in os.getenv("EDB_EMAILS").split(";") if e.strip()},
    "subject": os.getenv("EDB_SUBJECT_WORDS").strip(),
    "filenames": {f.strip() for f in os.getenv("EDB_FILENAMES").split(";") if f.strip()}

}

EBD_ATTACHMENT_DIR_ABS_PATH = os.getenv("EBD_ATTACHMENT_DIR_ABS_PATH")
EDB_TYPE_ALLOWED = os.getenv("EDB_TYPE_ALLOWED")
EDB_DESCRIPTION_ALLOWED = [os.getenv("EDB_DESCRIPTION_ALLOWED_1"), os.getenv("EDN_DESCRIPTION_ALLOWED_2")]

EDB_REQUIRED_COLUMNS = {

    "TYPE" : pl.Utf8,
    "DESCRIPTION" : pl.Utf8,
    "ACCOUNT" : pl.Utf8,
    "CURRENCY" : pl.Utf8,
    "AMOUNT" : pl.Float64
    
}

# UBS
UBS = {

    "emails": {e.strip() for e in os.getenv("UBS_EMAILS").split(";") if e.strip()},
    "subject": os.getenv("UBS_SUBJECT_WORDS").strip(),
    "filenames": {f.strip() for f in os.getenv("UBS_FILENAMES").split(";") if f.strip()}
}

COUNTERPARTIES = {

    "MS" : MS,
    "GS" : GS,
    "SAXO" : SAXO,
    "EDB" : EDB,
    "UBS" : UBS

}

PAIRS = ["EURUSD=X", "EURCHF=X", "EURGBP=X", "EURJPY=X"]