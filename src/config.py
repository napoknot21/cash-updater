from __future__ import annotations

import os
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


ALL_FUNDATIONS = [

    "HV",
    "WR"

]

ALL_KINDS = [

    "cash",
    "collateral"

]

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
    "Date" : pl.Date,
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
    "Date" : pl.Date,
    "Bank" : pl.Utf8,
    "Currency" : pl.Utf8,
    "Total" : pl.Float64, #"Total Collateral at Bank" : pl.Float64,
    "IM" : pl.Float64,
    "VM" : pl.Float64,
    "Requirement" : pl.Float64,
    "Net Excess/Deficit" : pl.Float64

}


KINDS_COLUMNS_DICT = {

    "cash" : CASH_COLUMNS,
    "collateral" : COLLATERAL_COLUMNS

}


CACHE_COLUMNS = {

    "Date" : pl.Date,
    "Bank" : pl.Utf8,
    "Fundation" : pl.Utf8,
    "Kind" : pl.Utf8,
    "Filename" : pl.Utf8

}


# PATHS

CACHE_DIR_ABS_PATH = os.getenv("CACHE_DIR_ABS_PATH")
CACHE_FILE_NAME = os.getenv("CACHE_FILE_NAME")

CACHE_FILENAME_ABS = os.path.join(CACHE_DIR_ABS_PATH, CACHE_FILE_NAME)

HISTORY_DIR_ABS_PATH= os.getenv("HISTORY_DIR_ABS_PATH")
ATTACH_DIR_ABS_PATH = os.getenv("ATTACH_DIR_ABS_PATH")
RAW_DIR_ABS_PATH = os.getenv("RAW_DIR_ABS_PATH")


FREQUENCY_DATE_MAP = {

    "Day" : "1d",
    "Week" : "1w",
    "Month" : "1mo",
    "Quarter" : "1q",
    "Year" : "1y"

}


## Counterparties

# MS
MS = {

    "emails": {e.strip() for e in os.getenv("MS_EMAILS").split(";") if e.strip()},
    "subject": os.getenv("MS_SUBJECT_WORDS").strip(),
    "filenames": {f.strip() for f in os.getenv("MS_FILENAMES").split(";") if f.strip()},

}

MS_REQUIRED_COLUMNS = {

    "account" : pl.Utf8,
    "ccy" : pl.Utf8,
    "quantity" : pl.Float64
}

MS_TARGET_FIELDS = {

    "Net MTM" : pl.Float64,
    "Upfront Amount Rec / (Pay)" : pl.Float64,
    "Customer Balances" : pl.Float64

}

MS_FILENAMES_CASH = os.getenv("MS_FILENAMES_CASH")
MS_FILENAMES_COLLATERAL = os.getenv("MS_FILENAMES_COLLATERAL")

MS_TABLE_PAGES = {

    "HV" : os.getenv("MS_TABLE_PAGE_HV"),
    "WR" : os.getenv("MS_TABLE_PAGE_WR")

}

MS_FILENAMES = os.getenv("MS_FILENAMES")

MS_ATTACHMENT_DIR_ABS_PATH = os.getenv("MS_ATTACHMENT_DIR_ABS_PATH")

MS_ENTITY = os.getenv("MS_ENTITY")

MS_ACCOUNTS = {

    "HV" : os.getenv("MS_ACCOUNT_HV"),
    "WR" : os.getenv("MS_ACCOUNT_WR")

}


# GS
GS = {

    "emails": {e.strip() for e in os.getenv("GS_EMAILS").split(";") if e.strip()},
    "subject": os.getenv("GS_SUBJECT_WORDS").strip(),
    "filenames": {f.strip() for f in os.getenv("GS_FILENAMES").split(";") if f.strip()}

}

GS_REQUIRED_COLUMNS = {

    "GS Entity" : pl.Utf8,
    "Account Number" : pl.Utf8,
    "Post/Held" : pl.Utf8,
    "Quantity" : pl.Float64,
    "Currency" : pl.Utf8,

}

GS_TARGET_FIELDS = {

    "Total Collateral" : pl.Float64,
    "CP Initial Margin" : pl.Float64,
    "Total Requirement" : pl.Float64,
    "Total Exposure" : pl.Float64,
    "Reference ccy" : pl.Utf8,
    "Exposure (VM)" : pl.Float64,

}

GS_FILENAMES_CASH = os.getenv("GS_FILENAMES_CASH")
GS_FILENAMES_COLLATERAL = os.getenv("GS_FILENAMES_COLLATERAL")

GS_FILENAMES = os.getenv("GS_FILENAMES")

GS_ENTITY = os.getenv("GS_ENTITY")

GS_ACCOUNTS = {

    "HV" : os.getenv("GS_ACCOUNT_HV"),
    "WR" : os.getenv("GS_ACCOUNT_WR")

}

GS_ATTACHMENT_DIR_ABS_PATH = os.getenv("GS_ATTACHMENT_DIR_ABS_PATH")

# SAXO
SAXO = {

    "emails": {e.strip() for e in os.getenv("SAXO_EMAILS").split(";") if e.strip()},
    "subject": os.getenv("SAXO_SUBJECT_WORDS").strip(),
    "filenames": {f.strip() for f in os.getenv("SAXO_FILENAMES").split(";") if f.strip()}

}

SAXO_REQUIRED_COLUMNS = {

    "Account" : pl.Utf8,
    "AccountCurrency" : pl.Utf8,
    "Balance" : pl.Float64,
    "TotalEquity" : pl.Float64,
    "ValueDateCashBalance" : pl.Float64,
    "AccountFunding" : pl.Float64
    
}

SAXO_FILENAMES = os.getenv("SAXO_FILENAMES")
SAXO_ATTACHMENT_DIR_ABS_PATH = os.getenv("SAXO_ATTACHMENT_DIR_ABS_PATH")


# EDB
EDB = {

    "emails": {e.strip() for e in os.getenv("EDB_EMAILS").split(";") if e.strip()},
    "subject": os.getenv("EDB_SUBJECT_WORDS").strip(),
    "filenames": {f.strip() for f in os.getenv("EDB_FILENAMES").split(";") if f.strip()}

}

EBD_ATTACHMENT_DIR_ABS_PATH = os.getenv("EBD_ATTACHMENT_DIR_ABS_PATH")
EDB_CASH_TYPE_ALLOWED = os.getenv("EDB_TYPE_ALLOWED_1")

EDB_CASH_DESC_ALLOWED = [

    os.getenv("EDB_DESCRIPTION_ALLOWED_1"),
    os.getenv("EDB_DESCRIPTION_ALLOWED_2")

]

EDB_COLLAT_TYPE_ALLOWED = [
    
    os.getenv("EDB_TYPE_ALLOWED_1"),
    os.getenv("EDB_TYPE_ALLOWED_2")

]

EDB_COLLAT_DESC_ALLOWED = [

    os.getenv("EDB_DESCRIPTION_ALLOWED_1"),
    os.getenv("EDB_DESCRIPTION_ALLOWED_2"),
    os.getenv("EDB_DESCRIPTION_ALLOWED_3")

]

EDB_COLLAT_DESC_DICT = {

    os.getenv("EDB_DESCRIPTION_ALLOWED_1") : ["Total"],
    os.getenv("EDB_DESCRIPTION_ALLOWED_2") : ["IM", "Requirement"],
    os.getenv("EDB_DESCRIPTION_ALLOWED_3") : []

}

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

UBS_REQUIRED_COLUMNS = {

    "Cusip/ISIN" : pl.Utf8,
    "Quantity" : pl.Float64,
    "CCY (Issue)" : pl.Utf8

}

USB_TARGET_FIELDS = {

    "Currency" : pl.Utf8,
    "Mtm Value" : pl.Float64, # VM

    "Client Initial Margin" : pl.Float64, # IM
    "Total Requirement" : pl.Float64, # Requirement
    "Collateral Held by UBS" : pl.Float64, # Total
    "Collateral Pledged by UBS" : pl.Utf8,
    
    "Net Excess/Deficit" : pl.Float64,
    
}

UBS_ATTACHMENT_DIR_ABS_PATH = os.getenv("UBS_ATTACHMENT_DIR_ABS_PATH")

UBS_FILENAMES_CASH = os.getenv("UBS_FILENAMES_CASH")
UBS_FILENAMES_COLLATERAL = os.getenv("UBS_FILENAMES_COLLATERAL")

# Counterparties
COUNTERPARTIES = {

    "MS" : MS,
    "GS" : GS,
    "SAXO" : SAXO,
    "EDB" : EDB,
    "UBS" : UBS

}


# Forex Pairs
PAIRS = ["EURUSD=X", "EURCHF=X", "EURGBP=X", "EURJPY=X", "EURAUD=X"]