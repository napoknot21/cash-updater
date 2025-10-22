from __future__ import annotations

import os
import argparse
import time
import yfinance as yf
import datetime as dt
import polars as pl
import pandas as pd # type: ignore

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

from src.config import SHARED_MAILS, PAIRS
from src.extraction import split_by_counterparty
from src.msla import *
from src.api import call_api_for_pairs
from src.utils import date_to_str

from src.counterparties.edb import edb_cash, edb_collateral
from src.counterparties.saxo import saxo_cash, saxo_collateral
from src.counterparties.gs import gs_cash, gs_collateral
from src.counterparties.ms import ms_cash, ms_collateral
#from src.counterparties.ubs import ubs_cash, ubs_collateral

def main (
    
        start_date : Optional[str | dt.datetime] = None,
        end_date : Optional[str | dt.datetime] = None,
        token : Optional[str] = None,
        fundation : Optional[str] = "HV",
        shared_emails: Optional[List[str]] = None,
        pairs : Optional[List[str]] = None,
        schema_df : Optional[Dict] = None
    
    ) -> None :
    """
    Main entry point
    """
    """
    token = get_token() if token is None else token
    shared_emails = SHARED_MAILS if shared_emails is None else shared_emails
    schema_df = EMAIL_COLUMNS if schema_df is None else schema_df
    """
    pairs = PAIRS if pairs is None else pairs

    start_date = date_to_str(start_date)
    end_date = date_to_str(end_date)
    
    close_values = call_api_for_pairs(None, pairs)
    print(f"\n{close_values}")

    """
    df = pl.DataFrame(schema=schema_df)

    for email in shared_emails :

        print(f"\n[*] Processing shared email: {email}")

        try :

            df_email = get_inbox_messages_between(start_date=start_date, end_date=end_date, token=token, email=email, with_attach=True)
            
            if df_email.is_empty() :

                print("\n[-] No messages found.\n")
                continue

        except Exception as e :
            print(f"\n[-] Error printing inbox of {email}: {e}")

        df = pl.concat([df, df_email], how="vertical")

    # CASH email information for different banks
    rules_df = split_by_counterparty(df)

    # Here
    # k => counterparty name
    # v -> dataframe of k (filtered info)

    for k, v in rules_df.items() :
        
        if k == "UNMATCHED" :
            continue
        
        v.write_excel("./raw/" + k.lower() + ".xlsx")
        
        for row in v.to_dicts() :

            id = row["Id"]
            origin = row["Shared Email"]
            
            download_attachments_for_message(id, token, f"./attachments/{k}", origin)
    """
    fundation = "HV"
    out = saxo_collateral(start_date, fundation, close_values)
    
    out.write_excel("testt.xlsx")

    print(out)

if __name__ == '__main__' :
    """
    
    """
    parser = argparse.ArgumentParser(description="Process shared mailboxes")

    parser.add_argument(
        "--shared-emails", nargs="+", required=False, help="List of shared mailboxes to treat"
    )
    
    parser.add_argument(
        "--start-date", required=False, help="YYYY-MM-DD or ISO. Default: today 00:00Z"
    )
    
    parser.add_argument(
        "--end-date", required=False, help="YYYY-MM-DD or ISO. Default: same as start or next day"
    )

    parser.add_argument(
        "--fund", required=False, help="Fundation name initials."

    )
    
    args = parser.parse_args()

    # **Always** pass by keyword to avoid positional mixups
    main(

        shared_emails=args.shared_emails,
        start_date=args.start_date,
        end_date=args.end_date,
    
    )