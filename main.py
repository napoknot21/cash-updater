from __future__ import annotations

import os
import argparse

import datetime as dt
from typing import Dict, List, Optional, Tuple

from src.config import SHARED_MAILS
from src.extraction import split_by_counterparty
from src.msla import *


def main (
    
        start_date : Optional[str | dt.datetime] = None,
        end_date : Optional[str | dt.datetime] = None,
        token : Optional[str] = None,
        shared_emails: Optional[List[str]] = None,
        schema_df : Optional[Dict] = None
    
    ) -> None:
    """
    Main entry point
    """
    token = get_token() if token is None else token
    shared_emails = SHARED_MAILS if shared_emails is None else shared_emails
    schema_df = EMAIL_COLUMNS if schema_df is None else schema_df

    df = pl.DataFrame(schema=schema_df)

    for email in shared_emails :

        print(f"\n[*] Processing shared email: {email}\n")
        
        try :

            df_email = get_inbox_messages_between(start_date=start_date, end_date=end_date, token=token, email=email, with_attach=True)
            
            if df_email.is_empty() :

                print("\n[-] No messages found.\n")
                continue

        except Exception as e :

            print(f"\n[-] Error printing inbox of {email}: {e}\n")

        df = pl.concat([df, df_email], how="vertical")


    #path = df.write_excel("./raw/emails.xlsx")

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
    
    args = parser.parse_args()

    # **Always** pass by keyword to avoid positional mixups
    main(

        shared_emails=args.shared_emails,
        start_date=args.start_date,
        end_date=args.end_date,
    
    )