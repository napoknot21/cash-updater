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

            df_email = get_inbox_messages_between(start_date=start_date, token=token, email=email, with_attach=True)
            
            if df_email.is_empty() :

                print("No messages found")
                continue

        except Exception as e :

            print(f"Error printing inbox of {email}: {e}")

        df = pl.concat([df, df_email], how="vertical")


    path = df.write_excel("./raw/emails.xlsx")

    #print(df)

    # CASH email information for different banks
    #df_gs, df_ms, df_saxo, df_ubs, df_edb = extract_emails_by_bank(df)
    rules_df = split_by_counterparty(df)

    for k, v in rules_df.items() :
        
        if k == "UNMATCHED" :
            continue
        
        print(v)
        v.write_excel("./raw/" + k + ".xlsx")
        
        #download_attachments_for_message()

        


if __name__ == '__main__' :
    """
    
    """
    parser = argparse.ArgumentParser(description="Process shared mailboxes")
    
    parser.add_argument("--shared-emails", nargs="+", required=False, help="List of shared mailboxes to treat")

    args = parser.parse_args()

    main(args.shared_emails)