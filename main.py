from __future__ import annotations

import os
import argparse
import traceback
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
from src.counterparties.ubs import ubs_cash, ubs_collateral

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

    out_map = run_all_in_parallel(start_date=start_date, fundation=fundation, close_values=close_values, max_workers=6, timeout_per_task=None)

    # Example: concatenate all CASH
    cash_dfs = [df for name, df in out_map.items() if name.endswith("_cash")]
    
    if cash_dfs :

        cash_all = pl.concat(cash_dfs, how="vertical_relaxed")
        cash_all.write_excel("./history/cash_all.xlsx")

    # Example: concatenate all COLLATERAL
    collat_dfs = [df for name, df in out_map.items() if name.endswith("_collateral")]

    if collat_dfs :

        collat_all = pl.concat(collat_dfs, how="vertical_relaxed")
        collat_all.write_excel("./history/collateral_all.xlsx")

    # If you want to keep per-bank outputs:
    """
    for name, df in out_map.items() :
        df.write_excel(f"./raw/{name}.xlsx")
    out = ms_cash(start_date, fundation, close_values)
    out.write_excel("testt.xlsx")

    print(out)
    """


def _task_wrapper (name : str, fn, *args, **kwargs) -> Tuple[str, Optional[pl.DataFrame], Optional[Exception]]:
    """
    Runs a single task and always returns (name, df_or_none, exc_or_none).
    Never raises to the executor.
    """
    try :
        
        df = fn(*args, **kwargs)
        return name, df, None, None
    
    except Exception as e :
        tb = traceback.format_exc()
        return name, None, e, tb


def run_all_in_parallel (
        
        start_date : str | dt.datetime,
        fundation : str,
        close_values : Dict[str, float],
        *,
        max_workers : int = 8,
        timeout_per_task: Optional[float] = None,
    
    ) -> Dict[str, pl.DataFrame] :
    """
    Submit all cash/collateral functions concurrently.
    Returns a dict {task_name: DataFrame}.
    Failed tasks are skipped (but logged to stdout).
    """
    tasks : List[Tuple[str, Any, tuple, dict]] = [

        # CASH
        ("ms_cash", ms_cash, (start_date, fundation, close_values), {}),
        ("gs_cash", gs_cash, (start_date, fundation, close_values), {}),
        ("edb_cash", edb_cash, (start_date, fundation, close_values), {}),
        ("saxo_cash", saxo_cash, (start_date, fundation, close_values), {}),
        
        # COLLATERAL
        ("ms_collateral", ms_collateral, (start_date, fundation, close_values), {}),
        ("gs_collateral", gs_collateral, (start_date, fundation, close_values), {}),
        ("edb_collateral", edb_collateral, (start_date, fundation, close_values), {}),
        ("saxo_collateral", saxo_collateral, (start_date, fundation, close_values), {}),
    
    ]

    results: Dict[str, pl.DataFrame] = {}

    with ThreadPoolExecutor(max_workers=max_workers) as ex :

        future_map = {

            ex.submit(_task_wrapper, name, fn, *args, **kwargs): name
            for (name, fn, args, kwargs) in tasks
        
        }

        for fut in as_completed(future_map, timeout=timeout_per_task) :

            name = future_map[fut]
            
            try :
                task_name, df, err, tb = fut.result()
            
            except Exception as e :

                print(f"[!] {name} crashed at future level: {e}")
                traceback.print_exc()
                continue

            if err is not None :

                print(f"[-] {task_name} failed: {err}")
                print(tb)
                continue

            if df is None or (isinstance(df, pl.DataFrame) and df.is_empty()) :

                print(f"[·] {task_name}: empty or None")
                continue

            results[task_name] = df

    return results


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