from __future__ import annotations

import os
import argparse
import traceback
import yfinance as yf
import datetime as dt
import polars as pl
import pandas as pd

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

from src.config import SHARED_MAILS, PAIRS, EMAIL_COLUMNS, RAW_DIR_ABS_PATH, ATTACH_DIR_ABS_PATH, ALL_FUNDATIONS, ALL_KINDS
from src.extraction import split_by_counterparty
from src.msla import *
from src.api import call_api_for_pairs
from src.utils import *

from src.counterparties.edb import edb_cash, edb_collateral
from src.counterparties.saxo import saxo_cash, saxo_collateral
from src.counterparties.gs import gs_cash, gs_collateral
from src.counterparties.ms import ms_cash, ms_collateral
from src.counterparties.ubs import ubs_cash, ubs_collateral


BANK_FN : Dict[Tuple[str, str], Any] = {

    # cash
    ("ms", "cash") : ms_cash,
    ("gs", "cash") : gs_cash,
    ("edb", "cash") : edb_cash,
    ("saxo", "cash") : saxo_cash,
    #("ubs", "cash") : ubs_cash,
    
    # collateral
    ("ms", "collateral") : ms_collateral,
    ("gs", "collateral") : gs_collateral,
    ("edb", "collateral") : edb_collateral,
    ("saxo", "collateral") : saxo_collateral,
    #("ubs", "collateral") : ubs_collateral,  # keep only if implemented

}


def ensure_inputs_for_date (
        
        date : Optional[str | dt.datetime | dt.date] = None,
        token : Optional[str] = None,
        
        shared_emails : Optional[List[str]] = None,
        schema_df : Optional[Dict[str, Any]] = None,

        raw_dir_abs : Optional[str] = None,
        attch_dir_abs : Optional[str] = None,
    
    ) -> None :
    """
    Only used when cache misses occur and we need to guarantee the local inputs.
    Idempotent. Downloads attachments and updates ./attachments/{BANK}/...
    We also dump mailbox rows into ./raw/{bank}_{date}.xlsx (optional).
    """
    token = get_token() if token is None else token
    shared_emails = SHARED_MAILS if shared_emails is None else shared_emails
    schema_df = EMAIL_COLUMNS if schema_df is None else schema_df

    raw_dir_abs = RAW_DIR_ABS_PATH if raw_dir_abs is None else raw_dir_abs
    attch_dir_abs = ATTACH_DIR_ABS_PATH if attch_dir_abs is None else attch_dir_abs

    try :

        inbox_df = pl.DataFrame(schema=schema_df)

        for email in shared_emails :

            try :

                df_email = get_inbox_messages_by_date(date=date, token=token, email=email, with_attach=True)
                
                if isinstance(df_email, pl.DataFrame) and not df_email.is_empty() :
                    inbox_df = pl.concat([inbox_df, df_email], how="vertical_relaxed")

            except Exception as e:
                print(f"\n[-] Inbox read error {email} {date}: {e}")

        if inbox_df.is_empty() :

            print(f"\n[-] No inbox data on {date}.")
            return

        rules_map = split_by_counterparty(inbox_df)

        # Here
        # k => counterparty name
        # v -> dataframe of k (filtered info)
        for counterparty, df_cp in rules_map.items() :

            if counterparty == "UNMATCHED" or df_cp.is_empty() :
                continue

            raw_out = os.path.join(raw_dir_abs, f"{counterparty.lower()}_{date}.xlsx")
            
            try :
                df_cp.write_excel(raw_out)

            except Exception as e :
                print(f"[-] Failed writing {raw_out}: {e}")

            for row in df_cp.to_dicts() :

                msg_id = row.get("Id")
                origin = row.get("Shared Email")

                if not msg_id :
                    continue

                try :

                    dest = os.path.join(attch_dir_abs, counterparty)
                    os.makedirs(dest, exist_ok=True)

                    download_attachments_for_message(msg_id, token, dest, origin)
                
                except Exception as e :
                    print(f"[-] Attachment download failed for {counterparty} {date}: {e}")

    except Exception as e :

        print(f"[-] ensure_inputs_for_date failed {date}: {e}")
        traceback.print_exc()

    return None


def look_inputs_from_history (
        
        start_date : Optional[str | dt.datetime | dt.date] = None,
        end_date : Optional[str | dt.datetime | dt.date] = None,

        fundations : Optional[List[str]] = None,
        kinds : Optional[List[str]] = None,

    ) :
    """
    
    """
    fundations = ALL_FUNDATIONS if fundations is None else fundations
    kinds = ALL_KINDS if kinds is None else kinds

    # History first check
    full_hit = True
    sliced_results : Dict[Tuple[str, str], pl.DataFrame] = {} # (fund, kind) -> slice

    for fund in fundations :

        for kind in kinds :

            h = load_history(fund, kind)
            slice_df = slice_history(h, start_date, end_date)

            if slice_df.is_empty() :
                full_hit = False

            sliced_results[(fund, kind)] = slice_df

    if full_hit is True :

        # Optional: merge per-bank across dates, then write a convenience output
        for fund in fundations :

            for kind in kinds:
            
                df = sliced_results[(fund, kind)]
                
                if not df.is_empty() :

                    out_path = os.path.join(OUT_DIR, f"{fund}_{kind}_{start_date}_to_{end_date}.xlsx")
                    df.write_excel(out_path)

        print("\n[+] Served entirely from history.")
        return



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
    
    token = get_token() if token is None else token
    shared_emails = SHARED_MAILS if shared_emails is None else shared_emails
    schema_df = EMAIL_COLUMNS if schema_df is None else schema_df
    pairs = PAIRS if pairs is None else pairs

    start_date = date_to_str(start_date)
    end_date = date_to_str(end_date)

    dates : List[str] = generate_dates(start_date=start_date, end_date=end_date)

    if dates is None or len(dates) == 0 :

        print(f"\n[-] Error during data range generation.")
        return None
    
    # Always check for the today's convertion rate
    close_values = call_api_for_pairs(None, pairs)
    print(f"\n[*] {close_values}")
    
    fundations = ALL_FUNDATIONS if fundation is None else [fundation]

    # First we cehck the history
    look_inputs_from_history(start_date, end_date, fundations, )
    
    # --- Otherwise, figure out which (date×fund×bank×kind) are missing from history

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
    #"""
    # If you want to keep per-bank outputs:
    #"""
    for name, df in out_map.items() :
        df.write_excel(f"./raw/{name}.xlsx")
    out = ms_cash(start_date, fundation, close_values)
    out.write_excel("testt.xlsx")

    print(out)
    #"""

    fundation = "HV"

    out = edb_cash(start_date, fundation, close_values)

    print(out)



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