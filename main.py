from __future__ import annotations

import os
import argparse
import traceback
import yfinance as yf
import datetime as dt
import polars as pl
import pandas as pd

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple, Any

from src.config import (
    SHARED_MAILS, PAIRS, EMAIL_COLUMNS, RAW_DIR_ABS_PATH,
    ATTACH_DIR_ABS_PATH, ALL_FUNDATIONS, ALL_KINDS, CASH_COLUMNS, COLLATERAL_COLUMNS,
    HISTORY_DIR_ABS_PATH
)
from src.extraction import split_by_counterparty
from src.msla import *
from src.api import call_api_for_pairs
from src.utils import *

from src.counterparties.edb import edb_cash, edb_collateral
from src.counterparties.saxo import saxo_cash, saxo_collateral
from src.counterparties.gs import gs_cash, gs_collateral
from src.counterparties.ms import ms_cash, ms_collateral
#from src.counterparties.ubs import ubs_cash, ubs_collateral


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

    fund_df : Dict[str, Dict[str, pl.DataFrame]] = {

        f : {k: pl.DataFrame() for k in kinds} for f in fundations

    }

    # Let's check if all information it's from history or not
    all_from_history = True

    for fund in fundations :

        for kind in kinds :

            h = load_history(fund, kind)
            if h is None :
                print("No history")
                break

            slice_df = slice_history(h, start_date, end_date)

            if slice_df.is_empty() :
                all_from_history = False
            
            #sliced_results[(fund, kind)] = slice_df
            fund_df[fund][kind] = pl.concat([fund_df[fund][kind], slice_df], how="vertical")
            print(fund_df[fund][kind])
    
    if all_from_history :
        return fund_df # Already ready to use / export
    
    return None

def main(start_date: Optional[str | dt.datetime] = None,
         end_date: Optional[str | dt.datetime] = None,
         token: Optional[str] = None,
         fundation: Optional[str] = None,
         kinds: Optional[str | List[str]] = None,
         shared_emails: Optional[List[str]] = None,
         pairs: Optional[List[str]] = None,
         schema_df: Optional[Dict] = None) -> None:
    """
    Main entry point
    """
    start_date = date_to_str(start_date)
    end_date = date_to_str(end_date)

    pairs = PAIRS if pairs is None else pairs
    fundations = ALL_FUNDATIONS if fundation is None else [fundation]

    dates: List[str] = generate_dates(start_date=start_date, end_date=end_date)

    if not dates:
        print(f"\n[-] Error during date range generation.")
        return None

    # FX/close values once (you can also refresh per day if needed)
    close_values = call_api_for_pairs(None, pairs)
    print(f"\n[*] FX close values: {close_values}")

    # Optionally prefetch inputs (mail/attachments)
    # token = get_token() if token is None else token
    # shared_emails = SHARED_MAILS if shared_emails is None else shared_emails
    # schema_df = EMAIL_COLUMNS if schema_df is None else schema_df
    #for d in dates:
    #    ensure_inputs_for_date(d, token, shared_emails, schema_df)

    # Kinds filter: None -> both cash & collateral; else normalize to a set

    #""" 
    if kinds is None:
        kinds_filter = None
    elif isinstance(kinds, str):
        kinds_filter = {kinds.lower()}
    else:
        kinds_filter = {k.lower() for k in kinds}

    for d in dates:
        for f in fundations:
            print(f"\n[+] Processing date = {d} fund = {f} ...")
            process_one_day_fund(d, f, close_values, kinds_filter=kinds_filter, max_workers=8)
    #"""
    #df = edb_cash("2025-11-11", "HV", close_values)
    #print(df)
    
    print("\n[-] Done.")


def compute_and_update_history () :
    """
    
    """



def _safe_exec(task_name: str, fn: Any, *args, **kwargs) -> tuple[str, Optional[pl.DataFrame], Optional[BaseException], Optional[str]]:
    try:
        df = fn(*args, **kwargs)
        return task_name, df, None, None
    except Exception as e:
        return task_name, None, e, traceback.format_exc()



def build_tasks_for(date: str, fundation: str, close_values: Dict[str, float],
                    kinds_filter: Optional[set[str]] = None) -> List[Tuple[str, Any, tuple, dict]]:
    """
    Create tasks from BANK_FN for a given (date, fundation).
    kinds_filter: e.g. {'cash','collateral'}; None = all.
    """
    kinds_filter = kinds_filter or {"cash", "collateral"}
    tasks: List[Tuple[str, Any, tuple, dict]] = []

    for (bank, kind), fn in BANK_FN.items():
        if kind not in kinds_filter:
            continue
        task_name = f"{bank}_{kind}"
        # Assuming uniform signature (date, fundation, close_values)
        tasks.append((task_name, fn, (date, fundation, close_values), {}))

    return tasks


def run_all_in_parallel(date: str,
                        fundation: str,
                        close_values: Dict[str, float],
                        *,
                        kinds_filter: Optional[set[str]] = None,
                        max_workers: int = 8,
                        timeout_per_task: Optional[float] = None) -> Dict[str, pl.DataFrame]:
    """
    Submit all cash/collateral functions concurrently for one (date, fundation).
    Returns {task_name: DataFrame}
    """
    tasks = build_tasks_for(date, fundation, close_values, kinds_filter)
    results: Dict[str, pl.DataFrame] = {}

    if not tasks:
        return results

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_map = {
            ex.submit(_safe_exec, name, fn, *args, **kwargs): name
            for (name, fn, args, kwargs) in tasks
        }

        for fut in as_completed(future_map, timeout=timeout_per_task):
            name = future_map[fut]
            try:
                task_name, df, err, tb = fut.result()
            except Exception as e:
                print(f"[!] {name} crashed at future level: {e}")
                traceback.print_exc()
                continue

            if err is not None:
                print(f"[-] {task_name} failed: {err}")
                if tb:
                    print(tb)
                continue

            if df is None or (isinstance(df, pl.DataFrame) and df.is_empty()):
                print(f"[·] {task_name}: empty or None")
                continue

            results[task_name] = df

    return results




def _filename_for_kind(kind: str) -> str:
    return f"{kind}.xlsx"  # => cash.xlsx / collateral.xlsx


def _schema_for_kind(kind: str) -> Dict[str, Any]:
    """
    Return the appropriate schema dict for Polars read/write,
    based on whether we're dealing with cash or collateral.
    """
    if kind == "cash":
        return CASH_COLUMNS
    elif kind == "collateral":
        return COLLATERAL_COLUMNS
    else:
        raise ValueError(f"Unknown kind: {kind}")


def _read_history(fund: str, kind: str) -> pl.DataFrame:
    os.makedirs(os.path.join(HISTORY_DIR_ABS_PATH, fund.upper()), exist_ok=True)
    path = os.path.join(HISTORY_DIR_ABS_PATH, fund.upper(), _filename_for_kind(kind))
    schema = _schema_for_kind(kind)
    if os.path.exists(path):
        try:
            return pl.read_excel(path, schema_overrides=schema)
        except Exception as e:
            print(f"[-] Failed to read history {path}: {e}")
            return pl.DataFrame(schema=schema)
    return pl.DataFrame(schema=schema)


def _write_history(fund: str, kind: str, df: pl.DataFrame) -> None:
    path = os.path.join(HISTORY_DIR_ABS_PATH, fund.upper(), _filename_for_kind(kind))
    if not os.path.exists(path) :
        """
        
        """
        parent = os.path.dirname(path)
        os.makedirs(parent, exist_ok=True)
    try:
        # exact-duplicate drop; keep order if available
        df = df.unique(maintain_order=True)
        df.write_excel(path)
    except Exception as e:
        print(f"[-] Failed writing {path}: {e}")

def process_one_day_fund(date: str,
                         fundation: str,
                         close_values: Dict[str, float],
                         kinds_filter: Optional[set[str]] = None,
                         *,
                         max_workers: int = 8) -> None:
    """
    Runs all bank functions for one (date, fundation), groups by kind, and updates history files.
    """
    results = run_all_in_parallel(
        date=date,
        fundation=fundation,
        close_values=close_values,
        kinds_filter=kinds_filter,
        max_workers=max_workers,
    )

    # Group dataframes by kind
    grouped: Dict[str, List[pl.DataFrame]] = {"cash": [], "collateral": []}
    for task_name, df in results.items():
        # task_name is like "gs_cash" -> extract kind suffix
        kind = "cash" if task_name.endswith("_cash") else "collateral" if task_name.endswith("_collateral") else None
        if kind is None:
            print(f"[!] Could not infer kind from task name '{task_name}', skipping.")
            continue
        grouped[kind].append(df)

    for kind, dfs in grouped.items():
        if not dfs:
            continue
        new_block = pl.concat(dfs, how="vertical_relaxed")

        # Read existing history and append
        history = _read_history(fundation, kind)
        if history.is_empty():
            merged = new_block
        else:
            # Relaxed concat to accommodate minor schema diffs
            merged = pl.concat([history, new_block], how="vertical_relaxed")

        _write_history(fundation, kind, merged)



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
        fundation=args.fund
    
    )