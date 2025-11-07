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

ALL_BANKS = ["ms", "gs", "edb", "saxo", "ubs"]
OUT_DIR     = "./out"


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
        fundation : Optional[str] = None,
        kinds : Optional[str | List[str]] = None,

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
    missing_tasks: List[Tuple[str, str, str, str]] = []  # (date, fund, bank, kind)

    for fund in fundations:
        for kind in ALL_KINDS :
            # dates missing in history (based on 'Date' column existence)
            h = load_history(fund, kind)
            have_dates = set()
            if not h.is_empty() and "Date" in h.columns :
                have_dates = set(h.select("Date").to_series().to_list())

            for d in dates:

                if d not in have_dates:

                    for bank in ALL_BANKS:
                        missing_tasks.append((d, fund, bank, kind))

    # --- Use cache second: for each missing date×fund×bank×kind, see if we already have the local input file
    cache_df = load_cache()

    # keep tasks that truly need inputs (cache miss) so we know which dates to call the API for
    dates_needing_inputs: set[str] = set()
    really_missing: List[Tuple[str, str, str, str]] = []
    for (d, fund, bank, kind) in missing_tasks:
        hit = cache_lookup(cache_df, bank, kind, fund, d)
        if hit is None:
            dates_needing_inputs.add(d)
        really_missing.append((d, fund, bank, kind))

    # --- If needed, fetch inputs via API and update cache (per date)
    if dates_needing_inputs:
        for d in sorted(dates_needing_inputs):
            ensure_inputs_for_date(d, token, shared_emails, schema_df)
            # After download, you may update the cache by scanning ATTACH_DIR per bank and
            # recording files that match date/bank/kind/fund. If you already know filenames
            # at download time, you can push exact paths here. For now we skip adding rows
            # automatically; bank extractors typically read from ATTACH_DIR.

    # --- Parallel extraction for all really_missing tasks
    results_by_kind_fund: Dict[Tuple[str, str], List[pl.DataFrame]] = {}
    if really_missing:
        with ThreadPoolExecutor(max_workers=min(8, len(really_missing))) as ex:
            futures = [
                ex.submit(_task_wrapper, d, fund, bank, kind, close_values)
                for (d, fund, bank, kind) in really_missing
            ]
            for fut in as_completed(futures):
                d, fund, bank, kind, df, err, tb = fut.result()
                if err is not None:
                    print(f"[-] Extraction failed {(d, fund, bank, kind)}: {err}\n{tb}")
                    continue
                if df is None or df.is_empty():
                    print(f"[·] Empty result {(d, fund, bank, kind)}")
                    continue
                results_by_kind_fund.setdefault((fund, kind), []).append(df)

    # --- Merge into history and persist
    for fund in fundations:
        for kind in ALL_KINDS:
            fresh = pl.DataFrame()
            if (fund, kind) in results_by_kind_fund:
                fresh = pl.concat(results_by_kind_fund[(fund, kind)], how="vertical_relaxed")
            hist = load_history(fund, kind)
            merged = pl.concat([hist, fresh], how="vertical_relaxed") if not fresh.is_empty() else hist
            if not merged.is_empty():
                save_history(merged, fund, kind)

    # --- Finally, return the demanded results (slice again, now complete)
    for fund in fundations :
        for kind in ALL_KINDS:
            final_hist = load_history(fund, kind)
            slic = slice_history(final_hist, start_date, end_date)
            if not slic.is_empty():
                # “Merged results by bank” (grouping key can be (Bank) + agg of numerics)
                # We simply emit the sliced table; users can group in UI. If you want true
                # bank-level aggregates here, you can add .group_by("Bank").agg(...)
                out_path = os.path.join(OUT_DIR, f"{fund}_{kind}_{start_date}_to_{end_date}.xlsx")
                slic.write_excel(out_path)

    print("[✓] Done.")



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