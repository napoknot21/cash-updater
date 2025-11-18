from __future__ import annotations

import os
import argparse
import traceback
import datetime as dt
import polars as pl

from typing import Dict, List, Optional, Tuple, Any

from src.config import (
    SHARED_MAILS, PAIRS, EMAIL_COLUMNS, RAW_DIR_ABS_PATH,
    ATTACH_DIR_ABS_PATH, ALL_FUNDATIONS, ALL_KINDS,
    CASH_COLUMNS, COLLATERAL_COLUMNS, HISTORY_DIR_ABS_PATH
)
from src.history import load_history, save_history
from src.extraction import split_by_counterparty
from src.msla import get_token, get_inbox_messages_by_date, download_attachments_for_message
from src.api import call_api_for_pairs, load_cache_close_values
from src.utils import generate_dates, date_to_str, str_to_date, slice_history
from src.cache import load_cache, get_cache, update_cache

from src.counterparties.edb import edb_cash, edb_collateral
from src.counterparties.saxo import saxo_cash, saxo_collateral
from src.counterparties.gs import gs_cash, gs_collateral
from src.counterparties.ms import ms_cash, ms_collateral
from src.counterparties.ubs import ubs_cash, ubs_collateral


BANK_FN: Dict[Tuple[str, str], Any] = {

    # cash
    ("MS", "cash") : ms_cash,
    ("GS", "cash") : gs_cash,
    ("EDB", "cash") : edb_cash,
    ("SAXO", "cash") : saxo_cash,
    ("UBS", "cash") : ubs_cash,

    # collateral
    ("MS", "collateral") : ms_collateral,
    ("GS", "collateral") : gs_collateral,
    ("EDB", "collateral") : edb_collateral,
    ("SAXO", "collateral") : saxo_collateral,
    ("UBS", "collateral") : ubs_collateral,

}


def ensure_inputs_for_date (
        
        date: Optional[str | dt.datetime | dt.date] = None,
        token: Optional[str] = None,
        shared_emails: Optional[List[str]] = None,
        schema_df: Optional[Dict[str, Any]] = None,
        raw_dir_abs: Optional[str] = None,
        attch_dir_abs: Optional[str] = None,
    
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

                df_email = get_inbox_messages_by_date(
                    date=date,
                    token=token,
                    email=email,
                    with_attach=True,
                )
                
                if isinstance(df_email, pl.DataFrame) and not df_email.is_empty() :
                    inbox_df = pl.concat([inbox_df, df_email], how="vertical_relaxed")

            except Exception as e :
                print(f"\n[-] Inbox read error {email} {date}: {e}")

        if inbox_df.is_empty() :

            print(f"\n[-] No inbox data on {date}.")
            return

        rules_map = split_by_counterparty(inbox_df)

        # k => counterparty name, v => filtered DF
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

                    # avoid re-writing if same file already exists
                    download_attachments_for_message(msg_id, token, dest, origin)

                except Exception as e :
                    print(f"[-] Attachment download failed for {counterparty} {date}: {e}")

    except Exception as e :

        print(f"[-] ensure_inputs_for_date failed {date}: {e}")
        traceback.print_exc()

    return None


def get_or_build_filename (
        
        date: str | dt.date | dt.datetime,
        fundation: str,
        bank: str,              # "MS", "UBS", etc.
        kind: str,              # "cash" or "collateral"
        cache_df: pl.DataFrame,
        token: Optional[str],
        shared_emails: Optional[List[str]],
        schema_df: Optional[Dict],
        downloaded_dates: set[str],
    
    ) -> Tuple[Optional[str], pl.DataFrame]:
    """
    Full logic to retrieve or build the correct filename for a given 
    (date, fundation, bank, kind) combination.

    Steps:
    1) Try to read filename from cache.
    2) If not found, update_cache() to index existing local files.
    3) Try cache again.
    4) If still missing → download inputs (emails + attachments) once per date.
    5) Re-run update_cache() to index the newly downloaded files.
    6) Final cache lookup.
    """

    # Normalize date/kind/bank
    date_str = date_to_str(date)
    date_obj = str_to_date(date_str)

    kind = kind.lower()
    bank = bank.upper()

    # 1) First lookup in existing cache
    filename = get_cache(
        dataframe=cache_df,
        bank=bank,
        fundation=fundation,
        kind=kind,
        date=date_obj,
    )

    if filename is not None:
        # File already registered in cache → fast path
        return filename, cache_df

    print(f"\n[*] Cache miss for {date_str} / {fundation} / {bank} / {kind}.")
    print("\n[*] Trying update_cache (index existing local attachments)...")

    # 2) Try to index local files
    cache_df = update_cache(
        date=date_obj,
        fundations=[fundation],
        banks=[bank],
        kinds=[kind],
    )

    # 3) Try cache again
    filename = get_cache(
        dataframe=cache_df,
        bank=bank,
        fundation=fundation,
        kind=kind,
        date=date_obj,
    )

    if filename is not None:
        print("\n[*] Found file after update_cache().")
        return filename, cache_df

    print("\n[*] Still missing. Now downloading inputs...")

    # 4) Download from mailboxes if not already done for this date
    if date_str not in downloaded_dates:
        print(f"\n[*] No inputs downloaded yet for {date_str} → calling ensure_inputs_for_date.")
        """
        ensure_inputs_for_date(
            date=date_str,
            token=token,
            shared_emails=shared_emails,
            schema_df=schema_df,
        )
        downloaded_dates.add(date_str)
        """
    else:
        print(f"\n[*] Inputs already downloaded for {date_str}, skip download.")

    # 5) Re-index local files after download
    cache_df = update_cache(
        date=date_obj,
        fundations=[fundation],
        banks=[bank],
        kinds=[kind],
    )

    # 6) Final cache lookup
    filename = get_cache(
        dataframe=cache_df,
        bank=bank,
        fundation=fundation,
        kind=kind,
        date=date_obj,
    )

    if filename is None:
        print("\n[-] No file found even after download → skipping.")

    return filename, cache_df


def main (
    
        start_date: Optional[str | dt.datetime] = None,
        end_date: Optional[str | dt.datetime] = None,
        token: Optional[str] = None,
        fundation: Optional[str] = None,
        kinds: Optional[str | List[str]] = None,
        shared_emails: Optional[List[str]] = None,
        pairs: Optional[List[str]] = None,
        schema_df: Optional[Dict] = None,
        cache: Optional[pl.DataFrame] = None,
    
    ) -> None:
    """
    Main entry point
    """
    # Normalize dates
    start_date = date_to_str(start_date)
    end_date = date_to_str(end_date)

    # FX pairs & fundations
    pairs = PAIRS if pairs is None else pairs
    fundations = ALL_FUNDATIONS if fundation is None else [fundation]

    # Kinds filter
    if kinds is None :
        kinds_filter = {"cash", "collateral"}
    
    elif isinstance(kinds, str) :
        kinds_filter = {kinds.lower()}
    
    else :
        kinds_filter = {k.lower() for k in kinds}

    dates: List[str] = generate_dates(start_date=start_date, end_date=end_date)

    if not dates:

        print(f"\n[-] Error during date range generation.")
        return None

    print(dates)

    # FX/close values once (you can also refresh per day if needed)
    close_values = call_api_for_pairs(None, pairs)

    if close_values is None :
        close_values = load_cache_close_values()

    print(f"\n[*] FX close values: {close_values}")

    # Load cache of filenames (attachments index)
    cache_df = load_cache()

    # Set of dates for which we already called ensure_inputs_for_date
    downloaded_dates: set[str] = set()

    # --------- Load existing history ONCE for all dates ---------
    history: Dict[Tuple[str, str], pl.DataFrame] = {}
    existing_dates: Dict[Tuple[str, str], set[str]] = {}

    for f in fundations :

        for kind in kinds_filter:
        
            hist_df = load_history(f, kind)  # may be empty DF if no file yet
            history[(f, kind)] = hist_df

            if hist_df.is_empty() :
                existing_dates[(f, kind)] = set()
            
            else :

                existing_dates[(f, kind)] = set(
                    hist_df.get_column("Date").cast(pl.Date).unique().to_list()
                )

    # ----------------- Main processing loop -----------------
    for d in dates :

        for f in fundations :

            print(f"\n[*] Processing date {d} | fundation {f}")

            for kind in kinds_filter:
                key = (f, kind)

                # Safety: ensure keys exist
                if key not in history :

                    history[key] = pl.DataFrame()
                    existing_dates[key] = set()

                # Skip if this date already in history
                if d in existing_dates[key] :
                    print(f"\n[*] Date {d} already in history for {f}/{kind}, skipping computations.")
                    continue

                # Loop over banks for this kind
                for (bank, fn_kind), fn in BANK_FN.items():
                    if fn_kind != kind:
                        continue

                    filename, cache_df = get_or_build_filename(
                        date=d,
                        fundation=f,
                        bank=bank,
                        kind=kind,
                        cache_df=cache_df,
                        token=token,
                        shared_emails=shared_emails,
                        schema_df=schema_df,
                        downloaded_dates=downloaded_dates,
                    )

                    if filename is None :
                    
                        print(f"\n[-] No file for {bank}/{kind} on {d} / {f}, skipping.")
                        continue

                    print(f"\n[*] Using {filename} for {bank}/{kind} on {d}/{f}")

                    try:
                        # fn is e.g. gs_cash, gs_collateral, ms_cash, ...
                        df_out = fn(
                            date=d,
                            fundation=f,
                            exchange=close_values,
                            #filename=filename,
                        )

                    except Exception as e :

                        print(f"\n[-] Processing error for {bank}/{kind} on {d}/{f}: {e}")
                        continue

                    if df_out is None or df_out.is_empty():
                        continue
                    
                    # Append to in-memory history (robust even if history[key] is empty/no-schema)
                    if history[key] is None or history[key].is_empty() or history[key].width == 0 :
                        # First batch for this (fundation, kind) → just assign df_out
                        history[key] = df_out
                    
                    else :

                        # Already have some data → concat
                        history[key] = pl.concat(
                            [history[key], df_out],
                            how="vertical_relaxed",
                        )

                    existing_dates[key].add(d)

    # ----------------- Save history to disk -----------------
    for (fundation_name, kind), df_hist in history.items() :

        if df_hist is None or df_hist.is_empty() :
            continue

        if "Date" in df_hist.columns :
            df_hist = df_hist.sort("Date")

        print(f"\n[+] History updated for {fundation_name}/{kind} :")
        print(df_hist)

        save_history(fundation_name, kind, df_hist)

    print("\n[+] Done.")


if __name__ == '__main__':
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

    main(
        shared_emails=args.shared_emails,
        start_date=args.start_date,
        end_date=args.end_date,
        fundation=args.fund,
    )
