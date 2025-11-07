from __future__ import annotations

import os
import datetime as dt
import polars as pl

from src.config import (FUNDATIONS, FREQUENCY_DATE_MAP, HISTORY_DIR_ABS_PATH,
    CACHE_DIR_ABS_PATH, ATTACH_DIR_ABS_PATH, RAW_DIR_ABS_PATH, KINDS_COLUMNS_DICT,
    CACHE_FILENAME_ABS, CACHE_COLUMNS
)

from typing import Optional, List, Dict


def date_to_str (date : Optional[str | dt.datetime] = None, format : str = "%Y-%m-%d") -> str :
    """
    Convert a date or datetime object to a string in "YYYY-MM-DD" format.

    Args:
        date (str | datetime): The input date.

    Returns:
        str: Date string in "YYYY-MM-DD" format.
    """
    if date is None:
        date_obj = dt.datetime.now()

    elif isinstance(date, dt.datetime):
        date_obj = date

    elif isinstance(date, dt.date):  # handles plain date (without time)
        date_obj = dt.datetime.combine(date, dt.time.min) # This will add 00 for the time

    elif isinstance(date, str) :

        try:
            date_obj = dt.datetime.strptime(date, format)

        except ValueError :
            
            try :
                date_obj = dt.datetime.fromisoformat(date)
            
            except ValueError :
                raise ValueError(f"Unrecognized date format: '{date}'")
    
    else :
        raise TypeError("date must be a string, datetime, or None")

    return date_obj.strftime(format)


def str_to_date (date : Optional[str | dt.datetime | dt.date] = None, format : str = "%Y-%m-%") -> str :
    """
    
    """
    return None


def get_full_name_fundation (fund : str, fundations : Optional[Dict] = None) -> Optional[str] :
    """
    
    """
    fundations = FUNDATIONS if fundations is None else fundations
    full_fund = fundations.get(fund, None)

    return full_fund


def convert_forex (
        
        ccys : Optional[List[str]] = None,
        amount : Optional[List[float]] = None,
        exchange : Optional[Dict[str, float]] = None
    
    ) -> Optional[List] :
    """
    
    """
    if ccys is None or amount is None:
        return None

    # Align lengths
    n_ccy, n_amt = len(ccys), len(amount)

    if n_ccy > n_amt :
        ccys = ccys[:n_amt]

    elif n_amt > n_ccy :
        ccys = ccys + ["EUR"] * (n_amt - n_ccy)

    # Build FX map from PAIRS like 'EURUSD=X'
    out: List[Optional[float]] = []

    for ccy, amt in zip(ccys, amount) :

        c = (ccy or "EUR").upper()
        
        if c == "EUR" :
            out.append(float(amt) if amt is not None else None)

        else :

            rate = exchange.get(c)
            out.append((float(amt) / rate) if (amt is not None and rate) else None)
    
    return out


def generate_dates (
        
        start_date : Optional[str | dt.datetime] = None,
        end_date : Optional[str | dt.datetime] = None,
        frequency : str = "Day",
        frequency_map : Optional[Dict] = None,
        format : str = "%Y-%m-%d"
    
    ) -> Optional[List]:
    """
    Function that returns a list of dates based on the start date, end date and frequency

    Args:
        start_date (str): start date in format 'YYYY-MM-DD'
        end_date (str): end date in format 'YYYY-MM-DD'
        frequency (str): 'Day', 'Week', 'Month', 'Quarter', 'Year' represents the frequency of the equity curve
        
    Returns:
        list: list of dates in format 'YYYY-MM-DD' or None
    """
    start_date = date_to_str(start_date)
    end_date = date_to_str(end_date)

    start_date = dt.datetime.strptime(start_date, format)
    end_date = dt.datetime.strptime(end_date, format)

    frequency_map = FREQUENCY_DATE_MAP if frequency_map is None else frequency_map
    interval = frequency_map.get(frequency)

    if interval is None :

        print(f"[-] Invalid frequency: {frequency}. Choose from 'Day', 'Week', 'Month', 'Quarter', 'Year'.")
        return None

    # This return a Series
    try :
        series_dates = pl.date_range(start_date, end_date, interval=interval, eager=True)

    except Exception as e :
        
        print(f"[-] Error generating dates: {e}")
        return None

    if series_dates.len() == 0 :

        print("[-] Error during generation: empty range (check start & end).")
        return None
    
    # Filter out weekends for non-business day frequencies
    series_dates_wd = series_dates.filter(series_dates.dt.weekday() <= 5)
    
    if series_dates_wd.len() == 0 :

        print("[*] No week day in the generated list after filter. Returning an empty List")
        return []

    # Convert the date range to a list of strings in the format 'YYYY-MM-DD'
    range_date_list = (
        
        series_dates_wd
            .to_frame("dates")
            .with_columns(pl.col("dates").dt.strftime(format).alias("formatted_dates"))["formatted_dates"]
            .to_list()
    
    )

    return range_date_list


def ensure_dirs () -> None :

    for p in [
        HISTORY_DIR_ABS_PATH, CACHE_DIR_ABS_PATH,
        ATTACH_DIR_ABS_PATH, RAW_DIR_ABS_PATH
    ] :
        os.makedirs(p, exist_ok=True)


def history_path (
    
        fundation : str = "HV",
        kind : str = "cash",
        history_dir_abs : Optional[str] = None
    
    ) -> str:
    # global per-fund file (your spec)
    # history/HV/cash.xlsx
    history_dir_abs = HISTORY_DIR_ABS_PATH if history_dir_abs is None else history_dir_abs
    
    fund_dir = os.join(history_dir_abs, fundation.upper())
    os.makedirs(fund_dir)

    return os.path.join(fund_dir, f"{kind}.xlsx")


def load_history (
        
        fundation : str = "HV",
        kind : str = "cash",

        kinds_dict : Optional[Dict] = None,
        history_dir_abs : Optional[str] = None
    
    ) -> Optional[pl.DataFrame] :
    """
    
    """
    history_dir_abs = HISTORY_DIR_ABS_PATH if history_dir_abs is None else history_dir_abs
    kinds_dict = KINDS_COLUMNS_DICT if kinds_dict is None else kinds_dict

    path = history_path(fundation, kind, history_dir_abs)
    columns = kinds_dict.get(kind)

    if not os.path.exists(path) :

        # minimal schema we expect: Date, Bank, ... (allow relaxed later)
        return pl.DataFrame(schema_overrides=columns)
    
    df = pl.read_excel(path, schema_overrides=columns)

    return df


def save_history (
        
        df : Optional[pl.DataFrame] = None, 
        fundation : str = "HV",
        kind : str = "cash"
    
    ) -> bool :
    """
    
    """
    # sort & drop dupes on (Date,Bank) if present
    if df is None :
        return False

    hst_df = load_history(fundation, kind)
    merged_df = hst_df.join(df)
    
    try :

        merged_df.write_excel(history_path(fundation, kind))
        return True
    
    except :
        return False


def slice_history (
        
        df : Optional[pl.DataFrame] = None,
        start : Optional[str] = None,
        end : Optional[str] = None
        
    ) -> pl.DataFrame :
    """
    
    """
    # handle Date as string ISO (YYYY-MM-DD) consistently
    if "Date" not in df.columns or df.is_empty() :
        return pl.DataFrame()
    
    start = str_to_date(start)
    end = str_to_date(end)

    df_filtered = df.filter((pl.col("Date") >= start) & (pl.col("Date") <= end))
    
    return df_filtered


def load_cache (

        cache_filename : Optional[str] = None,
        schema_overrides : Optional[Dict] = None,

    ) -> Optional[pl.DataFrame] :
    """
    
    """
    cache_filename = CACHE_FILENAME_ABS if cache_filename is None else cache_filename
    schema_overrides = CACHE_COLUMNS if schema_overrides is None else schema_overrides

    if not os.path.exists(cache_filename) :
        return pl.DataFrame(schema_overrides=schema_overrides)
    
    try :
        dataframe = pl.read_csv(cache_filename, schema_overrides=schema_overrides)
    
    except Exception :
        return None
    
    return dataframe


def save_cache (
        
        cache_df : Optional[pl.DataFrame] = None,
        cache_filename : Optional[str] = None,

    ) -> bool :
    """
    
    """
    if cache_df is None :
        return False
    
    cache_filename = CACHE_FILENAME_ABS if cache_filename is None else cache_filename
    full_df = load_cache(cache_filename)

    merged_df = full_df.join(cache_df)

    try :
        
        cache_df.write_csv(cache_filename)
        return True
    
    except :
        return False


def cache_lookup (
        
        cache_df : Optional[pl.DataFrame] = None,
        
        bank : Optional[str] = None,
        kind : str = "cash",
        fund : str = "HV",
        date : Optional[str | dt.datetime | dt.date] = None
    
    ) -> Optional[str] :
    """
    
    """
    if cache_df is None or cache_df.is_empty() :
        return None
    
    m = cache_df.filter(
    
        (pl.col("Bank") == bank) &
        (pl.col("Kind") == kind) &
        (pl.col("Fundation") == fund) &
        (pl.col("Date") == date)
    
    )

    if m.is_empty() :
        return None
    
    path = m.select("Filename").item()
    attch_path = os.join(ATTACH_DIR_ABS_PATH, m.select("Bank"))

    full_path = os.join(attch_path, path)

    return full_path if os.path.exists(full_path) else None


def cache_upsert (
        
        cache_df: Optional[pl.DataFrame] = None,
        bank : Optional[str] = None,
        kind : str = "cash",
        fund : str = "HV",
        date : Optional[str | dt.datetime | dt.date] = None,
        filepath : Optional[str] = None,
        
    ) -> pl.DataFrame :

    row = pl.DataFrame(

        {
            "Bank" : [bank],
            "Kind" : [kind],
            "Fundation" : [fund],
            "Date" : [date],
            "Filename" : [filepath],
        }

    )

    base = cache_df.filter(

        ~(
            (pl.col("Bank") == bank) &
            (pl.col("Kind") == kind) &
            (pl.col("Fundation") == fund) &
            (pl.col("date") == date)
        )

    )
    
    return pl.concat([base, row], how="vertical_relaxed")