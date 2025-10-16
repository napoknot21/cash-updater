from __future__ import annotations

import datetime as dt
import polars as pl

from src.config import FUNDATIONS, FREQUENCY_DATE_MAP
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
        
        self,
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
    range_date_list = (series_dates_wd
        .to_frame("dates")
        .with_columns(pl.col("dates").dt.strftime(format).alias("formatted_dates"))["formatted_dates"]
        .to_list()
    )

    return range_date_list

