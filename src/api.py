from __future__ import annotations

import pandas as pd # type: ignore
import polars as pl
import yfinance as yf
import datetime as dt

from typing import Optional, List, Dict

from src.config import PAIRS
from src.utils import date_to_str


def call_api_for_pairs (
    
        target_date : Optional[str | dt.datetime] = None,
        pairs : Optional[List[str]] = None,
        loopback : int = 3
    
    ) -> Optional[Dict[str, float]] :
    """
    
    """
    if loopback == 0 :

        print("\n[-] YFinance API error. Reload the script")
        return None

    pairs = PAIRS if pairs is None else pairs
    target_date = date_to_str(target_date)

    conversion = yf.download(tickers=pairs, start=target_date, progress=False, threads=True, auto_adjust=False)

    #target_date = pd.to_datetime(target_date)

    conversion.index = pd.to_datetime(conversion.index)

    if target_date in conversion.index :
        row = conversion.loc[target_date]
        
    else :
        row = conversion.loc[conversion.index.get_indexer([target_date], method="nearest")][0]

    close_values = row["Close"].to_dict()

    if check_nan_into_values(target_date, pairs, close_values) :

        print("\n[!] Missing value for conversion. Retrying...")
        return call_api_for_pairs(target_date, pairs, loopback - 1)

    else :

        print(f"\n[+] Close values at {target_date} :")
        return close_values


def check_nan_into_values (
        
        target_date : Optional[str | dt.datetime] = None,
        pairs : Optional[List[str]] = None,
        conversion : Optional[dict[str, float]] = None
    
    ) -> bool :
    """
    
    """
    conversion = call_api_for_pairs(target_date, pairs) if conversion is None else conversion

    for v in conversion.values() :

        if pd.isna(v) :
            return True

    return False


