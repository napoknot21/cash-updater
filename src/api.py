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

    conversion.index = pd.to_datetime(conversion.index)

    if target_date in conversion.index :
        row = conversion.loc[target_date]
        
    else :
        row = conversion.loc[conversion.index.get_indexer([target_date], method="nearest")][0]

    close_values = row["Close"].to_dict()

    if check_nan_into_values(target_date, pairs, close_values) :

        print("\n[!] Missing value for conversion. Retrying...")
        return call_api_for_pairs(target_date, pairs, loopback - 1)

    print(f"\n[+] Close values at {target_date} :")
    return normalize_fx_dict(close_values)


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


def normalize_fx_dict (raw_fx : Optional[Dict[str, float]] = None, ends_with : str = "-X", start_with = "EUR") -> Optional[Dict[str, float]] :
    """
    Normalize Yahoo Finance FX tickers into { 'USD': 1.10, 'CHF': 0.95, ... }
    Meaning: each value is the amount of that currency per 1 EUR.

    Examples:
        {'EURUSD=X': 1.1, 'EURCHF=X': 0.95} → {'USD': 1.1, 'CHF': 0.95, 'EUR': 1.0}
    """
    normalized : Dict[str, float] = {"EUR": 1.0}

    for pair, val in raw_fx.items() :

        if pd.isna(val) :
            # Normally never in this case.
            continue

        name = str(pair).upper()

        if name.endswith(ends_with) :
            name = name[:-2]  # remove trailing =X

        if name.startswith(start_with) and len(name) >= 6 :

            ccy = name[3:6]
            normalized[ccy] = float(val)

    return normalized



