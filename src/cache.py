from __future__ import annotations

import os
import polars as pl
import datetime as dt

from typing import Optional, List, Dict

from src.counterparties.edb import get_file_by_fund_n_date as get_edb_file_by_fund_n_date
from src.counterparties.gs import get_file_by_fund_n_date as get_gs_file_by_fund_n_date
from src.counterparties.ms import get_file_by_fund_n_date as get_ms_file_by_fund_n_date
from src.counterparties.saxo import get_file_by_fund_n_date as get_saxo_file_by_fund_n_dat
from src.counterparties.ubs import (get_file_by_fund_n_date_cash, get_file_by_fund_n_date_collat)

from src.config import FUNDATIONS
from src.config import CACHE_FILENAME_ABS, CACHE_COLUMNS

from src.utils import str_to_date


BANK_KIND_GETTER = {

    "EDB": {
        "cash": get_edb_file_by_fund_n_date,
        "collateral": get_edb_file_by_fund_n_date,
    },

    "GS": {
        "cash": get_gs_file_by_fund_n_date,
        "collateral": get_gs_file_by_fund_n_date,
    },

    "MS": {
        "cash": get_ms_file_by_fund_n_date,
        "collateral": get_ms_file_by_fund_n_date,
    },

    "SAXO": {
        "cash": get_saxo_file_by_fund_n_dat,
        "collateral": get_saxo_file_by_fund_n_dat,
    },

    "UBS": {
        "cash": get_file_by_fund_n_date_cash,
        "collateral": get_file_by_fund_n_date_collat,
    },

}


def load_cache (
        
        file_abs_path : Optional[str] = None,
        schema_overrides : Optional[Dict] = None

    ) :
    """
    
    """
    file_abs_path = CACHE_FILENAME_ABS if file_abs_path is None else file_abs_path
    schema_overrides = CACHE_COLUMNS if schema_overrides is None else schema_overrides
    columns = list(schema_overrides.keys())

    # Check if file exists, create it otherwise
    if not os.path.exists(file_abs_path) :

        dir_abs_path = os.path.dirname(file_abs_path)
        os.makedirs(dir_abs_path, exist_ok=True)

        dataframe = pl.DataFrame(schema=schema_overrides)
        dataframe.write_csv(file_abs_path)

        return dataframe


    dataframe = pl.read_csv(file_abs_path, schema_overrides=schema_overrides)
    print(f"\n[+] Cache load successfully :\n{dataframe}")

    return dataframe


def get_cache (
        
        dataframe : Optional[pl.DataFrame] = None,
        
        bank : Optional[str] = None,
        fundation : str = "HV",
        kind : str = "cash",
        date : Optional[str | dt.datetime | dt.date] = None
    
    ) :
    """
    
    """
    dataframe = load_cache() if dataframe is None else dataframe
    date = str_to_date(date)

    if dataframe is None or dataframe.is_empty() :
        
        print(f"\n[-] Cache file is empty. Continuining...")
        return None
    
    filters = []

    if date is not None :
        filters.append(pl.col("Date") == date)

    if bank is not None :
        filters.append(pl.col("Bank") == bank)

    if fundation is not None :
        filters.append(pl.col("Fundation") == fundation)
    
    if kind is not None :
        filters.append(pl.col("Kind") == kind)

    if not filters :

        print("\n[-] No filter provided for cache lookup.")
        return None

    df_match = dataframe.filter(pl.all_horizontal(filters))

    if df_match.is_empty() :

        print("\n[-] No matching entry found in cache.")
        return None
    
    # If many matches
    filename = df_match.select("Filename").to_series().to_list()[0]

    return filename



def update_cache (
        
        date: Optional[str | dt.date | dt.datetime] = None,
        fundations: Optional[List[str]] = None,
        
        banks: Optional[List[str]] = None,
        kinds: Optional[List[str]] = None,
        
        file_abs_path: Optional[str] = None,
    
    ) -> pl.DataFrame :
    """
    
    """
    file_abs_path = CACHE_FILENAME_ABS if file_abs_path is None else file_abs_path

    date = str_to_date(date)
    fundations = list(FUNDATIONS.keys()) if fundations is None else fundations
    banks = list(BANK_KIND_GETTER.keys()) if banks is None else banks
    kinds = ["cash", "collat"] if kinds is None else kinds

    # Charger le cache existant
    cache_df = load_cache(file_abs_path=file_abs_path)

    new_rows = []

    for fund in fundations :

        for bank in banks :

            # Si la bank n'a pas de mapping, on skip
            kind_map = BANK_KIND_GETTER.get(bank)
            
            if kind_map is None :
                continue

            for kind in kinds :

                getter = kind_map.get(kind)
                
                if getter is None :
                    continue

                # Vérifier si déjà dans le cache
                existing = get_cache(
                
                    dataframe=cache_df,
                    bank=bank,
                    fundation=fund,
                    kind=kind,
                    date=date,
                
                )

                if existing is not None:
                    continue

                # Get the file from counterparty
                try :
                    filename = getter(date=date, fundation=fund)
                
                except Exception as e :

                    print(f"\n[-] Error when calling getter for {bank} / {fund} / {kind}: {e}")
                    continue

                if not filename :
                    
                    # Nothing found
                    print(f"\n[-] No file found for {bank} / {fund} / {kind} on {date}")
                    continue

                # Add new line
                new_rows.append(
                
                    {
                        "Date": date,
                        "Bank": bank,
                        "Fundation": fund,
                        "Kind": kind,
                        "Filename": filename,
                    }
                
                )

    if new_rows :
            
        new_df = pl.DataFrame(new_rows)
        cache_df = pl.concat([cache_df, new_df], how="vertical")

        cache_df.write_csv(file_abs_path)
        print(f"\n[+] Cache updated with {len(new_rows)} new rows.")

    else :
        print("\n[+] Cache already up to date. No new rows added.")

    return cache_df