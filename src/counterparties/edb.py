from __future__ import annotations

import os
import polars as pl
import datetime as dt

from typing import Optional, Dict, Tuple, List

from src.config import COUNTERPARTIES, EBD_ATTACHMENT_DIR_ABS_PATH, EDB_REQUIRED_COLUMNS, EDB_TYPE_ALLOWED
from src.utils import get_full_name_fundation, date_to_str



# ---------------------- Cash ----------------------



def get_file_by_fund_n_date (
    
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : Optional[str] = "HV",

        d_format : str = "%Y%m%d",
        f_format : str = "_",

        rules : Optional[Dict] = None,
        dir_abs_path : Optional[str] = None,
    
    ) -> Optional[str] :
    """
    This function looks for the path file by date and fundation (in the file name)
    """
    date = date_to_str(date, d_format)
    dir_abs_path = EBD_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path

    full_fundation = get_full_name_fundation(fundation)
    formatted_fund = edb_fundation_name_format(full_fundation, f_format)

    if formatted_fund is None :

        print(f"\n[-] Fundation not found. Retry with a correct fundation name...\n")
        return full_fundation
    
    for entry in os.listdir(dir_abs_path) :

        if date in entry and formatted_fund in entry :

            print(f"[+] File found for {date} and for {full_fundation}\n")
            return os.path.abspath(entry)
        
    return None



def process_cash_by_fund (
        
        dataframe : pl.DataFrame,
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : str = "HV",

        type_allowed : Optional[str] = None
    
    ) :
    """
    
    """
    date = date_to_str(date)
    full_fund = get_full_name_fundation(fundation)

    type_allowed = EDB_TYPE_ALLOWED if type_allowed is None else type_allowed

    if dataframe is None or dataframe.is_empty() :

        return pl.DataFrame()
    
    dataframe_cleaned = dataframe.filter(pl.col("TYPE").is_in(type_allowed) )
    return # TODO




"""
def process_cash_from_df (

        dataframe : pl.DataFrame,
        file : Optional[str] = None,
    
    ) :

"""



def get_df_from_file (
        
        file_abs_path : Optional[str] = None,
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : Optional[str] = "HV",
        schema_overrides : Optional[Dict] = None,

    ) -> pl.DataFrame :
    """
    
    """
    file_abs_path = get_file_by_fund_n_date(date, fundation) if file_abs_path is None else file_abs_path

    schema_overrides = EDB_REQUIRED_COLUMNS if schema_overrides is None else schema_overrides
    specific_cols = list(schema_overrides.keys())

    dataframe = pl.read_excel(file_abs_path, schema_overrides=schema_overrides, columns=specific_cols)

    return dataframe





def edb_fundation_name_format (fundation : str, format : str = "_") :
    """
    
    """
    if fundation is None :

        print(f"\n[-] Fundation is None. Retry with a correct fundation name...\n")
        return None

    strip_fundation = fundation.strip()
    formatted_fund = strip_fundation.replace(" ", format)

    return formatted_fund








def get_fundation_file_path (dataframe : pl.DataFrame, date : Optional[str | dt.datetime | dt.date] = None, fundation : str = "HV") :
    """
    This function will look for the specific file
    """
    date = date_to_str(date)

    if dataframe is None or dataframe.is_empty :    
    
        print(f"\n[!] No information at {date}\n")
        return None

    full_fundation = get_full_name_fundation(fundation)

    if full_fundation is None :

        print(f"\n[-] Fundation not found. Retry with a correct fundation name...\n")
        return full_fundation
    
    for row in dataframe.to_dict :

        return # TODO
    
    return # TODO








def get_cash (dataframe : pl.DataFrame, md5 : Optional[str] = None, rules : Optional[Dict] = None) :
    """
    This function will return the cas for both fundations (HV, WR)
    """
    rules = COUNTERPARTIES if rules is None else rules
    df_dicts = dataframe.to_dicts()

    for row in df_dicts :
        return None
    
    return None


def get_cash_hv (dataframe : pl.DataFrame, md5 : Optional[str] = None, rules : Optional[Dict] = None) :
    """
    
    """
    return None



# ---------------------- Collateral ----------------------


def get_collateral (dataframe : pl.DataFrame, md5 : str) :
    """
    
    """
    return None

