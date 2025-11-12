from __future__ import annotations

import os
import polars as pl
import pandas as pd
import datetime as dt

from openpyxl import load_workbook
from typing import Dict, Optional, Tuple

from src.config import *
from src.utils import date_to_str


def ubs_cash (
        
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : Optional[str] = "HV",
        exchange : Optional[Dict[str, float]] = None,

        filename : Optional[str] = None,
        dir_abs_path : Optional[str] = None,
        schema_overrides : Optional[Dict] = None,

        rules : Optional[str] = None,
        cash_columns : Optional[Dict] = None

    ) -> Optional[str] :
    """
    
    """
    cash_columns = CASH_COLUMNS if cash_columns is None else cash_columns

    if fundation == "WR" :
        return pl.DataFrame(schema=cash_columns)
    
    dir_abs_path = UBS_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path
    schema_overrides = UBS_REQUIRED_COLUMNS if schema_overrides is None else schema_overrides

    rules = UBS_FILENAMES_CASH if rules is None else rules

    filename = get_file_by_fund_n_date(date, fundation, rules=rules) if filename is None else filename

    if filename is None :
        return pl.DataFrame()
    
    full_path = os.path.join(dir_abs_path, filename)

    #df = get_df_from_file_cash(full_path, date, fundation, schema_overrides)

    #out = process_cash_by_fund(df, date, fundation, exchange=exchange)

    return None


def ubs_collateral (
        
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : Optional[str] = "HV",
        
        exchange : Optional[Dict[str, float]] = None,

        filename : Optional[str] = None,
        dir_abs_path : Optional[str] = None,
        schema_overrides : Optional[Dict] = None,

        rules : Optional[str] = None,
        collat_columns : Optional[Dict] = None,

    ) -> Optional[pl.DataFrame] :
    """
    
    """
    collat_columns = COLLATERAL_COLUMNS if collat_columns is None else collat_columns

    if fundation == "WR" :
        return pl.DataFrame(schema=collat_columns)

    dir_abs_path = UBS_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path
    schema_overrides = USB_TARGET_FIELDS if schema_overrides is None else schema_overrides

    rules = UBS_FILENAMES_COLLATERAL if rules is None else rules

    filename = get_file_by_fund_n_date(date, fundation, rules=rules) if filename is None else filename
    full_path = os.path.join(dir_abs_path, filename)

    return None


# ---------------------- GENERAL FUNCTIONs ----------------------


def get_file_by_fund_n_date (
    
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : Optional[str] = "HV",

        d_format : str = "%b %d, %Y",

        rules : Optional[str] = None,
        dir_abs_path : Optional[str] = None,

        extensions : Tuple[str, str] = (".xls", ".xlsx")
    
    ) -> Optional[str] :
    """
    This function looks for the path file by date and fundation (in the file name)
    """
    date = date_to_str(date, d_format)
    print(date)
    rules = UBS_FILENAMES_CASH if rules is None else rules
    dir_abs_path = UBS_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path

    full_fundation = get_full_name_fundation(fundation)

    for entry in os.listdir(dir_abs_path) :

        if entry.lower().endswith(extensions) and rules in entry :

            full_path = os.path.join(dir_abs_path, entry)
            
            out = pl.read_excel(full_path, engine="calamine")

            #print(out)

            if date in entry and rules in entry :

                print(f"\n[+] File found for {date} and for {full_fundation}")
                return entry
            
    return None





def get_full_name_fundation (fund : str, fundations : Optional[Dict] = None) -> Optional[str] :
    """
    
    """
    fundations = FUNDATIONS if fundations is None else fundations
    full_fund = fundations.get(fund, None)

    return full_fund