from __future__ import annotations

import os
import polars as pl
import datetime as dt

from typing import Optional, Dict, List

from src.config import *
from src.utils import get_full_name_fundation, date_to_str, convert_forex, cache_update, str_to_date, cache_load_row, load_cache
from src.api import call_api_for_pairs


def saxo_cash (
        
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : str = "HV",
        exchange : Optional[Dict[str, float]] = None,

        filename : Optional[str] = None,
        dir_abs_path : Optional[str] = None,
        schema_overrides : Optional[Dict] = None,

        cash_columns : Optional[Dict] = None

    ) -> Optional[pl.DataFrame] :
    """
    
    """
    cash_columns = CASH_COLUMNS if cash_columns is None else cash_columns

    if fundation == "WR" :
        return pl.DataFrame(schema=cash_columns)
    
    dir_abs_path = SAXO_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path
    schema_overrides = SAXO_REQUIRED_COLUMNS if schema_overrides is None else schema_overrides

    filename = get_file_by_fund_n_date(date, fundation) if filename is None else filename
    if filename is None :
        return pl.DataFrame()
    full_path = os.path.join(dir_abs_path, filename)

    df = get_df_from_file(full_path, date, fundation, schema_overrides)

    out = process_cash_by_fund(df, date, fundation, exchange=exchange)

    return out


def saxo_collateral (
        
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : str = "HV",
        exchange : Optional[Dict[str, float]] = None,

        filename : Optional[str] = None,
        dir_abs_path : Optional[str] = None,
        schema_overrides : Optional[Dict] = None,

        cash_columns : Optional[Dict] = None

    ) -> Optional[pl.DataFrame] :
    """
    
    """
    cash_columns = CASH_COLUMNS if cash_columns is None else cash_columns

    if fundation == "WR" :
        return pl.DataFrame(schema=cash_columns)
    
    dir_abs_path = SAXO_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path
    schema_overrides = SAXO_REQUIRED_COLUMNS if schema_overrides is None else schema_overrides

    filename = get_file_by_fund_n_date(date, fundation) if filename is None else filename
    full_path = os.path.join(dir_abs_path, filename)

    df = get_df_from_file(full_path, date, fundation, schema_overrides)

    out = process_collat_by_fund(df, date, fundation, exchange=exchange)

    return out




# ---------------------- CASH ----------------------


def process_cash_by_fund (
        
        dataframe : pl.DataFrame,

        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : str = "HV",

        exchange : Optional[Dict[str, float]] = None,
        structure : Optional[Dict] = None,
    
    ) -> Optional[pl.DataFrame] :
    """
    
    """
    date = date_to_str(date)
    full_fund = get_full_name_fundation(fundation)

    exchange = call_api_for_pairs(date) if exchange is None else exchange
    structure = CASH_COLUMNS if structure is None else structure

    if dataframe is None or dataframe.is_empty() :
        return pl.DataFrame(schema_overrides=structure)

    ccy_list = dataframe["AccountCurrency"].to_list()
    amt_list = dataframe["Balance"].to_list()

    amt_convert_list = convert_forex(ccy_list, amt_list, exchange)
    val_exchange = [exchange.get(c) or 1.0 for c in ccy_list]

    out = pl.DataFrame(

        {
            "Fundation" : full_fund,
            "Account" : dataframe["Account"].to_list(),
            "Date" : date,
            "Bank" : "Saxo Bank",
            "Type" : "Held",
            "Currency" : ccy_list,
            "Amount in CCY": amt_list,
            "Exchange": val_exchange,
            "Amount in EUR" : amt_convert_list 
        },
        schema_overrides=structure

    )

    return out


# ---------------------- COLLATERAL ----------------------


def process_collat_by_fund (
        
        dataframe : pl.DataFrame,

        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : str = "HV",

        type_allowed : Optional[str | List[str]] = None,
        desc_allowed : Optional[str | List[str]] = None,

        exchange : Optional[Dict[str, float]] = None,
        structure : Optional[Dict] = None,
    
    ) -> Optional[pl.DataFrame] :
    """
    
    """
    date = date_to_str(date)
    full_fund = get_full_name_fundation(fundation)

    exchange = call_api_for_pairs(date) if exchange is None else exchange
    structure = COLLATERAL_COLUMNS if structure is None else structure

    columns = list(structure.keys())

    if dataframe is None or dataframe.is_empty() :
        return pl.DataFrame(schema_overrides=structure, schema=columns)

    ccy_list = dataframe["AccountCurrency"].to_list()

    df_out_dict = {

        "Fundation" : full_fund,
        "Account" : dataframe["Account"].to_list(),
        "Date" : date,
        "Bank" : "Saxo Bank",
        "Currency" : ccy_list,
        "Total" : 0.0, #"Total Collateral at Bank" : pl.Float64,
        "IM" : 0.0,
        "VM" : 0.0,
        "Requirement" : 0.0,
        "Net Excess/Deficit" : 0.0

    }


    df_out_dict["Total"] = (dataframe["TotalEquity"])
    
    df_out_dict["VM"] = dataframe["ValueDateCashBalance"] - df_out_dict.get("Total", 0.0)
    
    df_out_dict["Requirement"] = df_out_dict["IM"] + df_out_dict["VM"]#(dataframe["AccountFunding"])

    df_out_dict["Net Excess/Deficit"] = df_out_dict["Total"] + df_out_dict["Requirement"]

    out = pl.DataFrame(

        df_out_dict,
        schema_overrides=structure

    )

    return out


# ---------------------- GENERAL FUNCTIONs ----------------------


def get_file_by_fund_n_date (
    
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : Optional[str] = "HV",

        d_format : str = "%d-%m-%Y",
        kind : Optional[str] = "cash",

        rules : Optional[str] = None,
        dir_abs_path : Optional[str] = None,
    
    ) -> Optional[str] :
    """
    This function looks for the path file by date and fundation (in the file name)
    """
    date_obj = str_to_date(date)
    date_format = date_to_str(date, d_format)

    df_cache = load_cache()
    df = cache_load_row(df_cache, "SAXO", kind, fundation, date_obj)

    if df.height > 0 :

        col_data = df.select("Filename").item()
        return col_data

    rules = SAXO_FILENAMES if rules is None else rules
    dir_abs_path = SAXO_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path

    full_fundation = get_full_name_fundation(fundation)

    for entry in os.listdir(dir_abs_path) :

        if date_format in entry and rules in entry :

            print(f"\n[+] [SAXO] File found for {date} and for {full_fundation}")
            #cache_update(df_cache, date_obj, "SAXO", fundation, kind, entry)
            
            return entry
        
    return None


def get_df_from_file (
        
        file_abs_path : Optional[str] = None,
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : Optional[str] = "HV",
        schema_overrides : Optional[Dict] = None,
        separator : str = ";",

    ) -> pl.DataFrame :
    """
    
    """
    file_abs_path = get_file_by_fund_n_date(date, fundation) if file_abs_path is None else file_abs_path

    schema_overrides = SAXO_REQUIRED_COLUMNS if schema_overrides is None else schema_overrides
    specific_cols = list(schema_overrides.keys())

    dataframe = pl.read_csv(file_abs_path, separator=separator, schema_overrides=schema_overrides,)

    return dataframe

