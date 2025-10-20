from __future__ import annotations

import os
import polars as pl
import datetime as dt

from typing import Optional, Dict, List

from src.config import *
from src.utils import get_full_name_fundation, date_to_str, convert_forex
from src.api import call_api_for_pairs


def saxo_cash (
        
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : str = "HV",
        exchange : Optional[Dict[str, float]] = None,

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

    filename = get_file_by_fund_n_date(date, fundation)
    full_path = os.path.join(dir_abs_path, filename)

    df = get_df_from_file(full_path, date, fundation, schema_overrides)

    out = process_cash_by_fund(df, date, fundation, exchange=exchange)

    return out


def saxo_collateral (
        
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : str = "HV",
        exchange : Optional[Dict[str, float]] = None,

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

    filename = get_file_by_fund_n_date(date, fundation)
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

    print(dataframe)

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

    if dataframe is None or dataframe.is_empty() :
        return pl.DataFrame(schema_overrides=structure)

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
        "Net Exess/Deficit" : 0.0

    }


    df_out_dict["Total"] = (dataframe["TotalEquity"])
    df_out_dict["VM"] = dataframe["ValueDateCashBalance"] - df_out_dict.get("Total", 0.0)
    df_out_dict["Requirement"] = (dataframe["AccountFunding"])

    df_out_dict["Net Exess/Deficit"] = df_out_dict.get("Total", 0.0) - df_out_dict.get("IM", 0.0)

    """
    # Alternative ?

    amt_list = dataframe["TotalEquity"].to_list()
    date_cash_list = dataframe["ValueDateCashBalance"].to_list()
    fund_list = dataframe["AccountFunding"].to_list()

    amt_convert_list = convert_forex(ccy_list, amt_list, exchange)
    cash_bal_list = convert_forex(ccy_list, date_cash_list, exchange)
    acc_fund_list = convert_forex(ccy_list, fund_list, exchange)

    df_out_dict = {

        "Fundation" : full_fund,
        "Account" : dataframe["Account"].item(0), #.to_list(),
        "Date" : date,
        "Bank" : "Saxo Bank",
        "Currency" : "EUR",
        "Total" : 0.0, #"Total Collateral at Bank" : pl.Float64,
        "IM" : 0.0,
        "VM" : 0.0,
        "Requirement" : 0.0,
        "Net Exess/Deficit" : 0.0

    }


    df_out_dict["Total"] = sum(amt_convert_list)
    df_out_dict["VM"] = sum(cash_bal_list) - df_out_dict.get("Total", 0.0)
    df_out_dict["Requirement"] = sum(acc_fund_list)

    df_out_dict["Net Exess/Deficit"] = df_out_dict.get("Total", 0.0) - df_out_dict.get("IM", 0.0)
    
    """


    out = pl.DataFrame(

        df_out_dict,
        schema_overrides=structure

    )

    path = out.write_excel("test_saxo.xlsx")

    return out


# ---------------------- GENERAL FUNCTIONs ----------------------


def get_file_by_fund_n_date (
    
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : Optional[str] = "HV",

        d_format : str = "%d-%m-%Y",

        rules : Optional[str] = None,
        dir_abs_path : Optional[str] = None,
    
    ) -> Optional[str] :
    """
    This function looks for the path file by date and fundation (in the file name)
    """
    date = date_to_str(date, d_format)

    rules = SAXO_FILENAMES if rules is None else rules
    dir_abs_path = SAXO_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path

    full_fundation = get_full_name_fundation(fundation)
    
    for entry in os.listdir(dir_abs_path) :

        if date in entry and rules in entry :

            print(f"\n[+] File found for {date} and for {full_fundation}\n")
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
    dataframe.write_excel("testfullsaxo.xlsx")
    return dataframe

