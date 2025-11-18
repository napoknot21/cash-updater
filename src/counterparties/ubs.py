from __future__ import annotations

import os
import warnings
import polars as pl
import pandas as pd
import datetime as dt

from python_calamine import CalamineWorkbook
from openpyxl import load_workbook
from typing import Dict, Optional, Tuple

from src.config import *
from src.utils import date_to_str, convert_forex
from src.api import call_api_for_pairs


def ubs_cash (
        
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : Optional[str] = "HV",
        exchange : Optional[Dict[str, float]] = None,

        filename : Optional[str] = None,
        dir_abs_path : Optional[str] = None,
        
        schema_overrides : Optional[Dict] = None,
        structure : Optional[Dict] = None,

        rules : Optional[str] = None,

    ) -> Optional[str] :
    """
    
    """
    structure = CASH_COLUMNS if structure is None else structure

    if fundation == "WR" :
        return pl.DataFrame(schema=structure)
    
    dir_abs_path = UBS_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path
    schema_overrides = UBS_REQUIRED_COLUMNS if schema_overrides is None else schema_overrides

    rules = UBS_FILENAMES_CASH if rules is None else rules

    filename = get_file_by_fund_n_date_cash(date, fundation, rules=rules) if filename is None else filename

    if filename is None :
        return pl.DataFrame(schema=structure)
    
    full_path = os.path.join(dir_abs_path, filename)

    df = get_df_from_file_cash(full_path, date, fundation, schema_overrides)

    out = process_cash_by_fund(df, date, fundation, exchange=exchange)

    return out


def ubs_collateral (
        
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : Optional[str] = "HV",
        
        exchange : Optional[Dict[str, float]] = None,

        filename : Optional[str] = None,
        dir_abs_path : Optional[str] = None,
        
        schema_overrides : Optional[Dict] = None,
        structure : Optional[Dict] = None,

        rules : Optional[str] = None,

    ) -> Optional[pl.DataFrame] :
    """
    
    """
    print("--------------------COLLATERAL USB =======================================")
    structure = COLLATERAL_COLUMNS if structure is None else structure

    if fundation == "WR" :
        return pl.DataFrame(schema=structure)

    dir_abs_path = UBS_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path
    schema_overrides = USB_TARGET_FIELDS if schema_overrides is None else schema_overrides

    rules = UBS_FILENAMES_COLLATERAL if rules is None else rules

    filename = get_file_by_fund_n_date_collat(date, fundation, rules=rules) if filename is None else filename

    if filename is None :
        return pl.DataFrame(schema=structure)
    
    full_path = os.path.join(dir_abs_path, filename)

    dataframe = get_df_from_file_collateral(full_path, date, fundation)

    out = process_collateral_by_fund(dataframe, date, fundation, exchange, structure)
    print(out)
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

    ccy_list = dataframe["CCY (Issue)"].to_list()
    amt_list = dataframe["Quantity"].to_list()

    amt_convert_list = convert_forex(ccy_list, amt_list, exchange)
    val_exchange = [exchange.get(c) or 1.0 for c in ccy_list]

    out = pl.DataFrame(

        {
            "Fundation" : full_fund,
            "Account" : dataframe["Cusip/ISIN"].to_list(),
            "Date" : date,
            "Bank" : "UBS AG",
            "Currency" : ccy_list,
            "Type" : "Held",
            "Amount in CCY": amt_list,
            "Exchange": val_exchange,
            "Amount in EUR" : amt_convert_list 
        },
        schema_overrides=structure

    )

    return out


# ---------------------- COLLATERAL ----------------------


def process_collateral_by_fund (
        
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

    ccy_list = dataframe["Currency"].to_list()
    
    amt_list = dataframe["Collateral Held by UBS"].to_list()
    im = dataframe["Client Initial Margin"].to_list()
    vm = dataframe["Mtm Value"].to_list()
    requirement = dataframe["Total Requirement"].to_list()
    net = dataframe["Net Excess/Deficit"].to_list()

    amt_convert_list = convert_forex(ccy_list, amt_list, exchange)
    im_convert_list = [-x for x in convert_forex(ccy_list, im)]
    vm_convert_list = [-x for x in convert_forex(ccy_list, vm)]

    req_convert_list = [-x for x in convert_forex(ccy_list, requirement)]
    net_convert_list = [-x for x in convert_forex(ccy_list, net)]

    #val_exchange = [exchange.get(c) or 1.0 for c in ccy_list]

    out = pl.DataFrame(

        {
            "Fundation" : full_fund,
            "Account" : "CASH-EUR",
            "Date" : date,
            "Bank" : "UBS AG",
            "Currency" : ccy_list,
            "Total" : amt_convert_list, #"Total Collateral at Bank" : pl.Float64,
            "IM" : im_convert_list,
            "VM" : vm_convert_list,
            "Requirement" : req_convert_list,
            "Net Excess/Deficit" : net_convert_list
        },
        schema_overrides=structure

    )

    return out


# ---------------------- GENERAL FUNCTIONs ----------------------


def get_file_by_fund_n_date_cash (
    
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

    rules = UBS_FILENAMES_CASH if rules is None else rules
    dir_abs_path = UBS_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path

    full_fundation = get_full_name_fundation(fundation)

    for entry in os.listdir(dir_abs_path) :

        if entry.lower().endswith(extensions) and rules in entry :

            full_path = os.path.join(dir_abs_path, entry)
            
            out = pl.read_excel(full_path, engine="calamine")

            #if date in entry and rules in entry :
            if get_date_from_file_df(out, date) :

                print(f"\n[+] File found for {date} and for {full_fundation} : {entry}")
                return entry
            
    return None


def get_file_by_fund_n_date_collat (
        
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : Optional[str] = "HV",

        d_format : str = "%Y%m%d",

        rules : Optional[str] = None,
        dir_abs_path : Optional[str] = None,

        extensions : Tuple[str, str] = (".xls", ".xlsx")
    
    ) -> Optional[str] :
    """
    This function looks for the path file by date and fundation (in the file name)
    """
    date = date_to_str(date, d_format)
    
    rules = UBS_FILENAMES_COLLATERAL if rules is None else rules
    dir_abs_path = UBS_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path

    full_fundation = get_full_name_fundation(fundation)

    for entry in os.listdir(dir_abs_path) :

        if entry.lower().endswith(extensions) and rules in entry :

            full_path = os.path.join(dir_abs_path, entry)
            
            wb = CalamineWorkbook.from_path(full_path)
            sheet_name = wb.sheet_names[0]
            
            if sheet_name.endswith(date) :

                print(f"\n[+] [UBS] File found for {date} and for {full_fundation} : {entry}")
                return entry

    return None


def get_date_from_file_df (
        
        df : pl.DataFrame,
        date : Optional[str | dt.datetime | dt.date] = None,
        format : str = "%b %d, %Y",

    ) :
    date = date_to_str(date, format)
    date_formatted = date.replace(" 0", " ")
    colname = df.columns[0]

    # Get first 2 values from first column
    values = df[colname].head(2).to_list()

    for value in values :
        
        if value.startswith(date_formatted) :
            return True
        
    return False


def get_df_from_file_cash (
        
        file_abs_path : Optional[str] = None,

        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : Optional[str] = "HV",

        schema_overrides : Optional[Dict] = None,

    ) -> Optional[pl.DataFrame] :
    """
    
    """
    file_abs_path = get_file_by_fund_n_date_cash(date, fundation) if file_abs_path is None else file_abs_path
    schema_overrides = UBS_REQUIRED_COLUMNS if schema_overrides is None else schema_overrides

    # Don't use the schema overrides due to unknow columns
    out = pl.read_excel(file_abs_path, engine="calamine")
    out = out.drop_nulls()

    new_cols = out.row(0)  # -> tuple : ("Collateral Name / Type", "Cusip/ISIN", ...)

    out = out.slice(1)
    out = out.rename({old: new for old, new in zip(out.columns, new_cols)})

    for col, dtype in schema_overrides.items() :
    
        if col in out.columns :
            out = out.with_columns(pl.col(col).cast(dtype))
    
    return out


def get_df_from_file_collateral (
        
        file_abs_path : Optional[str] = None,
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : Optional[str] = "HV",
        schema_overrides : Optional[Dict] = None,


    ) -> Optional[pl.DataFrame] :
    """
    
    """
    file_abs_path = get_file_by_fund_n_date_cash(date, fundation) if file_abs_path is None else file_abs_path
    schema_overrides = USB_TARGET_FIELDS if schema_overrides is None else schema_overrides

    # Don't use the schema overrides due to unknow columns
    df = pl.read_excel(file_abs_path, engine="calamine", drop_empty_rows=True)
    df_clean = df.filter(~pl.all_horizontal(pl.all().is_null()))

    all_cols = df_clean.columns
    
    first_col_name = all_cols[0]       # col index 0
    data_cols = all_cols[1:] 
    
    df_marked = (

        df_clean
        .with_row_index("row_idx")  # pour garder la position
        .with_columns(
            is_net = (
                pl.col(first_col_name)
                .cast(pl.Utf8)
                .str.contains("Netted")
            )
        )

    )

    net_rows = df_marked.filter(pl.col("is_net"))
    row_idx = int(net_rows.select("row_idx").to_series()[0])

    df_pair = (

        df_marked
        .filter(pl.col("row_idx").is_in([row_idx - 1, row_idx]))
        .sort("row_idx")               # line i-1 then line i
        .select(data_cols)     
 
    )

    row_value_vals  = df_pair.row(1)   # line i
    
    value_non_null = [val for _, val in zip(data_cols, row_value_vals) if val is not None]
    data = {name: [val] for name, val in zip(list(schema_overrides.keys()), value_non_null)}

    new_df = pl.DataFrame(data, schema_overrides=schema_overrides, strict=False)
    
    return new_df
    

def get_full_name_fundation (fund : str, fundations : Optional[Dict] = None) -> Optional[str] :
    """
    
    """
    fundations = FUNDATIONS if fundations is None else fundations
    full_fund = fundations.get(fund, None)

    return full_fund