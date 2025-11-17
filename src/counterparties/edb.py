from __future__ import annotations

import os
import polars as pl
import datetime as dt

from typing import Optional, Dict, Tuple, List

from src.config import (
    EBD_ATTACHMENT_DIR_ABS_PATH, EDB_REQUIRED_COLUMNS, 
    EDB_CASH_TYPE_ALLOWED, EDB_CASH_DESC_ALLOWED,
    EDB_COLLAT_TYPE_ALLOWED, EDB_COLLAT_DESC_ALLOWED, EDB_COLLAT_DESC_DICT,
    CASH_COLUMNS, COLLATERAL_COLUMNS
)
from src.utils import get_full_name_fundation, date_to_str, convert_forex, cache_update, cache_load_row, str_to_date
from src.api import call_api_for_pairs


def edb_cash (
        
        date : Optional[str | dt.date | dt.datetime] = None,

        fundation : Optional[str] = "HV",
        exchange : Optional[Dict[str, float]] = None,

        filename : Optional[str] = None,
        dir_abs_path : Optional[str] = None,
        
        structure : Optional[Dict] = None,
        schema_overrides : Optional[Dict] = None

    ) -> Optional[str] :
    """
    
    """
    dir_abs_path = EBD_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path
    schema_overrides = EDB_REQUIRED_COLUMNS if schema_overrides is None else schema_overrides
    structure = CASH_COLUMNS if structure is None else structure

    filename = get_file_by_fund_n_date(date, fundation, kind="cash", dir_abs_path=dir_abs_path) if filename is None else filename

    if filename is None :

        print("\n[-] File not found...Donwload needed files")
        return pl.DataFrame(schema=structure)

    full_path = os.path.join(dir_abs_path, filename)

    df = get_df_from_file(full_path, date, fundation, schema_overrides)

    out = process_cash_by_fund(df, date, fundation, exchange=exchange, structure=structure)

    return out


def edb_collateral (
        
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : Optional[str] = "HV",
        exchange : Optional[Dict[str, float]] = None,

        filename : Optional[str] = None,

        dir_abs_path : Optional[str] = None,
        
        structure : Optional[Dict] = None,
        schema_overrides : Optional[Dict] = None

    ) :
    """
    
    """
    dir_abs_path = EBD_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path
    schema_overrides = EDB_REQUIRED_COLUMNS if schema_overrides is None else schema_overrides
    structure = COLLATERAL_COLUMNS if structure is None else structure

    filename = get_file_by_fund_n_date(date, fundation, kind="collateral", dir_abs_path=dir_abs_path) if filename is None else filename

    if filename is None :
        return pl.DataFrame(schema=structure)

    full_path = os.path.join(dir_abs_path, filename)

    df = get_df_from_file(full_path, date, fundation, schema_overrides)

    out = process_collat_by_fund(df, date, fundation, exchange=exchange, structure=structure)

    return out



# ---------------------- CASH ----------------------


def process_cash_by_fund (
        
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

    type_allowed = EDB_CASH_TYPE_ALLOWED if type_allowed is None else type_allowed
    desc_allowed = EDB_CASH_DESC_ALLOWED if desc_allowed is None else desc_allowed

    exchange = call_api_for_pairs(date) if exchange is None else exchange
    structure = CASH_COLUMNS if structure is None else structure

    if dataframe is None or dataframe.is_empty() :
        return pl.DataFrame(schema_overrides=structure)
    
    if isinstance(type_allowed, str): type_allowed = [type_allowed]
    if isinstance(desc_allowed, str): desc_allowed = [desc_allowed]

    df_type = dataframe.filter(pl.col("TYPE").is_in(type_allowed))
    df_desc = df_type.filter(pl.col("DESCRIPTION").is_in(desc_allowed))

    if df_desc.is_empty() :
        return None

    ccy_list = df_desc["CURRENCY"].to_list()
    amt_list = df_desc["AMOUNT"].to_list()

    amt_convert_list = convert_forex(ccy_list, amt_list, exchange)
    val_exchange = [exchange.get(c) or 1.0 for c in ccy_list]

    out = pl.DataFrame(

        {
            "Fundation" : full_fund,
            "Account" : df_desc["ACCOUNT"].cast(pl.Utf8),
            "Date" : date,
            "Bank" : "EDB",
            "Type" : df_desc["DESCRIPTION"],
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

    type_allowed = EDB_COLLAT_TYPE_ALLOWED if type_allowed is None else type_allowed
    desc_allowed = EDB_COLLAT_DESC_ALLOWED if desc_allowed is None else desc_allowed

    exchange = call_api_for_pairs(date) if exchange is None else exchange
    structure = COLLATERAL_COLUMNS if structure is None else structure

    columns = list(structure.keys())

    if dataframe is None or dataframe.is_empty() :
        return pl.DataFrame(schema_overrides=structure, schema=columns)
    
    if isinstance(type_allowed, str): type_allowed = [type_allowed]
    if isinstance(desc_allowed, str): desc_allowed = [desc_allowed]

    df_type = dataframe.filter(pl.col("TYPE").is_in(type_allowed))
    df_desc = df_type.filter(pl.col("DESCRIPTION").is_in(desc_allowed))

    df_out_dict = {

        "Fundation" : full_fund,
        "Account" : df_desc["ACCOUNT"].item(0),
        "Date" : date,
        "Bank" : "EDB",
        "Currency" : "EUR",
        "Total" : 0.0, #"Total Collateral at Bank" : pl.Float64,
        "IM" : 0.0,
        "VM" : 0.0,
        "Requirement" : 0.0,
        "Net Excess/Deficit" : 0.0

    }

    for description in desc_allowed :

        df_temp = df_desc.filter(pl.col("DESCRIPTION") == description)

        if df_temp.height == 0 :
            continue

        ccy_list = df_temp["CURRENCY"].to_list()
        amt_list = df_temp["AMOUNT"].to_list()

        convert_tmp = (convert_forex(ccy_list, amt_list, exchange))
        tmp_sum = round(sum(convert_tmp), 3)

        rows_to_affect = EDB_COLLAT_DESC_DICT.get(description)

        if len(rows_to_affect) == 0 :
            continue

        for field in rows_to_affect :

            df_out_dict[field] = tmp_sum
    
    df_out_dict["IM"] = (-1) * df_out_dict["IM"]
    df_out_dict["Requirement"] = (-1) * df_out_dict["Requirement"]

    df_out_dict["Net Excess/Deficit"] = df_out_dict.get("Total", 0.0) + df_out_dict.get("Requirement", 0.0)

    out = pl.DataFrame(

        df_out_dict,
        schema_overrides=structure

    )

    return out


# ---------------------- GENERAL FUNCTIONs ----------------------


def get_file_by_fund_n_date (
    
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : Optional[str] = "HV",

        d_format : str = "%Y%m%d",
        f_format : str = "_",

        kind : Optional[str] = "cash",
        rules : Optional[Dict] = None,

        dir_abs_path : Optional[str] = None,
    
    ) -> Optional[str] :
    """
    This function looks for the path file by date and fundation (in the file name)
    """
    date_obj = str_to_date(date)
    date_format = date_to_str(date, d_format)

    df = cache_load_row(None, "EDB", kind, fundation, date_obj)

    if df.height > 0 :

        print("\n[+] Data information found in cache...Loading")

        col_data = df.select("Filename").item()
        return col_data

    dir_abs_path = EBD_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path

    full_fundation = get_full_name_fundation(fundation)
    formatted_fund = edb_fundation_name_format(full_fundation, f_format)

    if formatted_fund is None :

        print(f"\n[-] Fundation not found. Retry with a correct fundation name...")
        return full_fundation
    
    for entry in os.listdir(dir_abs_path) :

        if (date_format) in entry and formatted_fund in entry :

            print(f"\n[+] [EDB] File found for {date} and for {full_fundation} : {entry}")
            #cache_update(None, date_obj, "EDB", fundation, kind, str(entry))

            return entry
        
    return None


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

        print(f"\n[-] Fundation is None. Retry with a correct fundation name...")
        return None

    strip_fundation = fundation.strip()
    formatted_fund = strip_fundation.replace(" ", format)

    return formatted_fund
