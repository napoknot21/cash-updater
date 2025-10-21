from __future__ import annotations

import os
import camelot
import polars as pl
import pandas as pd
import datetime as dt

from PyPDF2 import PdfReader
from typing import Optional, Dict, Tuple

from src.config import *
from src.parser import *
from src.api import call_api_for_pairs
from src.utils import get_full_name_fundation, date_to_str, convert_forex


def ms_cash (
        
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : str = "HV",

        exchange : Optional[Dict[str, float]] = None,

        dir_abs_path : Optional[str] = None,
        schema_overrides : Optional[Dict] = None,
        
        rules : Optional[Dict] = None

    ) -> Optional[pl.DataFrame] :
    """
    
    """
    dir_abs_path = MS_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path
    schema_overrides = MS_REQUIRED_COLUMNS if schema_overrides is None else schema_overrides

    rules = MS_FILENAMES if rules is None else rules

    filename = get_file_by_fund_n_date(date, fundation, rules=rules)
    full_path = os.path.join(dir_abs_path, filename)

    df = extract_collateral_fields_to_polars(full_path)

    out = process_cash_by_fund(df, date, fundation, exchange=exchange)

    return out


def ms_collateral (
        
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : str = "HV",
        exchange : Optional[Dict[str, float]] = None,

        dir_abs_path : Optional[str] = None,
        schema_overrides : Optional[Dict] = None,

        rules : Optional[str] = None,
        extensions : Tuple[str, str] = ("pdf",)
        
    ) -> Optional[pl.DataFrame] :
    """
    
    """
    dir_abs_path = GS_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path
    schema_overrides = GS_REQUIRED_COLUMNS if schema_overrides is None else schema_overrides

    rules = GS_FILENAMES_COLLATERAL if rules is None else rules

    filename = get_file_by_fund_n_date(date, fundation, rules=rules, extensions=extensions)
    full_path = os.path.join(dir_abs_path, filename)

    df = extract_collateral_fields_to_polars(full_path)
    
    out = process_collat_by_fund(df, date, fundation, exchange=exchange)

    return out


# ---------------------- CASH ----------------------


def process_cash_by_fund (
        
        dataframe : pl.DataFrame,

        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : str = "HV",

        exchange : Optional[Dict[str, float]] = None,
        structure : Optional[Dict] = None,

        rules : Optional[Dict] = None,
        entity : Optional[str] = None,
    
    ) -> Optional[pl.DataFrame] :
    """
    
    """
    date = date_to_str(date)
    full_fund = get_full_name_fundation(fundation)

    rules = MS_ACCOUNTS if rules is None else rules
    entity = MS_ENTITY if entity is None else entity

    exchange = call_api_for_pairs(date) if exchange is None else exchange
    structure = CASH_COLUMNS if structure is None else structure

    if dataframe is None or dataframe.is_empty() :
        return pl.DataFrame(schema_overrides=structure)

    ccy_list = ["EUR"]
    amt_list = dataframe["Rounding Amount"].to_list()

    amt_convert_list = convert_forex(ccy_list, amt_list, exchange)
    val_exchange = [exchange.get(c) or 1.0 for c in ccy_list]
    
    out = pl.DataFrame(

        {
            "Fundation" : full_fund,
            "Account" : rules.get(fundation),
            "Date" : date,
            "Bank" : entity,
            "Type" : ["Held"],
            "Currency" : ccy_list,
            "Amount in CCY": amt_list,
            "Exchange": val_exchange,
            "Amount in EUR" : amt_convert_list 
        },
        schema_overrides=structure

    )

    #print(dataframe)

    return out


# ---------------------- COLLATERAL ----------------------


def process_collat_by_fund (
        
        df : pl.DataFrame,

        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : str = "HV",

        exchange : Optional[Dict[str, float]] = None,
        structure : Optional[Dict] = None,

        entity : Optional[str] = None,
        target : Optional[Dict] = None
    
    ) -> Optional[pl.DataFrame] :
    """
    
    """
    date = date_to_str(date)
    full_fund = get_full_name_fundation(fundation)

    exchange = call_api_for_pairs(date) if exchange is None else exchange
    structure = COLLATERAL_COLUMNS if structure is None else structure

    target = GS_TARGET_FIELDS if target is None else target
    columns = list(structure.keys())

    entity = GS_ENTITY if entity is None else entity

    if df is None or df.is_empty() :
        return pl.DataFrame(schema_overrides=structure, schema=columns)
    
    account = GS_ACCOUNTS.get(fundation, "000000000")

    df_out_dict = {

        "Fundation" : full_fund,
        "Account" : account,
        "Date" : date,
        "Bank" : entity,
        "Currency" : None,
        "Total" : 0.0, #"Total Collateral at Bank" : pl.Float64,
        "IM" : 0.0,
        "VM" : 0.0,
        "Requirement" : 0.0,
        "Net Exess/Deficit" : 0.0

    }


    ccy_list = df["Reference ccy"].to_list()

    exp_list = convert_forex(ccy_list, df["Total Exposure"].to_list(), exchange)
    req_list = convert_forex(ccy_list, df["Total Requirement"].to_list(), exchange)
    im_list = convert_forex(ccy_list, df["CP Initial Margin"].to_list(), exchange)
    col_list = convert_forex(ccy_list, df["Total Collateral"].to_list(), exchange)

    df_out_dict["Currency"] = ccy_list[0]
    df_out_dict["Total"] = col_list[0]

    df_out_dict["IM"] = im_list[0]
    df_out_dict["Requirement"] = req_list[0]
    
    vm = exp_list[0] - df_out_dict.get("IM", 0.0) # TODO : Hardcoded here, 
    df_out_dict["VM"] =  vm
    
    df_out_dict["Net Exess/Deficit"] = df_out_dict.get("Total", 0.0) - df_out_dict.get("Requirement", 0.0)

    out = pl.DataFrame(

        df_out_dict,
        schema_overrides=structure

    )

    return out


# ---------------------- GENERAL FUNCTIONs ----------------------


def get_file_by_fund_n_date (
    
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : Optional[str] = "HV",

        d_format : str = "%B %d, %Y",

        rules : Optional[str] = None,
        dir_abs_path : Optional[str] = None,

        extensions : Tuple[str, str] = ("pdf",),
        n_lines : int = 3
    
    ) -> Optional[str] :
    """
    This function looks for the path file by date and fundation (in the file name)
    """
    date = date_to_str(date, d_format)

    dir_abs_path = MS_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path
    rules = MS_FILENAMES if rules is None else rules

    full_fundation = get_full_name_fundation(fundation).upper()
    fund_words = [w for w in full_fundation.split() if w]

    for entry in os.listdir(dir_abs_path) :
        
        # Here we don't have a current way to determine if a file it's for HV, WR, etc...
        # We have to open each file (not optimal but no other solution for now...)

        lines = get_info_from_file_collateral(os.path.join(dir_abs_path, entry), n_lines=n_lines)

        date_line_idx = None

        for i, ln in enumerate(lines) :
        
            if date in ln :

                date_line_idx = i
                break
        
        if date_line_idx is None :
            continue  # no date found at all

        # Look for the fundation in a different line
        found_fund = False

        for j, ln in enumerate(lines) :

            if j == date_line_idx :
                continue  # skip same line as the date
            
            if fund_words[0] in ln.upper() or fundation.upper() in ln.upper() :
                
                found_fund = True
                break

        if found_fund :

            print(f"\n[+] File found for {date} and for {full_fundation.lower()} : {entry}\n")
            return entry
        
    return None


def get_df_from_file_cash (
        
        file_abs_path : Optional[str] = None,
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : Optional[str] = "HV",
        schema_overrides : Optional[Dict] = None,
        skip_rows : int = 9

    ) -> pl.DataFrame :
    """
    
    """
    file_abs_path = get_file_by_fund_n_date(date, fundation) if file_abs_path is None else file_abs_path

    schema_overrides = MS_REQUIRED_COLUMNS if schema_overrides is None else schema_overrides

    # TODO : Using pandas temp but should chage to polars
    # dataframe = pl.read_excel(file_abs_path, read_options={"skip_rows" : skip_rows}, schema_overrides=schema_overrides)
    dataframe = pd.read_excel(file_abs_path, skiprows=skip_rows)
    df_clean = dataframe.dropna(subset=["Actual/Pending"]) # Help us to clean the df
    
    return pl.from_pandas(df_clean, schema_overrides=schema_overrides)


def get_info_from_file_collateral (
        
        file_abs_path : Optional[str] = None,
        date : Optional[str | dt.datetime | dt.date] = None,
        fundation : Optional[str] = "HV",
        n_lines : Optional[int] = 3

    ) -> str :
    """
    
    """
    reader = PdfReader(file_abs_path)
    text = reader.pages[0].extract_text() or ""
    
    if n_lines is None :
    
        tables = camelot.read_pdf(file_abs_path, pages="1", flavor="lattice")
        
        df_raw = pl.from_pandas(tables[0].df)
        
        keys = df_raw["0"].to_list()
        vals = df_raw["1"].to_list()
        df_wide = pl.DataFrame([dict(zip(keys, vals))])

        return df_wide

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    return lines[:n_lines]


def extract_collateral_fields_to_polars (
        
        file_abs_path: str,
        target_fields: Optional[Dict] = None,
    
    ) -> Optional[pl.DataFrame] :
    """
    
    """
    target_fields = MS_REQUIRED_COLUMNS if target_fields is None else target_fields
    
    dataframe = get_info_from_file_collateral(file_abs_path, n_lines=None)
    
    df_parsed = dataframe.with_columns(
        [
            pl.col(c).map_elements(parse_amount, return_dtype=pl.Float64).alias(c)
            for c in dataframe.columns
        ]
    )

    common_cols = [c for c in df_parsed.columns if c in target_fields]

    df_keep = df_parsed.select(common_cols)

    df_keep = df_keep.select(
        [
            pl.col(c).cast(target_fields[c], strict=False).alias(c)
            for c in common_cols
        ]
    )

    return df_keep


