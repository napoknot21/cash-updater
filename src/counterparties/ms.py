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
from src.utils import get_full_name_fundation, date_to_str, convert_forex, cache_update, str_to_date, cache_load_row, load_cache


def ms_cash (
        
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : str = "HV",

        exchange : Optional[Dict[str, float]] = None,

        filename : Optional[str] = None,
        dir_abs_path : Optional[str] = None,

        schema_overrides : Optional[Dict] = None,
        structure : Optional[Dict] = None,

        rules : Optional[Dict] = None

    ) -> Optional[pl.DataFrame] :
    """
    
    """
    dir_abs_path = MS_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path
    schema_overrides = MS_REQUIRED_COLUMNS if schema_overrides is None else schema_overrides
    structure = CASH_COLUMNS if structure is None else structure

    rules = MS_FILENAMES_CASH if rules is None else rules

    filename = get_file_by_fund_n_date(date, fundation, kind="cash", rules=rules) #if filename is None else filename

    if filename is None :
        return pl.DataFrame(schema=structure)

    full_path = os.path.join(dir_abs_path, filename)

    df = get_df_from_file_cash(full_path)

    out = process_cash_by_fund(df, date, fundation, exchange=exchange, structure=structure)

    return out


def ms_collateral (
        
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : str = "HV",
        exchange : Optional[Dict[str, float]] = None,
        
        filename : Optional[str] = None,
        dir_abs_path : Optional[str] = None,

        schema_overrides : Optional[Dict] = None,
        structure : Optional[Dict] = None,

        rules : Optional[str] = None,
        extensions : Tuple[str, str] = ("pdf",)
        
    ) -> Optional[pl.DataFrame] :
    """
    
    """
    dir_abs_path = MS_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path
    schema_overrides = MS_TARGET_FIELDS if schema_overrides is None else schema_overrides
    structure = COLLATERAL_COLUMNS if structure is None else structure

    rules = MS_FILENAMES_COLLATERAL if rules is None else rules

    filename = get_file_by_fund_n_date(date, fundation, kind="collateral", rules=rules, extensions=extensions) if filename is None else filename

    if filename is None :
        return  pl.DataFrame(schema=structure)

    full_path = os.path.join(dir_abs_path, filename)

    df = extract_collateral_fields_to_polars(full_path, target_fields=schema_overrides, fundation=fundation)
    
    out = process_collat_by_fund(df, date, fundation, exchange=exchange, structure=structure)

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

    ccy_list = dataframe["ccy"].to_list()
    amt_list = dataframe["quantity"].to_list()

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

    target = MS_TARGET_FIELDS if target is None else target
    columns = list(structure.keys())

    entity = MS_ENTITY if entity is None else entity

    if df is None or df.is_empty() :
        return pl.DataFrame(schema_overrides=structure, schema=columns)
    
    n = df.height

    account = MS_ACCOUNTS.get(fundation, "000000000")

    # TODO  : Look for a method that extract this from the pdf. For now it's hardcoded
    #         But the rest of the method is dynamical
    ccy_list = "EUR"
    ccy_list = [ccy_list]

    vm_list = convert_forex(ccy_list, df["Net MTM"].to_list(), exchange=exchange)
    im_list = convert_forex(ccy_list, df["Upfront Amount Rec / (Pay)"].to_list(), exchange)

    col_list = convert_forex(ccy_list, df["Customer Balances"].to_list(), exchange)
    

    df_out_dict = {

        "Fundation" : str(full_fund),
        "Account" : account,
        "Date" : date,
        "Bank" : entity,
        "Currency" : ccy_list,
        "Total" : col_list, #"Total Collateral at Bank" : pl.Float64,
        "IM" : im_list,
        "VM" : vm_list,
        "Requirement" : 0.0,
        "Net Excess/Deficit" : 0.0

    }
    
    df_out_dict["Requirement"] = [(im or 0.0) + (vm or 0.0) for im, vm in zip(im_list, vm_list)]
    df_out_dict["Net Excess/Deficit"] = [t + r for t, r in zip(col_list, df_out_dict["Requirement"])]

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
        kind : Optional[str] = "cash",

        rules : Optional[str] = None,
        dir_abs_path : Optional[str] = None,

        extensions : Tuple[str, str] = ("pdf",),
        n_lines : int = 3
    
    ) -> Optional[str] :
    """
    This function looks for the path file by date and fundation (in the file name)
    """
    date_obj = str_to_date(date)
    date_format = date_to_str(date, d_format)
    
    df_cahe = load_cache()
    df = cache_load_row(df_cahe, "MS", kind, fundation, date_obj)

    if df.height > 0 :

        col_data = df.select("Filename").item()
        return col_data

    rules = MS_FILENAMES_CASH if rules is None else rules
    dir_abs_path = MS_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path

    full_fundation = get_full_name_fundation(fundation).upper()
    account = MS_ACCOUNTS.get(fundation, "HV")

    for entry in os.listdir(dir_abs_path) :

        if rules in entry and account in entry and date_format in entry :

            print(f"\n[+] File found for {date} and for {full_fundation.lower()} : {entry}")
            #cache_update(df_cahe, date_obj, "MS", fundation, kind, entry)
            
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
    columns = list(schema_overrides.keys())
    
    dataframe = pl.read_excel(file_abs_path, schema_overrides={c : pl.Utf8 for c in schema_overrides.keys()}, columns=columns, drop_empty_rows=True)
    
    # Drop rows where all cells are null or blank
    df_clean = dataframe.filter(
        ~pl.all_horizontal(
            [
                pl.col(c).is_null() | (pl.col(c).cast(pl.Utf8).str.strip_chars() == "")
                for c in dataframe.columns
            ]
        )
    )

    # Cast numerical values
    df = df_clean.with_columns(
    
        pl.col("quantity").map_elements(parse_amount, return_dtype=pl.Float64)
    
    )

    return df


def get_info_from_file_collateral (
        
        file_abs_path : Optional[str] = None,
        rules : Optional[int] = None,
        fundation : Optional[str] = None,
        target_fields : Optional[Dict] = None,

    ) -> Optional[pl.DataFrame] :
    """
    
    """
    rules = MS_TABLE_PAGES if rules is None else rules
    target_fields = MS_TARGET_FIELDS if target_fields is None else target_fields

    page = rules.get(fundation)
    tables = camelot.read_pdf(file_abs_path, pages=str(page), flavor="stream")

    n_tables = tables.n

    keywords = list(target_fields.keys())
    best_match = None
    best_score = 0

    for i in range(n_tables) :

        df = tables[i].df
        rows, cols = df.shape

        if cols < 2 :
            continue
        
        if rows >= 25 :
            continue

        df_flat = " ".join(df.astype(str).values.flatten()).lower()

        # Count keyword matches
        score = sum(1 for word in keywords if word.lower() in df_flat)

        if score > best_score :

            best_score = score
            best_match = i

    if best_match is not None and best_score > 0 :

        print(f"\n[+] Information successfully found and extracted !")
        df_best = tables[best_match].df

        return pl.from_pandas(df_best)

    print("[!] No matching table found\n")

    return None


def extract_collateral_fields_to_polars (
        
        file_abs_path: str,
        target_fields: Optional[Dict] = None,
        fundation : str = "HV"
    
    ) -> Optional[pl.DataFrame] :
    """
    
    """
    target_fields = MS_TARGET_FIELDS if target_fields is None else target_fields
    
    dataframe = get_info_from_file_collateral(file_abs_path, fundation=fundation, target_fields=target_fields)

    if dataframe is None or dataframe.height == 0 :
        return None

    # Drop rows where all cells are null or blank
    df_clean = dataframe.filter(
        ~pl.all_horizontal(
            [
                pl.col(c).is_null() | (pl.col(c).cast(pl.Utf8).str.strip_chars() == "")
                for c in dataframe.columns
            ]
        )
    )

    cols = df_clean.columns
    
    key_col = cols[0]
    val_cols = cols[1:]

    raw_names = (
        df_clean
        .select(pl.col(key_col).cast(pl.Utf8).str.strip_chars().alias(key_col))
        .to_series()
        .to_list()
    )

    def make_unique(names: list[str], base_if_empty: str = "col") -> list[str]:
        seen = {}
        unique = []
        for name in names:
            # normalize / avoid None
            name = (name or "").strip()
            if not name:
                name = base_if_empty
            count = seen.get(name, 0)
            if count > 0:
                new_name = f"{name}_{count}"
            else:
                new_name = name
            unique.append(new_name)
            seen[name] = count + 1
        return unique

    # Transpose the rest
    col_names = make_unique(raw_names) #df_clean.select(key_col).to_series().to_list()
    new_df = df_clean.select(val_cols).transpose(column_names=col_names)

    new_df = new_df.select(list(target_fields.keys()))
    print(new_df)

    df_new_clean = new_df.filter(
        ~pl.all_horizontal(
            [
                pl.col(c).is_null() | (pl.col(c).cast(pl.Utf8).str.strip_chars() == "")
                for c in new_df.columns
            ]
        )
    )

    df_parsed = df_new_clean.with_columns(
        [
            pl.col(c).map_elements(parse_amount, return_dtype=pl.Float64).alias(c)
            for c in df_new_clean.columns
        ]
    )
    
    return df_parsed


