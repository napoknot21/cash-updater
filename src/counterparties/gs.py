from __future__ import annotations

import os
import re
import polars as pl
import pandas as pd
import datetime as dt

from PyPDF2 import PdfReader
from typing import Optional, Dict, List, Tuple

from src.config import *
from src.utils import get_full_name_fundation, date_to_str, convert_forex
from src.api import call_api_for_pairs


def gs_cash (
        
        date : Optional[str | dt.date | dt.datetime] = None,
        fundation : str = "HV",
        exchange : Optional[Dict[str, float]] = None,

        dir_abs_path : Optional[str] = None,
        schema_overrides : Optional[Dict] = None,
        
        rules : Optional[Dict] = None

    ) -> Optional[pl.DataFrame] :
    """
    
    """
    dir_abs_path = GS_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path
    schema_overrides = GS_REQUIRED_COLUMNS if schema_overrides is None else schema_overrides

    rules = GS_FILENAMES_CASH if rules is None else rules

    filename = get_file_by_fund_n_date(date, fundation, rules=rules)
    full_path = os.path.join(dir_abs_path, filename)

    df = get_df_from_file_cash(full_path, date, fundation, schema_overrides)

    out = process_cash_by_fund(df, date, fundation, exchange=exchange)

    return out


def gs_collateral (
        
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
    amt_list = dataframe["Quantity"].to_list()

    amt_convert_list = convert_forex(ccy_list, amt_list, exchange)
    val_exchange = [exchange.get(c) or 1.0 for c in ccy_list]

    out = pl.DataFrame(

        {
            "Fundation" : full_fund,
            "Account" : dataframe["Account Number"].to_list(),
            "Date" : date,
            "Bank" : dataframe["GS Entity"],
            "Type" : dataframe["Post/Held"],
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

        d_format : str = "%d_%b_%Y",

        rules : Optional[str] = None,
        dir_abs_path : Optional[str] = None,

        extensions : Tuple[str, str] = ("xls", "xlsx")
    
    ) -> Optional[str] :
    """
    This function looks for the path file by date and fundation (in the file name)
    """
    date = date_to_str(date, d_format)

    dir_abs_path = GS_ATTACHMENT_DIR_ABS_PATH if dir_abs_path is None else dir_abs_path
    rules = GS_FILENAMES_CASH if rules is None else rules

    full_fundation = get_full_name_fundation(fundation).upper()
    fund_words = [w for w in full_fundation.split() if w]

    for entry in os.listdir(dir_abs_path) :

        if date in entry and entry.startswith(rules) and fund_words[0] in entry :

            if entry.lower().endswith(extensions) : 

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

    schema_overrides = GS_REQUIRED_COLUMNS if schema_overrides is None else schema_overrides

    # TODO : Using pandas temp but should chage to polars
    # dataframe = pl.read_excel(file_abs_path, read_options={"skip_rows" : skip_rows}, schema_overrides=schema_overrides)
    dataframe = pd.read_excel(file_abs_path, skiprows=skip_rows)
    df_clean = dataframe.dropna(subset=["Actual/Pending"]) # Help us to clean the df
    
    return pl.from_pandas(df_clean, schema_overrides=schema_overrides)


def get_info_from_file_collateral (
        
        file_abs_path : Optional[str] = None,
        date : Optional[str | dt.datetime | dt.date] = None,
        fundation : Optional[str] = "HV",

    ) -> pl.DataFrame :
    """
    
    """
    reader = PdfReader(file_abs_path)
    text = reader.pages[0].extract_text()

    return text



def extract_collateral_fields_to_polars (
        
        file_abs_path: str,
        target_fields: Optional[Dict] = None,
    
    ) -> Optional[pl.DataFrame] :
    """
    
    """
    target_fields = GS_TARGET_FIELDS if target_fields is None else target_fields
    
    text = get_info_from_file_collateral(file_abs_path)
    lines = build_line_list(text)

    values: Dict[str, str | None] = {}

    for fld in target_fields.keys() :

        raw, _ = extract_field_value_from_lines(lines, fld)
        values[fld] = raw

    row : Dict[str, object] = {}

    for fid, dtype in target_fields.items() :

        raw = values.get(fid)
        row[fid] = cast_raw_value(raw, dtype)

    dataframe = pl.DataFrame([row])

    for col, dtype in target_fields.items() :

        if col not in dataframe.columns :
            dataframe = dataframe.with_columns(pl.lit(None).cast(dtype).alias(col))

    df = dataframe.with_columns([pl.col(c).cast(t) for c, t in target_fields.items()])
    df = df.select(list(target_fields.keys()))

    return df



def parse_amount (s: str) -> Optional[float] :
    """
    Convert :
    - '2,153,209.39' -> 2153209.39
    - '(2,045,725.53)' -> -2045725.53
    '-' -> None
    """
    s = s.strip()
    
    if s.strip() in {"-", "—", "–", ""} :
        return None
    
    neg = False
    if s.startswith("(") and s.endswith(")") :
        
        neg = True
        s = s[1:-1].strip() # delete parenthesis
    
    # Delete comas separators
    s = s.replace(",", "")

    # Garde uniquement chiffres, point et éventuellement signe
    m = re.search(r"[-+]?\d+(?:\.\d+)?", s)
    
    if not m :
        return None
    
    val = float(m.group(0))

    return -val if neg else val


def build_line_list (text: str) -> List[str] :
    """
    
    """
    lines = [ln for ln in (x.strip() for x in text.splitlines()) if ln]

    # Optionnel : supprimer bordures ASCII (╞╡ etc.) si présentes
    lines = [ln for ln in lines if not re.fullmatch(r"[┌┐└┘╞╡═╬─│]+", ln)]
    
    return lines


def extract_field_value_from_lines (lines : List[str], field : str) -> Tuple[Optional[str], Optional[str]] :
    """
    Retourne (raw_value, value_text)
    Heuristiques:
      1) Ligne commence par le champ -> capture du reste sur la même ligne
      2) Ligne == champ exact -> prend la ligne suivante non vide
    """
    # 1) même ligne : "Field : value" ou "Field value"
    #    on autorise ":" optionnel et espaces multiples
    pat_same = re.compile(rf"^{re.escape(field)}\s*:?\s*(.+?)\s*$", re.IGNORECASE)

    for i, ln in enumerate(lines) :

        ln_norm = re.sub(r"\s+", " ", ln.strip())

        # même ligne
        m = pat_same.match(ln_norm)
        
        if m :
            
            raw = m.group(1).strip()
            
            if raw :
                return raw, raw

        # ligne suivante
        if ln_norm.lower() == field.lower() :

            # chercher la prochaine ligne non vide
            j = i + 1
            
            while j < len(lines) and not lines[j].strip() :
                j += 1

            if j < len(lines) :

                nxt = re.sub(r"\s+", " ", lines[j].strip())
                return nxt, nxt

    return None, None


def cast_raw_value (raw: str | None, dtype: pl.datatypes.PolarsDataType) :
    """
    Generic, extensible caster from raw string to a Python value compatible with target Polars dtype.
    Extend formats / booleans / numerics as needed.
    """
    if raw is None or str(raw).strip() in {"-", "—", "–", ""}:
        return None

    s = str(raw).strip()

    # Floats
    if dtype in (pl.Float64, pl.Float32) :
        return parse_amount(s)

    # Ints
    if dtype in (pl.Int64, pl.Int32, pl.Int16, pl.Int8, pl.UInt64, pl.UInt32, pl.UInt16, pl.UInt8) :
        
        val = parse_amount(s)
        return int(val) if val is not None else None

    # Boolean
    if dtype == pl.Boolean :
        return s.lower() in {"true", "yes", "1", "y", "t"}

    # Dates / Datetimes
    if dtype == pl.Date :

        for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y", "%b %d, %Y") :
            
            try :
                return dt.datetime.strptime(s, fmt).date()
            
            except ValueError :
                continue

        return None

    if dtype == pl.Datetime :
        
        for fmt in ("%d-%b-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%b %d, %Y %H:%M:%S") :
        
            try :
                return dt.datetime.strptime(s, fmt)
            
            except ValueError :
                continue

        # Fallback: try date-only then elevate to datetime at midnight
        for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y", "%b %d, %Y") :

            try :

                d = dt.datetime.strptime(s, fmt).date()
                return dt.datetime(d.year, d.month, d.day)
            
            except ValueError :
                continue

        return None

    # Utf8 / default text
    return s