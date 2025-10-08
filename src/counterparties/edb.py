from __future__ import annotations

import os
import polars as pl

from typing import Optional, Dict, Tuple, List
from src.config import COUNTERPARTIES, FUNDATIONS




# ---------------------- Cash ----------------------


def get_full_name_fundation (fund : str, fundations : Optional[Dict] = None) :
    """
    
    """
    fundations = FUNDATIONS if fundations is None else fundations
    full_fund = fundations.get(fund, None)

    return full_fund


def get_fundation_file_path (fund : str = "HV") :
    """
    This function will look for the specific file
    """
    return None



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