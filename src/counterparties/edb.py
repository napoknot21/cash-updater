from __future__ import annotations

import os
import polars as pl

from typing import Optional, Dict, Tuple, List
from src.config import COUNTERPARTIES




# ---------------------- Cash ----------------------



def get_cash (dataframe : pl.DataFrame, md5 : Optional[str] = None, rules : Optional[Dict] = None) :
    """
    This function will return the cas for both fundations (HV, WR)
    """
    rules = COUNTERPARTIES if rules is None else rules
    df_dicts = dataframe.to_dicts()

    for row in df_dicts :
        return None
    
    return None
























# ---------------------- Collateral ----------------------


def get_collateral (dataframe : pl.DataFrame, md5 : str) :
    """
    
    """
    return None