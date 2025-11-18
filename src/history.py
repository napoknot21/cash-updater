from __future__ import annotations

import os
import polars as pl
import datetime as dt

from typing import Optional, Dict

from src.config import HISTORY_DIR_ABS_PATH, CASH_COLUMNS, COLLATERAL_COLUMNS


def get_history_path (fundation: str, kind: str) -> str:
    """
    kind: 'cash' or 'collateral'
    """
    subdir = os.path.join(HISTORY_DIR_ABS_PATH, fundation)
    os.makedirs(subdir, exist_ok=True)

    return os.path.join(subdir, f"{kind}.xlsx")


def load_history(
        
        fundation: str,
        kind: str,

        structure: Optional[Dict] = None,
    
    ) -> pl.DataFrame:
    """
    Load existing history file for (fundation, kind).
    If file does not exist, return an empty DataFrame with the proper schema.
    """
    if kind == "cash":
        structure = CASH_COLUMNS if structure is None else structure
    
    else :
        structure = COLLATERAL_COLUMNS if structure is None else structure

    path = get_history_path(fundation, kind)

    if not os.path.exists(path):
        # empty history with expected columns
        return pl.DataFrame(schema_overrides=structure)

    # Using pandas for Excel, then convert to polars
    df = pl.read_excel(path, schema_overrides=structure)

    # Ensure at least the expected columns exist
    for col in structure.keys() :

        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))

    # Reorder columns
    df = df.select(list(structure.keys()))
    
    return df


def save_history (
        
        
        fundation: str,
        kind: str,
        dataframe: pl.DataFrame,
    
    ) -> None :
    """
    Save the history DataFrame to the corresponding Excel file.
    """
    path = get_history_path(fundation, kind)
    dataframe.write_excel(path)
    
    print(f"\n[+] History updated: {path}")