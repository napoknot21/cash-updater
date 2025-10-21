from __future__ import annotations

import os
import re
import polars as pl
import datetime as dt

from typing import Optional, Dict, Tuple, List


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