from __future__ import annotations

import os
import re, pathlib
import polars as pl

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Union, Tuple, Optional, Iterable, Set

from src.config import COUNTERPARTIES


def split_by_conterparty (dataframe : pl.DataFrame, rules : Optional[Dict[str, Dict]] = None)  -> Dict[str, pl.DataFrame] :
    """
    
    """
    rules = COUNTERPARTIES if rules is None else rules 

    compiled = _compile_subject_patterns(rules)
    buckets : Dict[str, List[dict]] = {name: [] for name in rules.keys()}
    buckets["UNMATCHED"] = []

    for row in (dataframe).iter_rows(named=True) :

        matched = False

        for name, rule in rules.items() :

            if _row_matches_counterparty(row, rule, compiled[name], None) :

                buckets[name].append(row)
                matched = True  

                break

        if not matched :
            buckets["UNMATCHED"].append(row)

    out : Dict[str, pl.DataFrame] = {}

    for name, rows in buckets.items() :

        out[name] = pl.DataFrame(rows, schema_overrides=dataframe.schema)
    
    return out
    

def _row_matches_counterparty (row : Dict, rule : Dict, subj_re : re.Pattern, filenames : Optional[Iterable[str]] = None) -> bool :
    """
    
    """
    sender  = str((row.get("from") or "")).lower().strip()
    
    # email exact match
    emails = {e.lower() for e in rule.get("emails", set())}
    
    if sender in emails :
        return True
    
    subject = str(row.get("subject") or "")
    # subject regex
    if subj_re.search(subject or "") :
        return True
    
    # filename hit (if we passed any filenames for this row)
    if filenames :

        targets = {f.lower() for f in rule.get("filenames", set())}
        
        for fname in filenames :

            if fname and fname.lower() in targets :
                return True
            
    return False


def _compile_subject_patterns (rules : Optional[Dict[str, Dict]]) -> Dict[str, re.Pattern] :
    """
    
    """
    compiled = {}

    for k, v in rules.items() :

        pat = v.get("subject") or ""
        
        try :
            compiled[k] = re.compile(pat) if pat else re.compile(r"^(?!)")

        except re.error :
            compiled[k] = re.compile(r"^(?!)")
            
    return compiled

