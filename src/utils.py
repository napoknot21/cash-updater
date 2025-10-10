from __future__ import annotations

import datetime as dt

from typing import Optional, List


def date_to_str (date : Optional[str | dt.datetime | dt.date] = None, format : str = "%Y-%m-%d") -> str :
    """
    
    """
    if date is None :
        date = dt.datetime.now()

    if isinstance(date, str) :
        return str(date)

    return date.strftime(format)