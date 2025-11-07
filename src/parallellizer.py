from __future__ import annotations

import os
import polars as pl

from concurrent.futures import ThreadPoolExecutor, as_completed 
from typing import List, Optional, Dict

from src.api import call_api_for_pairs
from src.utils import date_to_str
from src.config import *


def process_date_range (
        
        dates : Optional[List[str]] = None,
        fundation : Optional[str] = "HV",
        exchange : Optional[Dict[str, float]] = None,    
    
    ) -> None :
    """

    """
    dates = [date_to_str()] if dates is None else dates
    exchange = call_api_for_pairs(date_to_str()) if exchange is None else exchange# Looks for now() forex values

    with ThreadPoolExecutor() as executor :

        futures = []

        for date in dates :
            futures.append(executor.submit(process_single_date, date, fundation, exchange))

        
        # Wait all task to finish
        for future in as_completed(futures) :

            result = future.result()
            print(result)
        

def process_single_date (
        
        date : Optional[str] = None,
        fundation : Optional[str] = "HV",
        exchange : Optional[Dict[str, float]] = None,
        
    ) -> None :
    """
    
    """
    date = date_to_str(date) if date is None else date
    exchange = call_api_for_pairs(date_to_str()) if exchange is None else exchange

    with ThreadPoolExecutor() as executor :

        futures = []

        # Parallelize cash and collateral tasks
        futures.append(executor.submit(download_file_for_date, "cash", date, fundation, exchange, fundation))
        futures.append(executor.submit(download_file_for_date, "collateral", date, fundation, exchange, fundation))
        
        # Wait for all the file download tasks for this date to complete
        for future in as_completed(futures) :

            result = future.result()
            print(result)

