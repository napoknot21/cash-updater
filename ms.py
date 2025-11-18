from __future__ import annotations

from src.counterparties.ms import ms_cash, ms_collateral
from src.counterparties.ubs import ubs_cash, ubs_collateral
from src.api import call_api_for_pairs


exchange = call_api_for_pairs()

df = ubs_collateral("2025-11-14", "HV", exchange=exchange)

print(df)