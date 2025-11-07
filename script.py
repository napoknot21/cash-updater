import sys
import os
import polars as pl


sys.path.append("N:/INVESTMENT MANAGEMENT/MATTHIEU/cash_page_update_Sentinelle/")


HV_WR_CASH = "N:/INVESTMENT MANAGEMENT/MATTHIEU/cash_page_update_Sentinelle/dataCash.xlsx"
HV_WR_COLLAT = "N:/INVESTMENT MANAGEMENT/MATTHIEU/cash_page_update_Sentinelle/dataCollat.xlsx"


HV_WR_COLLAT_COLS = {

    "Date" : pl.Date,
    "Fund" : pl.Utf8,
    "AccNumber" : pl.Utf8,
    "Bank" : pl.Utf8,
    "Currency" : pl.Utf8,
    "TotalCollat" : pl.Float64,
    "IM" : pl.Float64,
    "VM" : pl.Float64,
    "Requirement" : pl.Float64,
    "NetExcessDeficit" : pl.Float64,

}


HV_WR_CASH_COLS = {

    "Fund" : pl.Utf8,
    "AccNumber" : pl.Utf8,
    "Date" : pl.Date,
    "Bank" : pl.Utf8,
    "Currency" : pl.Utf8,
    "Type" : pl.Utf8,
    "Amount in Ccy" : pl.Float64,
    "Amount in EUR" : pl.Float64

}


collat_df = pl.read_excel(HV_WR_COLLAT, schema_overrides=HV_WR_COLLAT_COLS, columns=list(HV_WR_COLLAT_COLS.keys()))

print(collat_df)

"""

RENAME_HV_WR_CASH_COLS = {

    "Fund" : "Fundation",
    "AccNumber" : "Account",
    "Date" : "Date",
    "Bank" : "Bank",
    "Type" : "Type",
    "Currency" : "Currency",
    "Amount in Ccy" : "Amount in CCY",
    "Amount in EUR" : "Amount in EUR"

}


cash_rename_df = cash_df.rename(RENAME_HV_WR_CASH_COLS)

print(cash_rename_df)


CASH_COLUMNS = {

    "Fundation" : pl.Utf8,
    "Account" : pl.Utf8, # "Account Number" : pl.Utf8,
    "Date" : pl.Date,
    "Bank" : pl.Utf8,
    "Currency" : pl.Utf8,
    "Type" : pl.Utf8,
    "Amount in CCY" : pl.Float64,
    "Exchange" : pl.Float64,
    "Amount in EUR" : pl.Float64

}

# For example, let's set a dummy value for the "Exchange" column (you can modify this logic)
cash_rename_df = cash_rename_df.with_columns([
    (pl.lit(1.0)).alias("Exchange")  # Replace 1.1 with actual exchange rate logic if needed
])

column_order = [
    "Fundation", "Account", "Date", "Bank", "Type", "Currency", "Amount in CCY", "Exchange", "Amount in EUR"
]

cash_rename_df = cash_rename_df.select(column_order)

grouped = cash_rename_df["Fundation"].unique()

for fund in grouped :

    fund_group = cash_rename_df.filter(pl.col("Fundation") == fund)

    sorted_df = fund_group.sort("Date")
    print(sorted_df )
    if fund == "WR by Heroics" :
        sorted_df.write_excel("./history/WR/cash.xlsx")

    else :
        sorted_df.write_excel("./history/HV/cash.xlsx")
"""
#sort_cash_df = grouped_df.sort("Date")

#print(sort_cash_df)


RENAME_HV_WR_COLLAT_COLS = {

    "Date" : "Date",
    "Fund" : "Fundation",
    "AccNumber" : "Account",
    "Bank" : "Bank",
    "Currency" : "Currency",
    "TotalCollat" : "Total",
    "IM" : "IM",
    "VM" : "VM",
    "Requirement" : "Requirement",
    "NetExcessDeficit" : "Net Excess/Deficit"

}



# Collateral columns format
COLLATERAL_COLUMNS = {

    "Fundation" : pl.Utf8,
    "Account" : pl.Utf8, # "Account Number" : pl.Utf8,
    "Date" : pl.Date,
    "Bank" : pl.Utf8,
    "Currency" : pl.Utf8,
    "Total" : pl.Float64, #"Total Collateral at Bank" : pl.Float64,
    "IM" : pl.Float64,
    "VM" : pl.Float64,
    "Requirement" : pl.Float64,
    "Net Excess/Deficit" : pl.Float64

}

collat_df = collat_df.rename(RENAME_HV_WR_COLLAT_COLS)
print(collat_df)
column_order = list(COLLATERAL_COLUMNS.keys())
cash_rename_df = collat_df.select(column_order)

# Apply the calculations
cash_rename_df = cash_rename_df.with_columns([
    # IM = -1 * IM
    (pl.col("IM") * -1).alias("IM"),
    
    # VM = -1 * VM
    (pl.col("VM") * -1).alias("VM")])
    
cash_rename_df = cash_rename_df.with_columns([
    # Requirement = IM + VM
    (pl.col("IM") + pl.col("VM")).alias("Requirement"),
])

cash_rename_df = cash_rename_df.with_columns([
    # Net Excess/Deficit = Total + Requirement
    (pl.col("Total") + pl.col("Requirement")).alias("Net Excess/Deficit")
])

print(cash_rename_df)

grouped = cash_rename_df["Fundation"].unique()
print(grouped)

for fund in grouped :
    fund_group = cash_rename_df.filter(pl.col("Fundation") == fund)

    sorted_df = fund_group.sort("Date")
    print(sorted_df )
    if fund == "WR by Heroics" :
        sorted_df.write_excel("./history/WR/collateral.xlsx")

    elif fund == "Heroics Volatility":
        sorted_df.write_excel("./history/HV/collateral.xlsx")
