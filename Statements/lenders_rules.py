# -*- coding: utf-8 -*-
"""
Data-driven lender selector (converted from lender-rules JSON).

Expected inputs:
- app: {
    "business_name": str|None,
    "state": str|None,             # e.g. 'CA'
    "industry": str|None,          # free text
    "fico": int|float|str|None,    # numeric-ish
    "length_months": float|int|None
  }

- bank: {
    "avg_revenue": float|int|None,       # average monthly deposits excluding Zelle
    "avg_daily_balance": float|int|None,
    "neg_days": int|None,                # (per-month cap in some rules; if only aggregate is available, pass it here)
    "deposit_freq": float|int|None,      # average credit count per month
    "positions": int|None                # current open positions (MCA)
  }

- statements_count: int  (not used by most JSON lenders; you may enforce state-based month counts in your API/UI)

Returns:
  select_lenders(app, bank, statements_count) -> List[{"business_name","score","reason"}]
"""

from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple

# ---------------------------
# 1) Rules converted from JSON (every lender present)
# ---------------------------
LENDERS: Dict[str, Dict[str, Any]] = {
    "AmericanChoice": {
        "positionsAccepted": [2,3,4,5,6,7,8,9,10],
        "minFICO": 550, "minRevenueMonthly": 15000, "minTIB": 12,
        "maxNegativeDaysPerMonth": 10, "minDepositsPerMonth": 5, "minADB": 1000,
        "fundingRange": {"min": 10000, "max": 1000000},
        "restrictedIndustries": [
            "Non-Profits","Credit Protection","Collection Agencies","Debt Buyers",
            "Tax Reduction Services","Mailing Houses","Title Companies",
            "Loan/Finance Companies","Real Estate","Adult Entertainment"
        ],
        "restrictedStates": [],
        "conditionalDeclineRules": [
            {"industry":"trucking","revenueLT":100000},
            {"industry":"construction","revenueLT":50000},
            {"industry":"auto sales","revenueLT":100000},
        ],
    },
    "Alternative": {
        "positionsAccepted":[1,2,3,4],
        "minFICO":550,"minRevenueMonthly":20000,"minTIB":12,
        "maxNegativeDaysPerMonth":4,"minDepositsPerMonth":5,"minADB":1000,
        "fundingRange":{"min":5000,"max":750000},
        "restrictedIndustries":[
            "Bail Bonds","Cannabis","Check Cashing","Crypto","Stockbrokers",
            "Financial Firms","Lawyers","Auto Sales","Mechanic","Real Estate"
        ],
        "restrictedStates":["CA"],
        "conditionalDeclineRules":[
            {"industry":"construction","revenueLT":75000},
            {"industry":"trucking","revenueLT":75000},
        ],
    },
    "Arena": {
        "positionsAccepted":[1,2,3,4],
        "minFICO":600,"minRevenueMonthly":20000,"minTIB":9,
        "minDepositsPerMonth":5,"minADB":1000,"maxNegativeDaysPerMonth":10,
        "fundingRange":{"min":5000,"max":300000},
        "restrictedIndustries":[
            "Auto Sales","ATM Machines","Brokerage/Sales","Cannabis","Check Cashing",
            "Collections","Construction with limited AR","Credit Repair","Transportation"
        ],
        "restrictedStates":[],
        "conditionalDeclineRules":[
            {"industry":"construction","revenueLT":50000},
            {"industry":"transportation","revenueLT":50000},
        ],
    },
    "Aspire": {
        "positionsAccepted":[1,2],
        "minFICO":500,"minRevenueMonthly":8333,"minTIB":9,
        "minDepositsPerMonth":3,"minADB":1000,"maxNegativeDaysPerMonth":6,
        "fundingRange":{"min":5000,"max":150000},
        "restrictedIndustries":["Check Cashing","Financial Services","Online Gambling"],
        "restrictedStates":["NY"],
        "conditionalDeclineRules":[]
    },
    "BarclaysAdvance": {
        "positionsAccepted":[2,3,4],
        "minFICO":500,"minRevenueMonthly":30000,"minTIB":6,
        "minDepositsPerMonth":5,"minADB":1000,"maxNegativeDaysPerMonth":10,
        "fundingRange":{"min":10000,"max":1000000},
        "restrictedIndustries":["Financial Services","Casinos"],
        "restrictedStates":[],
        "conditionalDeclineRules":[]
    },
    "BCA": {
        "positionsAccepted":[2,3,4,5],
        "minFICO":500,"minRevenueMonthly":20000,"minTIB":6,
        "minDepositsPerMonth":5,"minADB":1000,"maxNegativeDaysPerMonth":10,
        "fundingRange":{"min":5000,"max":100000},
        "restrictedIndustries":[
            "Adult Entertainment","Cannabis","Collections","Credit Repair",
            "Cruise Lines","Crypto","Debt Consolidation","Foreign Entities",
            "Gambling","MLMs","Non-Profits"
        ],
        "restrictedStates":["CA","NY","VA"],
        "conditionalDeclineRules":[
            {"industry":"car sales","revenueLT":50000},
            {"industry":"construction","revenueLT":50000},
            {"industry":"jewelry","revenueLT":50000},
            {"industry":"insurance","revenueLT":50000},
            {"industry":"staffing","revenueLT":50000},
            {"industry":"real estate","revenueLT":50000},
            {"industry":"trucking","revenueLT":50000},
        ],
    },
    "Biz2Credit": {
        "positionsAccepted":[1],
        "minFICO":600,"minRevenueMonthly":40000,"minTIB":24,
        "minDepositsPerMonth":5,"minADB":1000,"maxNegativeDaysPerMonth":10,
        "fundingRange":{"min":10000,"max":300000},
        "restrictedIndustries":["Construction","Used Car Dealership","Trucking"],
        "restrictedStates":[],
        "conditionalDeclineRules":[
            {"industry":"trucking","revenueLT":100000},
        ],
    },
    "BroadwayAdvance": {
        "positionsAccepted":[1,2,3,4],
        "minFICO":500,"minRevenueMonthly":5000,"minTIB":12,
        "minDepositsPerMonth":6,"minADB":500,"maxNegativeDaysPerMonth":4,
        "fundingRange":{"min":5000,"max":300000},
        "restrictedIndustries":[
            "Bail Bonds","Auto Sales","Entertainment","Check Cashing","Law Firms",
            "Construction","Real Estate","Financial Product Companies","Cannabis"
        ],
        "restrictedStates":[],
        "conditionalDeclineRules":[]
    },
    "BCP": {
        "positionsAccepted":[1,2,3],
        "minFICO":500,"minRevenueMonthly":10000,"minTIB":6,
        "minDepositsPerMonth":5,"minADB":1000,"maxNegativeDaysPerMonth":10,
        "fundingRange":{"min":10000,"max":500000},
        "restrictedIndustries":[],
        "restrictedStates":["CA"],
        "conditionalDeclineRules":[]
    },
    "Capybara": {
        "positionsAccepted":[2,3,4],
        "minFICO":500,"minRevenueMonthly":25000,"minTIB":12,
        "minDepositsPerMonth":4,"minADB":1000,"maxNegativeDaysPerMonth":10,
        "fundingRange":{"min":10000,"max":500000},
        "restrictedIndustries":[
            "Staffing","Non-Profits","Law Firms","Banks","Trucking",
            "House Flippers","Auto Sales"
        ],
        "restrictedStates":["CA","VA","UT"],
        "conditionalDeclineRules":[]
    },
    "CapytalNewco": {
        "positionsAccepted":[2,3,4],
        "minFICO":550,"minRevenueMonthly":15000,"minTIB":12,
        "minDepositsPerMonth":4,"minADB":1000,"maxNegativeDaysPerMonth":10,
        "fundingRange":{"min":10000,"max":1000000},
        "restrictedIndustries":[],
        "restrictedStates":[],
        "conditionalDeclineRules":[]
    },
    "Cashfloit": {
        "positionsAccepted":[1,2,3],
        "minFICO":500,"minRevenueMonthly":35000,"minTIB":12,
        "minDepositsPerMonth":8,"minADB":1000,"maxNegativeDaysPerMonth":10,
        "fundingRange":{"min":15000,"max":200000},
        "restrictedIndustries":[
            "Adult","Cannabis","Construction","Financial Services","Real Estate","Trucking"
        ],
        "restrictedStates":["AK","HI","NY","UT","VA"],
        "conditionalDeclineRules":[
            {"industry":"construction","revenueLT":200000},
            {"industry":"trucking","revenueLT":100000},
        ],
    },
    "Cashable": {
        "positionsAccepted":[2,3,4,5,6,7,8,9,10],
        "minFICO":500,"minRevenueMonthly":25000,"minTIB":12,
        "minDepositsPerMonth":5,"minADB":1000,"maxNegativeDaysPerMonth":10,
        "fundingRange":{"min":10000,"max":500000},
        "restrictedIndustries":[
            "Auto Sales","Visa/Passport Services","Financial Services",
            "Non-Profits","All Cash/Zelle Deposit Businesses","1 Truck Transportation/Trucking"
        ],
        "restrictedStates":["HI","AK","PR","Canada"],
        "conditionalDeclineRules":[]
    },
    "CreationCapital": {
        "positionsAccepted":[1,2,3,4,5,6,7],
        "minFICO":500,"minRevenueMonthly":30000,"minTIB":6,
        "minDepositsPerMonth":5,"minADB":1000,"maxNegativeDaysPerMonth":10,
        "fundingRange":{"min":10000,"max":250000},
        "restrictedIndustries":["Gambling","Auto Sales","Religious Institutions","Pay Day Loans"],
        "restrictedStates":["CA"],
        "conditionalDeclineRules":[
            {"industry":"trucking","revenueLT":100000},
        ],
    },
    "Cobalt": {
        "positionsAccepted":[1,2,3,4],
        "minFICO":600,"minRevenueMonthly":40000,"minTIB":12,
        "minDepositsPerMonth":5,"minADB":1000,"maxNegativeDaysPerMonth":10,
        "fundingRange":{"min":10000,"max":3000000},
        "restrictedIndustries":["Real Estate","Check Cashing","Jewelry","Bail Bonds","Lawyers"],
        "restrictedStates":[],
        "conditionalDeclineRules":[
            {"industry":"construction","revenueLT":150000},
            {"industry":"trucking","revenueLT":150000},
            {"industry":"auto sales","revenueLT":500000},
        ],
    },
    "EAdvance": {
        "positionsAccepted":[1],
        "minFICO":500,"minRevenueMonthly":30000,"minTIB":6,
        "minDepositsPerMonth":3,"minADB":1000,"maxNegativeDaysPerMonth":5,
        "fundingRange":{"min":20000,"max":250000},
        "restrictedIndustries":[
            "Auto Sales","Check Cashing","Consulting","Financial Services",
            "Gas Stations","Property Management","Real Estate","Roofing"
        ],
        "restrictedStates":["CA","Montana","UT"],
        "conditionalDeclineRules":[]
    },
    "Eminent": {
        "positionsAccepted":[2,3,4,5,6,7,8,9,10],
        "minFICO":500,"minRevenueMonthly":50000,"minTIB":12,
        "minDepositsPerMonth":5,"minADB":1000,"maxNegativeDaysPerMonth":10,
        "fundingRange":{"min":10000,"max":500000},
        "restrictedIndustries":["Used Auto","Financial Companies"],
        "restrictedStates":[],
        "conditionalDeclineRules":[]
    },
    "EverestVaderGranite": {
        "positionsAccepted":[3,4,5,6,7,8,9,10],
        "minFICO":550,"minRevenueMonthly":5000,"minTIB":3,
        "minDepositsPerMonth":5,"minADB":1000,"maxNegativeDaysPerMonth":5,
        "fundingRange":{"min":2600,"max":15000},
        "restrictedIndustries":["Financial Institutions","Auto Sales","Attorneys"],
        "restrictedStates":[],
        "conditionalDeclineRules":[]
    },
    "Fenix": {
        "positionsAccepted":[1,2,3],
        "minFICO":500,"minRevenueMonthly":15000,"minTIB":12,
        "minDepositsPerMonth":5,"minADB":1000,"maxNegativeDaysPerMonth":10,
        "fundingRange":{"min":7500,"max":150000},
        "restrictedIndustries":[
            "Auto Dealerships","Bail Bonds","Check Cashing","Collections","Gambling",
            "Law Firms","Oil Services","Gas Stations","Trucking","Home-based Construction"
        ],
        "restrictedStates":["CA","Hawaii","Alaska","Puerto Rico"],
        "conditionalDeclineRules":[]
    },
    "Fintap": {
        "positionsAccepted":[1,2,3],
        "minFICO":600,"minRevenueMonthly":20000,"minTIB":24,
        "minDepositsPerMonth":5,"minADB":1000,"maxNegativeDaysPerMonth":10,
        "fundingRange":{"min":10000,"max":750000},
        "restrictedIndustries":["Auto Sales","Legal Services","Financial Services","THC"],
        "restrictedStates":[],
        "conditionalDeclineRules":[]
    },
    "MINTFunding": {
        "positionsAccepted":[1,2,3,4,5,6,7,8,9],
        "minFICO":550,"minRevenueMonthly":25000,"minTIB":9,
        "minDepositsPerMonth":5,"minADB":1000,"maxNegativeDaysPerMonth":5,
        "fundingRange":{"min":5000,"max":2500000},
        "restrictedIndustries":[
            "Auto Sales","Cannabis","Financial Services","Law Firms","Non-Profit",
            "Solar","Staffing","Real Estate"
        ],
        "restrictedStates":[],
        "conditionalDeclineRules":[
            {"industry":"construction","revenueLT":50000},
            {"industry":"trucking","revenueLT":50000},
        ],
    },
    "MNY": {
        "positionsAccepted":[3,4,5],
        "minFICO":600,"minRevenueMonthly":75000,"minTIB":12,
        "minDepositsPerMonth":5,"minADB":1000,"maxNegativeDaysPerMonth":4,
        "fundingRange":{"min":25000,"max":500000},
        "restrictedIndustries":[],
        "restrictedStates":[],
        "conditionalDeclineRules":[
            {"industry":"restaurants","revenueLT":75000},
        ],
    },
    "Moneywell": {
        "positionsAccepted":[2,3,4,5,6,7],
        "minFICO":500,"minRevenueMonthly":100000,"minTIB":6,
        "minDepositsPerMonth":5,"minADB":1000,"maxNegativeDaysPerMonth":1,
        "fundingRange":{"min":50000,"max":3500000},
        "restrictedIndustries":["Financial Services","Law Offices"],
        "restrictedStates":["NY"],
        "conditionalDeclineRules":[]
    },
    "MrAdvance": {
        "positionsAccepted":[1,2,3,4,5],
        "minFICO":550,"minRevenueMonthly":0,"minTIB":12,
        "minDepositsPerMonth":5,"minADB":1000,"maxNegativeDaysPerMonth":10,
        "fundingRange":{"min":5000,"max":500000},
        "restrictedIndustries":[
            "Gas Station","Auto Sales","Real Estate","Insurance",
            "Property Management","Debt Collectors","Trucking/Transportation"
        ],
        "restrictedStates":[],
        "conditionalDeclineRules":[]
    },
    "NitroAdvance": {
        "positionsAccepted":[1,2,3,4,5,6,7],
        "minFICO":500,"minRevenueMonthly":35000,"minTIB":12,
        "minDepositsPerMonth":5,"minADB":1000,"maxNegativeDaysPerMonth":10,
        "fundingRange":{"min":10000,"max":2000000},
        "restrictedIndustries":[
            "Auto Sales","Bail Bonds","Billing Services","Check Cashing",
            "Churches","Collection Agencies","Credit Repair","Currency Exchange",
            "Insurance Brokers","Telephone Order","Mortgage Broker"
        ],
        "restrictedStates":[],
        "conditionalDeclineRules":[
            {"industry":"trucking","revenueLT":200000},
            {"industry":"construction","revenueLT":200000},
        ],
    },
    "Ocean": {
        "positionsAccepted":[2,3,4],
        "minFICO":500,"minRevenueMonthly":30000,"minTIB":8,
        "minDepositsPerMonth":5,"minADB":1000,"maxNegativeDaysPerMonth":0,
        "fundingRange":{"min":10000,"max":750000},
        "restrictedIndustries":["Casinos","Financial Brokers","Funding Institutions"],
        "restrictedStates":["CA","NY"],
        "conditionalDeclineRules":[]
    },
    "Radiance": {
        "positionsAccepted":[1,2,3],
        "minFICO":520,"minRevenueMonthly":20000,"minTIB":6,
        "minDepositsPerMonth":4,"minADB":1000,"maxNegativeDaysPerMonth":4,
        "fundingRange":{"min":10000,"max":250000},
        "restrictedIndustries":[
            "Law Firms","Auto Sales","Travel Agents","Trucking","Brokers","Nonprofits","Cannabis","Oil","Gas"
        ],
        "restrictedStates":[],
        "conditionalDeclineRules":[]
    },
    "Rapid": {
        "positionsAccepted":[1],
        "minFICO":550,"minRevenueMonthly":0,"minTIB":24,
        "minDepositsPerMonth":5,"minADB":1000,"maxNegativeDaysPerMonth":10,
        "fundingRange":{"min":5000,"max":500000},
        "restrictedIndustries":["Auto Sales","Land Development"],
        "restrictedStates":["NJ","RI","NV","VT","SD","ND","MT"],
        "conditionalDeclineRules":[]
    },
    "RightAwayCapital": {
        "positionsAccepted":[1,2,3,4,5,6,7,8],
        "minFICO":540,"minRevenueMonthly":50000,"minTIB":12,
        "minDepositsPerMonth":5,"minADB":1000,"maxNegativeDaysPerMonth":5,
        "fundingRange":{"min":50000,"max":1000000},
        "restrictedIndustries":[],
        "restrictedStates":[],
        "conditionalDeclineRules":[
            {"industry":"trucking","revenueLT":150000},
            {"industry":"construction","revenueLT":150000},
            {"industry":"autosales","revenueLT":150000},
        ],
    },
    "RocketCap": {
        "positionsAccepted":[1,2],
        "minFICO":500,"minRevenueMonthly":40000,"minTIB":12,
        "minDepositsPerMonth":5,"minADB":1000,"maxNegativeDaysPerMonth":7,
        "fundingRange":{"min":15000,"max":1500000},
        "restrictedIndustries":["Auto Sales","Cannabis","Logistics","Non-Profits","Trucking/Transportation"],
        "restrictedStates":[],
        "conditionalDeclineRules":[]
    },
    "Rowan": {
        "positionsAccepted":[2,3],
        "minFICO":550,"minRevenueMonthly":20000,"minTIB":12,
        "minDepositsPerMonth":2,"minADB":1000,"maxNegativeDaysPerMonth":4,
        "fundingRange":{"min":5000,"max":300000},
        "restrictedIndustries":[
            "Nail Salons","Law Firms/Attorneys","Financial Services","Auto Sales",
            "Non-Profits","Gambling & Gaming","Gas Stations","Real Estate Brokers/Agents/Flippers"
        ],
        "restrictedStates":[],
        "conditionalDeclineRules":[]
    },
    "SFS": {
        "positionsAccepted":[2,3,4,5,6],
        "minFICO":550,"minRevenueMonthly":40000,"minTIB":12,
        "minDepositsPerMonth":4,"minADB":1000,"maxNegativeDaysPerMonth":7,
        "fundingRange":{"min":15000,"max":250000},
        "restrictedIndustries":[],
        "restrictedStates":[],
        "conditionalDeclineRules":[
            {"industry":"auto","revenueLT":200000},
            {"industry":"law_firm","revenueLT":50000},
            {"industry":"commercial_construction","revenueLT":150000},
            {"industry":"trucking","revenueLT":100000},
            {"industry":"real_estate","revenueLT":75000},
        ],
    },
    "SmartStep": {
        "positionsAccepted":[1,2],
        "minFICO":500,"minRevenueMonthly":10000,"minTIB":24,
        "maxNegativeDaysPerMonth":5,
        "fundingRange":{},
        "restrictedIndustries":[
            "Shipping","Internet-only businesses","Consultants","Accounting","Financial Services",
            "Insurance","Nonprofits","Advertising/Marketing","Mining","Auto Dealerships",
            "Brokers","Building Managers","Engineering Services","Hospice","Manufacturing",
            "Movie Production","Nail Salons","Oil/Gas","Gas Stations","Real Estate Management",
            "Retail Electronics","Used Merchandise Resale","Staffing Services","Telecommunications",
            "Trucking","Transportation Services"
        ],
        "restrictedStates":[],
        "conditionalDeclineRules":[]
    },
    "SmarterMerchant": {
        "positionsAccepted":[1,2,3,4,5,6],
        "minFICO":500,"minRevenueMonthly":30000,"minTIB":12,
        "maxNegativeDaysPerMonth":6,
        "fundingRange":{"max":250000},
        "restrictedIndustries":[],
        "restrictedStates":["CA"],
        "conditionalDeclineRules":[
            {"industry":"construction","revenueLT":75000},
            {"industry":"trucking","revenueLT":75000},
        ],
    },
    "Spartan": {
        "positionsAccepted":[1,2,3,4,5],
        "minFICO":550,"minRevenueMonthly":15000,"minTIB":12,
        "maxNegativeDaysPerMonth": None,
        "fundingRange":{},
        "restrictedIndustries":[
            "Non-profit","Marijuana","Auto/Recreational Dealers","Guns/Firearms Vendors",
            "Loan Brokers","Financial Services"
        ],
        "restrictedStates":["CA"],
        "conditionalDeclineRules":[
            {"industry":"trucking","revenueLT":200000},
            {"industry":"real estate","revenueLT":100000},
            {"industry":"auto/rv/boat sales","revenueLT":100000},
        ],
    },
    "SuperfastCap": {
        "positionsAccepted":[1,2],
        "minFICO":500,"minRevenueMonthly":None,"minTIB":12,
        "maxNegativeDaysPerMonth":None,
        "fundingRange":{},
        "restrictedIndustries":[
            "Construction","Transportation","Trucking","Property Management","Real Estate Agents","Car Sales","Non-Profits"
        ],
        "restrictedStates":[],
        "conditionalDeclineRules":[]
    },
    "Torro": {
        "positionsAccepted":[2,3,4,5,6,7,8],
        "minFICO":500,"minRevenueMonthly":10000,"minTIB":12,
        "maxNegativeDaysPerMonth":4,
        "fundingRange":{"min":5000,"max":250000},
        "restrictedIndustries":["Transportation","Trucking","Financial Services"],
        "restrictedStates":["NY"],
        "conditionalDeclineRules":[]
    },
    "TVT": {
        "positionsAccepted":[1,2],
        "minFICO":600,"minRevenueMonthly":40000,"minTIB":12,
        "maxNegativeDaysPerMonth":None,
        "fundingRange":{"min":150000},
        "restrictedIndustries":[],
        "restrictedStates":[],
        "conditionalDeclineRules":[
            {"industry":"trucking","revenueLT":350000},
            {"industry":"construction","revenueLT":350000},
        ],
    },
    "Vader": {
        "positionsAccepted":[3,4,5,6,7,8,9,10],
        "minFICO":500,"minRevenueMonthly":4000,"minTIB":1,
        "maxNegativeDaysPerMonth":7,
        "fundingRange":{"min":2600,"max":15000},
        "restrictedIndustries":["Financial Institutions","Auto Sales","Attorneys"],
        "restrictedStates":[],
        "conditionalDeclineRules":[]
    },
    "Velocity": {
        "positionsAccepted":[1,2],
        "minFICO":500,"minRevenueMonthly":20000,"minTIB":6,
        "maxNegativeDaysPerMonth":None,
        "fundingRange":{},
        "restrictedIndustries":["Transportation","Trucking"],
        "restrictedStates":[],
        "conditionalDeclineRules":[]
    },
    "Vivian": {
        "positionsAccepted":[1,2,3,4,5,6,7,8,9],
        "minFICO":550,"minRevenueMonthly":25000,"minTIB":None,
        "maxNegativeDaysPerMonth":None,
        "fundingRange":{},
        "restrictedIndustries":["CPA","Cannabis"],
        "restrictedStates":["CA","NY"],
        "conditionalDeclineRules":[]
    },
    "Vox": {
        "positionsAccepted":[1],
        "minFICO":500,"minRevenueMonthly":10000,"minTIB":12,
        "maxNegativeDaysPerMonth":5,
        "fundingRange":{},
        "restrictedIndustries":[
            "Adult Entertainment","Cash Advance Companies","Credit Card Protection","Escort Services",
            "Interior Design","Real Estate","Childcare Facility","Mortgage Lenders","Marijuana Shops",
            "Pawn Shops","Check Cashing","Used Auto Dealer","Sole Proprietors","Wire Transfer Company",
            "State or Gov’t Agency"
        ],
        "restrictedStates":[],
        "conditionalDeclineRules":[
            {"industry":"trucking","revenueLT":300000},
            {"industry":"construction","revenueLT":90000},
        ],
    },
    "Wall": {
        "positionsAccepted":[1],
        "minFICO":500,"minRevenueMonthly":50000,"minTIB":6,
        "maxNegativeDaysPerMonth":None,
        "fundingRange":{},
        "restrictedIndustries":[],
        "restrictedStates":[],
        "conditionalDeclineRules":[]
    },
    "Wellen": {
        "positionsAccepted":[1],
        "minFICO":525,"minRevenueMonthly":10000,"minTIB":24,
        "maxNegativeDaysPerMonth":None,
        "fundingRange":{"min":10000,"max":100000},
        "restrictedIndustries":[
            "Accounting Services","Adult Content","Agriculture","Arts & Entertainment",
            "Attorneys","Auto Sales","Consulting","Financial Services","Firearms","Gaming",
            "Gas Stations","Lenders","Marijuana Dispensaries","Limo Companies","Mining",
            "Multi-level Marketing","Non-Profit","Public Administration","Real Estate",
            "Trucking","Utilities","Vape Shops","Wholesale"
        ],
        "restrictedStates":["UT","VA"],
        "conditionalDeclineRules":[]
    },
    "WGFinancing": {
        "positionsAccepted":[1,2,3,4,5,6,7,8],
        "minFICO":500,"minRevenueMonthly":25000,"minTIB":6,
        "maxNegativeDaysPerMonth":None,
        "fundingRange":{},
        "restrictedIndustries":[],
        "restrictedStates":["CA"],
        "conditionalDeclineRules":[]
    },
    "LastChance": {
        "positionsAccepted":[1,2,3,4,5,6,7,8,9,10],
        "minFICO":0,"minRevenueMonthly":6000,"minTIB":3,
        "maxNegativeDaysPerMonth":None,
        "fundingRange":{"min":2000,"max":2000000},
        "restrictedIndustries":[],
        "restrictedStates":[],
        "conditionalDeclineRules":[],
        "preferredIndustries":[
            "Hotel","Spa","Beauty Salons","Restaurants","Coffee Shops","Quick Service Food",
            "Liquor Store","Pet Shops","Florist","Rehabilitation Center","Physicians","Surgeons","Dentists"
        ],
    },
}

# ---------------------------
# 2) Matching engine
# ---------------------------

def _norm_state(s: Optional[str]) -> str:
    return (s or "").strip().upper()[:2]

def _to_int(x) -> Optional[int]:
    try:
        return int(float(x))
    except Exception:
        return None

def _to_float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def _norm_industry_text(s: Optional[str]) -> str:
    return (s or "").strip().lower()

def _ind_match(industry_text: str, needle: str) -> bool:
    """
    Case-insensitive substring match with a few normalizations so
    'auto sales', 'autosales', 'auto/rv/boat sales' all stand a chance.
    """
    hay = industry_text.replace("/", " ").replace("-", " ").replace("&", " ")
    hay = " ".join(hay.split())
    ndl = needle.strip().lower().replace("/", " ").replace("-", " ")
    ndl = " ".join(ndl.split())
    return ndl in hay

def _positions_ok(accepted: Optional[List[int]], positions: Optional[int]) -> Tuple[bool, str]:
    if not accepted:
        return (True, "OK")
    if positions is None:
        # if unknown, don't auto-fail—treat as soft-miss
        return (True, "OK")
    return (positions in set(accepted), f"Positions {positions} not in {accepted}")

def _min_gate(name: str, actual: Optional[float], threshold: Optional[float]) -> Tuple[bool, str]:
    if threshold is None:
        return (True, "OK")
    if actual is None:
        return (False, f"{name} missing")
    return (actual >= threshold, f"{name} < {threshold:g}")

def _max_gate(name: str, actual: Optional[float], threshold: Optional[float]) -> Tuple[bool, str]:
    if threshold is None:
        return (True, "OK")
    if actual is None:
        return (False, f"{name} missing")
    return (actual <= threshold, f"{name} > {threshold:g}")

def _declines_by_industry(restricted: List[str], ind_text: str) -> Optional[str]:
    for r in restricted or []:
        if _ind_match(ind_text, r):
            return f"Restricted industry: {r}"
    return None

def _conditional_decline(rules: List[Dict[str, Any]], ind_text: str, avg_rev: Optional[float]) -> Optional[str]:
    if not rules:
        return None
    for rule in rules:
        needle = str(rule.get("industry") or "").strip()
        limit = _to_float(rule.get("revenueLT"))
        if needle and limit is not None and _ind_match(ind_text, needle):
            if (avg_rev or 0.0) < limit:
                return f"Revenue < {limit:g} for industry '{needle}'"
    return None

def _bonus_preferred(preferred: List[str], ind_text: str) -> float:
    for p in preferred or []:
        if _ind_match(ind_text, p):
            return 0.05
    return 0.0

def _evaluate_one(name: str, rule: Dict[str, Any], app: Dict[str, Any], bank: Dict[str, Any]) -> Tuple[bool, float, str]:
    reasons: List[str] = []

    state = _norm_state(app.get("state"))
    fico  = _to_int(app.get("fico"))
    tib_m = _to_float(app.get("length_months"))
    if tib_m is None:
        tib_m = _coerce_length_months_from_string(app.get("length_of_ownership") or app.get("LengthOfOwnership") or "")
    ind   = _norm_industry_text(app.get("industry"))

    avg_rev = _to_float(bank.get("avg_revenue"))
    adb     = _to_float(bank.get("avg_daily_balance"))
    negd    = _to_int(bank.get("neg_days"))
    depf    = _to_float(bank.get("deposit_freq"))
    pos     = _to_int(bank.get("positions"))

    # state restrictions
    rst = rule.get("restrictedStates") or []
    if state and state in set(rst):
        return (False, 0.0, f"State restricted: {state}")

    # industry hard blocks
    ri = rule.get("restrictedIndustries") or []
    msg = _declines_by_industry(ri, ind)
    if msg:
        return (False, 0.0, msg)

    # positions
    ok, why = _positions_ok(rule.get("positionsAccepted"), pos)
    if not ok:
        return (False, 0.0, why)

    # numeric gates
    gates = [
        _min_gate("FICO", fico, _to_int(rule.get("minFICO"))),
        _min_gate("Monthly revenue", avg_rev, _to_float(rule.get("minRevenueMonthly"))),
        _min_gate("TIB months", tib_m, _to_float(rule.get("minTIB"))),
        _min_gate("Deposits/mo", depf, _to_float(rule.get("minDepositsPerMonth"))),
        _min_gate("ADB", adb, _to_float(rule.get("minADB"))),
        _max_gate("Negative days/mo", negd, _to_float(rule.get("maxNegativeDaysPerMonth"))),
    ]
    for ok, why in gates:
        if not ok:
            return (False, 0.0, why)

    # conditional declines (industry×revenue)
    cd_msg = _conditional_decline(rule.get("conditionalDeclineRules") or [], ind, avg_rev)
    if cd_msg:
        return (False, 0.0, cd_msg)

    # eligible → score
    score = 1.0 + _bonus_preferred(rule.get("preferredIndustries") or [], ind)
    return (True, score, "OK")

# ---------------------------
# 3) Public API
# ---------------------------

def generate_lenders(application: Dict[str, Any], statements: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Convenience adapter so you can call this like your old rules:

      matches = generate_lenders(app_dict, statements_payload)

    We'll derive 'bank' features from the `statements` payload if possible.
    """
    # Try to derive bank features from your consolidated statements payload
    months = len((statements or {}).get("monthly_deposits") or {})
    agg_credits = (statements or {}).get("aggregate_credit_count")
    deposit_freq = (agg_credits / months) if (months and agg_credits is not None) else None

    bank = {
        "avg_revenue": (statements or {}).get("average_revenue"),
        "avg_daily_balance": (statements or {}).get("average_daily_balance"),
        "neg_days": (statements or {}).get("aggregate_negative_days"),
        "deposit_freq": deposit_freq,
        # positions count is not derivable here; pass via application if you have it
        "positions": application.get("positions") if isinstance(application, dict) else None,
    }
    return select_lenders(application, bank, statements_count=months)

def select_lenders(app: Dict[str, Any], bank: Dict[str, Any], statements_count: int = 0) -> List[Dict[str, Any]]:
    """
    Core matcher. Returns a list of dicts shaped for your UI:
      [{"business_name": <LenderName>, "score": 1.0, "reason": "OK"}, ...]
    """
    out: List[Dict[str, Any]] = []
    for name, rule in LENDERS.items():
        ok, score, reason = _evaluate_one(name, rule, app, bank)
        out.append({"business_name": name, "score": float(score if ok else 0.0), "reason": reason})

    # Sort: eligible (score>0) first, by score desc, then name
    out.sort(key=lambda x: (x["score"] <= 0.0, -x["score"], x["business_name"]))
    return out

def _coerce_length_months_from_string(s: str) -> Optional[float]:
    if not isinstance(s, str) or not s.strip():
        return None
    s_low = s.lower()
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*months?", s_low)
    if m:
        try: return float(m.group(1))
        except: pass
    y = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*years?", s_low)
    if y:
        try: return float(y.group(1)) * 12.0
        except: pass
    y2 = re.search(r"\(([0-9]+(?:\.[0-9]+)?)\s*years?\)", s_low)
    if y2:
        try: return float(y2.group(1)) * 12.0
        except: pass
    return None

# Optional: simple CLI for quick local tests
if __name__ == "__main__":
    sample_app = {"state":"NY","industry":"restaurants","fico":620,"length_months":18}
    sample_bank = {"avg_revenue":30000,"avg_daily_balance":2500,"neg_days":2,"deposit_freq":12,"positions":2}
    res = select_lenders(sample_app, sample_bank, statements_count=4)
    for r in res[:10]:
        print(r)
