import datetime


TO_NUMBER = lambda n: float(n)


def TO_DATE(s):
    # Check number of hypens to understand formatting: 0 >> just year, 1 >> year-month, 2 >> year-month-day
    if s.count("-") == 2:
        return datetime.datetime.strptime(s, "%Y-%m-%d")
    if s.count("-") == 1:
        return datetime.datetime.strptime(s, "%Y-%m")
    if s.count("-") == 0:
        return datetime.datetime.strptime(s, "%Y")


def asset_schema():
    """
    Return validation schema for ASSETS data type.
    """
    schema = {
        "AS1": {
            "type": "datetime",
            "coerce": TO_DATE,
            "min": datetime.datetime(2012, 1, 1, 0, 0),
            "max": datetime.datetime(2030, 12, 31, 0, 0),
            "meta": {"label": "Pool Cut-off Date"},
        },
        "AS2": {"type": "string", "meta": {"label": "Pool Identifier"}},
        "AS3": {"type": "string", "meta": {"label": "Loan Identifier"}},
        "AS4": {"type": "string", "meta": {"label": "Originator"}},
        "AS5": {"type": "string", "meta": {"label": "Servicer Identifier"}},
        "AS6": {"type": "string", "meta": {"label": "Servicer Name"}},
        "AS7": {"type": "string", "meta": {"label": "Borrower Identifier"}},
        "AS8": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Group Company Identifier"},
        },
        "AS15": {"type": "string", "meta": {"label": "Country"}},
        "AS16": {"type": "string", "meta": {"label": "Postcode "}},
        "AS17": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Geographic Region"},
        },
        "AS18": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5"],
            "nullable": True,
            "meta": {"label": "Obligor Legal Form / Business Type"},
        },
        "AS19": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime(2030, 12, 31, 0, 0),
            "nullable": True,
            "meta": {"label": "Obligor Incorporation Date"},
        },
        "AS20": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Obligor is a Customer since?"},
        },
        "AS22": {
            "type": "string",
            "allowed": ["1", "2", "3", "4"],
            "nullable": True,
            "meta": {"label": "Borrower Basel III Segment"},
        },
        "AS23": {
            "type": "string",
            "allowed": ["Y", "N"],
            "nullable": True,
            "meta": {"label": "Originator Affiliate?"},
        },
        "AS24": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Obligor Tax Code"},
        },
        "AS25": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5", "6", "7", "8"],
            "nullable": True,
            "meta": {"label": "Asset Type"},
        },
        "AS26": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5"],
            "nullable": True,
            "meta": {"label": "Seniority"},
        },
        "AS27": {
            "type": "number",
            "nullable": True,
            "coerce": TO_NUMBER,
            "min": 0.0,
            "meta": {"label": "Total credit limit granted to the loan"},
        },
        "AS28": {
            "type": "number",
            "nullable": True,
            "coerce": TO_NUMBER,
            "min": 0.0,
            "meta": {"label": "Total Credit Limit Used"},
        },
        "AS29": {
            "type": "string",
            "allowed": ["Y", "N"],
            "nullable": True,
            "meta": {"label": "Syndicated"},
        },
        "AS30": {
            "type": "number",
            "nullable": True,
            "coerce": TO_NUMBER,
            "min": 0.0,
            "meta": {"label": "Bank Internal Rating"},
        },
        "AS31": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Last Internal Obligor Rating Review"},
        },
        "AS32": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "S&P Public Rating (equivalent)"},
        },
        "AS33": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Moody Public Rating (equivalent)"},
        },
        "AS34": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Fitch Public Rating (equivalent)"},
        },
        "AS35": {
            "type": "string",
            "nullable": True,
            "meta": {
                "label": "Dominion Bond Rating Service (DBRS) Public Rating (equivalent)"
            },
        },
        "AS36": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Other Public Rating"},
        },
        "AS37": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "meta": {"label": "Bank Internal Loss Given Default (LGD) Estimate"},
        },
        "AS38": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {
                "label": "Bank Internal Loss Given Default (LGD) Estimate (Down-Turn)"
            },
        },
        "AS39": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "S&P Industry Code"},
        },
        "AS40": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Moody Industry Code"},
        },
        "AS41": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Fitch Industry Code"},
        },
        "AS42": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "NACE Industry Code"},
        },
        "AS43": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Other Industry Code"},
        },
        "AS44": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Borrower deposit amount"},
        },
        "AS45": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Borrower deposit currency"},
        },
        "AS50": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Loan Origination Date"},
        },
        "AS51": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "meta": {"label": "Final Maturity Date"},
        },
        "AS52": {"type": "string", "meta": {"label": "Loan Denomination Currency"}},
        "AS53": {
            "type": "string",
            "allowed": ["Y", "N"],
            "meta": {"label": "Loan Hedged"},
        },
        "AS54": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "meta": {"label": "Original Loan Balance"},
        },
        "AS55": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "meta": {"label": "Current Balance"},
        },
        "AS56": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "meta": {"label": "Securitised Loan Amount"},
        },
        "AS57": {
            "type": "string",
            "allowed": [
                "1",
                "2",
                "3",
                "4",
                "5",
                "6",
                "7",
                "8",
                "9",
                "10",
                "11",
                "12",
                "13",
            ],
            "nullable": True,
            "meta": {"label": "Purpose"},
        },
        "AS58": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5", "6"],
            "nullable": True,
            "meta": {"label": "Principal Payment Frequency"},
        },
        "AS59": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5", "6"],
            "nullable": True,
            "meta": {"label": "Interest Payment Frequency"},
        },
        "AS60": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Maximum Balance"},
        },
        "AS61": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Weighted Average Life"},
        },
        "AS62": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5", "6", "7"],
            "meta": {"label": "Amortization Type"},
        },
        "AS63": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Regular Principal Instalment"},
        },
        "AS64": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Regular Interest Instalment"},
        },
        "AS65": {
            "type": "string",
            "allowed": ["1", "2", "3"],
            "nullable": True,
            "meta": {"label": "Type of Loan"},
        },
        "AS66": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "meta": {"label": "Balloon Amount"},
        },
        "AS67": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Next Payment Date"},
        },
        "AS68": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5"],
            "nullable": True,
            "meta": {"label": "Payment type"},
        },
        "AS69": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Prepayment Penalty"},
        },
        "AS70": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Principal Grace Period End Date"},
        },
        "AS71": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Interest Grace Period End Date"},
        },
        "AS80": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Current Interest Rate"},
        },
        "AS81": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Interest Cap Rate"},
        },
        "AS82": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Interest Floor Rate"},
        },
        "AS83": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"],
            "meta": {"label": "Interest Rate Type"},
        },
        "AS84": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"],
            "meta": {"label": "Current Interest Rate Index"},
        },
        "AS85": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "meta": {"label": "Current Interest Rate Margin"},
        },
        "AS86": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Revision Margin 1"},
        },
        "AS87": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Interest Revision Date 1"},
        },
        "AS88": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Revision Margin 2"},
        },
        "AS89": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Interest Revision Date 2"},
        },
        "AS90": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Revision Margin 3"},
        },
        "AS91": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Interest Revision Date 3"},
        },
        "AS92": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Revised Interest Rate Index"},
        },
        "AS93": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Final Margin"},
        },
        "AS94": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5", "6"],
            "nullable": True,
            "meta": {"label": "Interest Reset Period"},
        },
        "AS100": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Turnover of Obligor"},
        },
        "AS101": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Equity"},
        },
        "AS102": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Total Liabilities (excluding Equity)"},
        },
        "AS103": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Short Term Financial Debt "},
        },
        "AS104": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Commercial Liabilities "},
        },
        "AS105": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Long Term Debt"},
        },
        "AS106": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Financial Expenses"},
        },
        "AS107": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {
                "label": "Earnings Before Interest, Taxes, Depreciation and Amortisation (EBITDA) "
            },
        },
        "AS108": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Earnings Before Interest, Taxes (EBIT) "},
        },
        "AS109": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Net Profit"},
        },
        "AS110": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Number of Employees"},
        },
        "AS111": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Currency of Financials"},
        },
        "AS112": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Date of Financials"},
        },
        "AS115": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "meta": {"label": "Interest Arrears Amount"},
        },
        "AS116": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "meta": {"label": "Number of Days in Interest Arrears"},
        },
        "AS117": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "meta": {"label": "Principal Arrears Amount"},
        },
        "AS118": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "meta": {"label": "Number of Days in Principal Arrears"},
        },
        "AS119": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Loan Entered Arrears"},
        },
        "AS120": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Days in Arrears Prior"},
        },
        "AS121": {
            "type": "string",
            "allowed": ["Y", "N"],
            "nullable": True,
            "meta": {
                "label": "Default or Foreclosure on the loan per the transaction definition"
            },
        },
        "AS122": {
            "type": "string",
            "allowed": ["Y", "N"],
            "nullable": True,
            "meta": {
                "label": "Default or Foreclosure on the loan per Basel III definition"
            },
        },
        "AS123": {
            "type": "string",
            "allowed": ["1", "2", "3", "4"],
            "nullable": True,
            "meta": {"label": "Reason for Default (Basel II definition)"},
        },
        "AS124": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Default Date"},
        },
        "AS125": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "meta": {"label": "Default Amount"},
        },
        "AS126": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Bank Internal Rating Prior to Default"},
        },
        "AS127": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Legal Proceedings Start Date"},
        },
        "AS128": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Cumulative Recoveries"},
        },
        "AS129": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5", "6"],
            "nullable": True,
            "meta": {"label": "Recovery Source"},
        },
        "AS130": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Work-out Process Started"},
        },
        "AS131": {
            "type": "string",
            "allowed": ["Y", "N"],
            "nullable": True,
            "meta": {"label": "Work-out Process Complete"},
        },
        "AS132": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "meta": {"label": "Allocated Losses"},
        },
        "AS133": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Redemption Date"},
        },
        "AS134": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Date Loss Allocated"},
        },
        "AS135": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Real Estate Sale Price"},
        },
        "AS136": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Total Proceeds from Other Collateral or Guarantees"},
        },
        "AS137": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Date of End of Work-out"},
        },
        "AS138": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Foreclosure Cost"},
        },
    }

    return schema


def collateral_schema():
    """
    Return validation schema for COLLATERALS data type.
    """
    schema = {
        "CS1": {"type": "string", "meta": {"label": "Collateral ID"}},
        "CS2": {"type": "string", "meta": {"label": "Loan Identifier"}},
        "CS3": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5"],
            "nullable": True,
            "meta": {"label": "Security Type"},
        },
        "CS4": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Collateral value"},
        },
        "CS5": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Collateralisation Ratio"},
        },
        "CS6": {
            "type": "string",
            "allowed": [
                "1",
                "2",
                "3",
                "4",
                "5",
                "6",
                "7",
                "8",
                "9",
                "10",
                "11",
                "12",
                "13",
                "14",
                "15",
                "16",
                "17",
                "18",
                "19",
                "20",
                "21",
                "22",
                "23",
            ],
            "nullable": True,
            "meta": {"label": "Collateral Type"},
        },
        "CS7": {
            "type": "string",
            "allowed": ["Y", "N"],
            "nullable": True,
            "meta": {"label": "Finished Property?"},
        },
        "CS8": {
            "type": "string",
            "allowed": ["Y", "N"],
            "nullable": True,
            "meta": {"label": "Licensed Property?"},
        },
        "CS9": {
            "type": "string",
            "allowed": ["Y", "N"],
            "nullable": True,
            "meta": {"label": "Asset Insurance"},
        },
        "CS10": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "meta": {"label": "Original Valuation Amount"},
        },
        "CS11": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Original Valuation Date"},
        },
        "CS12": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Current Valuation Date"},
        },
        "CS13": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5", "6", "7", "8", "9"],
            "nullable": True,
            "meta": {"label": "Original Valuation Type"},
        },
        "CS14": {
            "type": "string",
            "allowed": ["1", "2", "3"],
            "nullable": True,
            "meta": {"label": "Ranking"},
        },
        "CS15": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Prior Balances"},
        },
        "CS16": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Property Postcode"},
        },
        "CS17": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Geographic Region"},
        },
        "CS18": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Unconditional Personal Guarantee Amount "},
        },
        "CS19": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Unconditional Corporate /Third Party Guarantee Amount"},
        },
        "CS20": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Corporate Guarantor Identifier"},
        },
        "CS21": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {
                "label": "Corporate Guarantor Bank Internal 1 Year Probability Default"
            },
        },
        "CS22": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Corporate Guarantor Last Internal Rating Review"},
        },
        "CS23": {
            "type": "string",
            "allowed": ["1", "2", "3", "4"],
            "nullable": True,
            "meta": {"label": "Origination Channel / Arranging Bank or Division"},
        },
        "CS24": {"type": "string", "meta": {"label": "Collateral Currency"}},
        "CS25": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Personal Guarantee Currency"},
        },
        "CS26": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Corporate Guarantee Currency"},
        },
        "CS27": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Prior Balance Currency"},
        },
        "CS28": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "meta": {"label": "Number of Collateral Items Securing The Loan"},
        },
    }
    return schema


def bond_info_schema():
    """
    Return validation schema for BOND_INFO data type.
    """
    schema = {
        "BS1": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "meta": {"label": "Report Date"},
        },
        "BS2": {"type": "string", "meta": {"label": "Issuer"}},
        "BS3": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Ending Reserve Account Balance"},
        },
        "BS4": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Target Reserve Account Balance"},
        },
        "BS5": {
            "type": "string",
            "allowed": ["Y", "N"],
            "meta": {"label": "Drawings under Liquidity Facility"},
        },
        "BS6": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Currency of Reserve Account Balance"},
        },
        "BS11": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Excess Spread Amount"},
        },
        "BS12": {
            "type": "string",
            "allowed": ["Y", "N"],
            "meta": {"label": "Trigger Measurements/Ratios"},
        },
        "BS13": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "meta": {"label": "Average Constant Pre-payment Rate"},
        },
        "BS19": {"type": "string", "meta": {"label": "Point Contact"}},
        "BS20": {"type": "string", "meta": {"label": "Contact Information"}},
        "BS30": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Original Principal Balance"},
        },
        "BS31": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Total Ending Balance Subsequent to Payment"},
        },
        "BS33": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Relevant Margin"},
        },
        "BS34": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Coupon Reference Rate"},
        },
        "BS35": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Current Coupon"},
        },
        "BS36": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Cumulative Interest Shortfall "},
        },
        "BS37": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Cumulative Principal Shortfalls"},
        },
    }

    return schema


def amortisation_schema():
    """
    Return validation schema for AMORTISATION PROFILE data type.
    """
    schema = {"AS3": {"type": "string", "meta": {"label": "Loan Identifier"}}}
    for i in range(150, 270):
        if i % 2 == 0:
            schema[f"AS{i}"] = {
                "type": "number",
                "coerce": TO_NUMBER,
                "min": 0.0,
                "meta": {"label": f"Outstanding Balance Period {i-149}"},
            }
        else:
            schema[f"AS{i}"] = {
                "type": "datetime",
                "coerce": TO_DATE,
                "min": datetime.datetime(2012, 1, 1),
                "max": (datetime.datetime.now() + datetime.timedelta(days=13880)),
                "meta": {"label": f"Outstanding Balance Period {i-150} Date"},
            }
    return schema
