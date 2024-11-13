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
        "AR1": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "meta": {"label": "Pool Cut-off Date"},
        },
        "AR2": {
            "type": "string",
            "meta": {"label": "Pool Identifier"},
        },
        "AR3": {
            "type": "string",
            "meta": {"label": "Loan Identifier"},
        },
        "AR4": {
            "type": "string",
            "allowed": ["Y", "N"],
            "nullable": True,
            "meta": {"label": "Regulated Loan"},
        },
        "AR5": {"type": "string", "meta": {"label": "Originator"}},
        "AR6": {
            "type": "string",
            "meta": {"label": "Servicer Identifier"},
        },
        "AR7": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Borrower Identifier"},
        },
        "AR8": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Property Identifier"},
        },
        "AR15": {
            "type": "string",
            "allowed": ["1", "2", "3"],
            "nullable": True,
            "meta": {"label": "Borrower Type"},
        },
        "AR16": {
            "type": "string",
            "allowed": ["Y", "N"],
            "nullable": True,
            "meta": {"label": "Foreign National"},
        },
        "AR17": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Borrower Credit Quality"},
        },
        "AR18": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Borrower Year of Birth"},
        },
        "AR19": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Number of Debtors"},
        },
        "AR20": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Second Applicant Year of Birth"},
        },
        "AR21": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5", "6", "7", "8", "9"],
            "nullable": True,
            "meta": {"label": "Borrower's Employment Status"},
        },
        "AR22": {
            "type": "string",
            "allowed": ["Y", "N"],
            "nullable": True,
            "meta": {"label": "First-time Buyer "},
        },
        "AR23": {
            "type": "string",
            "allowed": ["Y", "N"],
            "nullable": True,
            "meta": {"label": "Right to Buy"},
        },
        "AR24": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Right to Buy Price"},
        },
        "AR25": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Class of Borrower"},
        },
        "AR26": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Primary Income"},
        },
        "AR27": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5"],
            "nullable": True,
            "meta": {"label": "Income Verification for Primary Income"},
        },
        "AR28": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Secondary Income"},
        },
        "AR29": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5"],
            "nullable": True,
            "meta": {"label": "Income Verification for Secondary Income"},
        },
        "AR30": {
            "type": "string",
            "allowed": ["1", "2", "3"],
            "nullable": True,
            "meta": {"label": "Resident"},
        },
        "AR31": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {
                "label": "Number of County Court Judgements or equivalent - Satisfied"
            },
        },
        "AR32": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {
                "label": "Value of County Court Judgements or equivalent - Satisfied"
            },
        },
        "AR33": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {
                "label": "Number of County Court Judgements or equivalent - Unsatisfied"
            },
        },
        "AR34": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {
                "label": "Value of County Court Judgements or equivalent - Unsatisfied"
            },
        },
        "AR35": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Last County Court Judgements or equivalent - Year"},
        },
        "AR36": {
            "type": "string",
            "allowed": ["Y", "N"],
            "nullable": True,
            "meta": {"label": "Bankruptcy or Individual Voluntary Arrangement Flag"},
        },
        "AR37": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5", "6", "7"],
            "nullable": True,
            "meta": {"label": "Bureau Krediet Registratie 1 to 10 - Credit Type"},
        },
        "AR38": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Bureau Krediet Registratie 1 to 10- Registration Date"},
        },
        "AR39": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5", "6"],
            "nullable": True,
            "meta": {"label": "Bureau Krediet Registratie 1 to 10 - Arrears Code"},
        },
        "AR40": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Bureau Krediet Registratie 1 to 10 - Credit Amount"},
        },
        "AR41": {
            "type": "string",
            "allowed": ["Y", "N"],
            "nullable": True,
            "meta": {"label": "Bureau Krediet Registratie 1 to 10 - Is Coding Cured?"},
        },
        "AR42": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {
                "label": "Bureau Krediet Registratie 1 to 10 - Number of Months Since Cured"
            },
        },
        "AR43": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5", "6", "7"],
            "nullable": True,
            "meta": {"label": "Bureau Score Provider"},
        },
        "AR44": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5", "6", "7", "8"],
            "nullable": True,
            "meta": {"label": "Bureau Score Type"},
        },
        "AR45": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Bureau Score Date"},
        },
        "AR46": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Bureau Score Value"},
        },
        "AR47": {
            "type": "string",
            "allowed": ["Y", "N"],
            "nullable": True,
            "meta": {"label": "Prior Repossessions"},
        },
        "AR48": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Previous Mortgage Arrears 0-6 Months"},
        },
        "AR49": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Previous Mortgage Arrears 6+ Months"},
        },
        "AR55": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Loan Origination Date"},
        },
        "AR56": {
            "type": "datetime",
            "coerce": TO_DATE,
            "nullable": True,
            "meta": {"label": "Date of Loan Maturity"},
        },
        "AR57": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Account Status Date"},
        },
        "AR58": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5", "6"],
            "nullable": True,
            "meta": {"label": "Origination Channel / Arranging Bank or Division"},
        },
        "AR59": {
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
            ],
            "nullable": True,
            "meta": {"label": "Purpose"},
        },
        "AR60": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5", "6"],
            "nullable": True,
            "meta": {"label": "Shared Ownership"},
        },
        "AR61": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Loan Term"},
        },
        "AR62": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Principal Grace Period"},
        },
        "AR63": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Amount Guaranteed"},
        },
        "AR64": {
            "type": "string",
            "allowed": ["Y", "N"],
            "nullable": True,
            "meta": {"label": "Subsidy"},
        },
        "AR65": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Loan Currency Denomination "},
        },
        "AR66": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Original Balance"},
        },
        "AR67": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "meta": {"label": "Current Balance"},
        },
        "AR68": {
            "type": "string",
            "allowed": ["Y", "N"],
            "nullable": True,
            "meta": {"label": "Fractioned / Subrogated Loans"},
        },
        "AR69": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5", "6", "7", "8", "9"],
            "nullable": True,
            "meta": {"label": "Repayment Method"},
        },
        "AR70": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5", "6"],
            "nullable": True,
            "meta": {"label": "Payment Frequency"},
        },
        "AR71": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Payment Due"},
        },
        "AR72": {
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
            "meta": {"label": "Payment Type"},
        },
        "AR73": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Debt to Income"},
        },
        "AR74": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"],
            "nullable": True,
            "meta": {"label": "Type of Guarantee Provider"},
        },
        "AR75": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Guarantee Provider"},
        },
        "AR76": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Income Guarantor"},
        },
        "AR77": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Subsidy Received"},
        },
        "AR78": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Mortgage Indemnity Guarantee Provider"},
        },
        "AR79": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Mortgage Indemnity Guarantee Attachment Point"},
        },
        "AR80": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Prior Balances"},
        },
        "AR81": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Other Prior Balances"},
        },
        "AR82": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Pari Passu Loans"},
        },
        "AR83": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Subordinated Claims"},
        },
        "AR84": {
            "type": "string",
            "allowed": ["1", "2", "3", "4"],
            "nullable": True,
            "meta": {"label": "Lien"},
        },
        "AR85": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Retained Amount"},
        },
        "AR86": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Retained Amount Date"},
        },
        "AR87": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Maximum Balance"},
        },
        "AR88": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Further Loan Advance"},
        },
        "AR89": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Further Loan Advance Date"},
        },
        "AR90": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Flexible Loan Amount"},
        },
        "AR91": {
            "type": "string",
            "allowed": ["Y", "N"],
            "nullable": True,
            "meta": {"label": "Further Advances"},
        },
        "AR92": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Length of Payment Holiday"},
        },
        "AR93": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Subsidy Period"},
        },
        "AR94": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Mortgage Inscription"},
        },
        "AR95": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Mortgage Mandate"},
        },
        "AR96": {
            "type": "string",
            "allowed": ["Y", "N"],
            "nullable": True,
            "meta": {"label": "Deed of Postponement?"},
        },
        "AR97": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Pre-payment Amount"},
        },
        "AR98": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Pre-payment Date"},
        },
        "AR99": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Pre-payment Penalties"},
        },
        "AR100": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Cumulative Pre-payments"},
        },
        "AR101": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Percentage of pre-payments allowed per year"},
        },
        "AR107": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5", "6", "7", "8"],
            "nullable": True,
            "meta": {"label": "Interest Rate Type"},
        },
        "AR108": {
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
            "meta": {"label": "Current Interest Rate Index"},
        },
        "AR109": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "meta": {"label": "Current Interest Rate"},
        },
        "AR110": {
            "type": "number",
            "coerce": TO_NUMBER,
            "nullable": True,
            "meta": {"label": "Current Interest Rate Margin"},
        },
        "AR111": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Interest Rate Reset Interval"},
        },
        "AR112": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Interest Cap Rate"},
        },
        "AR113": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Revision Margin 1"},
        },
        "AR114": {
            "type": "datetime",
            "coerce": TO_DATE,
            "nullable": True,
            "meta": {"label": "Interest Revision Date 1"},
        },
        "AR115": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Revision Margin 2"},
        },
        "AR116": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Interest Revision Date 2"},
        },
        "AR117": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Revision Margin 3"},
        },
        "AR118": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Interest Revision Date 3"},
        },
        "AR119": {
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
            "meta": {"label": "Revised Interest Rate Index"},
        },
        "AR120": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Final Margin"},
        },
        "AR121": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Final Step Date"},
        },
        "AR122": {
            "type": "string",
            "allowed": ["Y", "N"],
            "nullable": True,
            "meta": {"label": "Restructuring Arrangement"},
        },
        "AR128": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Geographic Region List"},
        },
        "AR129": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Property Postcode "},
        },
        "AR130": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5"],
            "nullable": True,
            "meta": {"label": "Occupancy Type"},
        },
        "AR131": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"],
            "nullable": True,
            "meta": {"label": "Property Type"},
        },
        "AR132": {
            "type": "string",
            "allowed": ["1", "2", "3"],
            "nullable": True,
            "meta": {"label": "New Property"},
        },
        "AR133": {
            "type": "datetime",
            "coerce": TO_DATE,
            "nullable": True,
            "meta": {"label": "Construction Year"},
        },
        "AR134": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Property Rating"},
        },
        "AR135": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Original Loan to Value"},
        },
        "AR136": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Valuation Amount"},
        },
        "AR137": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5", "6", "7", "8", "9"],
            "nullable": True,
            "meta": {"label": "Original Valuation Type"},
        },
        "AR138": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": " Valuation Date"},
        },
        "AR139": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {
                "label": "Confidence Interval for Original Automated Valuation Model Valuation"
            },
        },
        "AR140": {
            "type": "string",
            "nullable": True,
            "meta": {
                "label": "Provider of Original Automated Valuation Model Valuation"
            },
        },
        "AR141": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Current Loan to Value"},
        },
        "AR142": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Purchase Price Lower Limit"},
        },
        "AR143": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Current Valuation Amount"},
        },
        "AR144": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5", "6", "7", "8", "9"],
            "nullable": True,
            "meta": {"label": "Current Valuation Type"},
        },
        "AR145": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Current Valuation Date"},
        },
        "AR146": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {
                "label": "Confidence Interval for Current Automated Valuation Model Valuation"
            },
        },
        "AR147": {
            "type": "string",
            "nullable": True,
            "meta": {
                "label": "Provider of Current Automated Valuation Model Valuation"
            },
        },
        "AR148": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Property Value at Time of Latest Loan Advance"},
        },
        "AR149": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Indexed Foreclosure Value"},
        },
        "AR150": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Ipoteca"},
        },
        "AR151": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Date of Sale"},
        },
        "AR152": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5"],
            "nullable": True,
            "meta": {"label": "Additional Collateral"},
        },
        "AR153": {
            "type": "string",
            "nullable": True,
            "meta": {"label": "Additional Collateral Provider"},
        },
        "AR154": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Gross Annual Rental Income"},
        },
        "AR155": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Number of Buy to Let Properties"},
        },
        "AR156": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Debt Service Coverage Ratio"},
        },
        "AR157": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Additional Collateral Value"},
        },
        "AR158": {
            "type": "string",
            "allowed": ["Y", "N"],
            "nullable": True,
            "meta": {"label": "Real Estate Owned"},
        },
        "AR159": {
            "type": "string",
            "allowed": ["Y", "N"],
            "nullable": True,
            "meta": {"label": "Is Property Transferability Limited"},
        },
        "AR160": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Time Until Declassification"},
        },
        "AR166": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5", "6"],
            "nullable": True,
            "meta": {"label": "Account Status"},
        },
        "AR167": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Date Last Current"},
        },
        "AR168": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Date Last in Arrears"},
        },
        "AR169": {
            "type": "number",
            "coerce": TO_NUMBER,
            "nullable": True,
            "meta": {"label": "Arrears Balance"},
        },
        "AR170": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Number Months in Arrears"},
        },
        "AR171": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Arrears 1 Month Ago"},
        },
        "AR172": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Arrears 2 Months Ago"},
        },
        "AR173": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Performance Arrangement"},
        },
        "AR174": {
            "type": "string",
            "allowed": ["Y", "N"],
            "nullable": True,
            "meta": {"label": "Litigation"},
        },
        "AR175": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Redemption Date"},
        },
        "AR176": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Months in Arrears Prior"},
        },
        "AR177": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Default or Foreclosure"},
        },
        "AR178": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "nullable": True,
            "meta": {"label": "Date of Default or Foreclosure"},
        },
        "AR179": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Sale Price lower limit"},
        },
        "AR180": {
            "type": "number",
            "coerce": TO_NUMBER,
            "nullable": True,
            "meta": {"label": "Loss on Sale"},
        },
        "AR181": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Cumulative Recoveries"},
        },
        "AR182": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Professional Negligence Recoveries"},
        },
        "AR183": {
            "type": "string",
            "allowed": ["Y", "N"],
            "nullable": True,
            "meta": {"label": "Loan flagged as Contencioso"},
        },
    }
    return schema


def bond_info_schema():
    """
    Return validation schema for BOND_INFO data type.
    """
    schema = {
        "BR1": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "meta": {"label": "Report Date"},
        },
        "BR2": {"type": "string", "meta": {"label": "Issuer"}},
        "BR3": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Ending Reserve Account Balance"},
        },
        "BR4": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Target Reserve Account Balance"},
        },
        "BR5": {
            "type": "string",
            "allowed": ["Y", "N"],
            "meta": {"label": "Drawings under Liquidity Facility"},
        },
        "BR11": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Excess Spread Amount"},
        },
        "BR12": {
            "type": "string",
            "allowed": ["Y", "N"],
            "meta": {"label": "Trigger Measurements/Ratios"},
        },
        "BR13": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "meta": {"label": "Average Constant Pre-payment Rate"},
        },
        "BR19": {"type": "string", "meta": {"label": "Point Contact"}},
        "BR20": {"type": "string", "meta": {"label": "Contact Information"}},
        "BR25": {"type": "string", "meta": {"label": "Bond Class Name"}},
        "BR26": {
            "type": "string",
            "meta": {"label": "International Securities Identification Number "},
        },
        "BR27": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "meta": {"label": "Interest Payment Date"},
        },
        "BR28": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "meta": {"label": "Principal Payment Date"},
        },
        "BR29": {"type": "string", "meta": {"label": "Currency"}},
        "BR30": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Original Principal Balance"},
        },
        "BR31": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Total Ending Balance Subsequent to Payment"},
        },
        "BR32": {
            "type": "string",
            "allowed": ["1", "2", "3", "4", "5", "6", "7", "8", "9"],
            "meta": {"label": "Reference Rate"},
        },
        "BR33": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Relevant Margin"},
        },
        "BR34": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Coupon Reference Rate"},
        },
        "BR35": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Current Coupon"},
        },
        "BR36": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Cumulative Interest Shortfall "},
        },
        "BR37": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
            "meta": {"label": "Cumulative Principal Shortfalls"},
        },
        "BR38": {
            "type": "datetime",
            "coerce": TO_DATE,
            "nullable": True,
            "meta": {"label": "Legal Maturity"},
        },
        "BR39": {
            "type": "datetime",
            "coerce": TO_DATE,
            "max": datetime.datetime.now(),
            "meta": {"label": "Bond Issue Date"},
        },
    }
    return schema
