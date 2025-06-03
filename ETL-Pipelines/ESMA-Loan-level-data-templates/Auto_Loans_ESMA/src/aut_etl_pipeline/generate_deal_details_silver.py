import logging
import sys
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StringType, DoubleType, BooleanType, IntegerType
from src.aut_etl_pipeline.utils.silver_funcs import (
    cast_to_datatype,
)
from src.aut_etl_pipeline.config import PROJECT_ID

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

def set_job_params():
    config = {}
    config["DATE_COLUMNS"] = [
    "Data Cut-Off Date",
    "Pool Addition Date",
    "Date Of Repurchase",
    "Redemption Date",
    "Origination Date",
    "Maturity Date",
    "Principal Grace Period End Date",
    "Prepayment Fee End Date",
    "Prepayment Date",
    "Date Of Updated Residual Valuation Of Vehicle",
    "Date Of Restructuring",
    "Date Last In Arrears",
    "Default Date"
]
    config["DEAL_DETAILS_COLUMNS"] = {
        "Unique Identifier": StringType(),
        "Original Underlying Exposure Identifier": StringType(),
        "New Underlying Exposure Identifier": StringType(),
        "Original Obligor Identifier": StringType(),
        "New Obligor Identifier": StringType(),
        "Geographic Region - Obligor": StringType(),
        "Geographic Region Classification": StringType(),
        "Employment Status": StringType(),
        "Credit Impaired Obligor": StringType(),
        "Obligor Legal Type": StringType(),
        "Customer Type": StringType(),
        "Primary Income": DoubleType(),
        "Primary Income Type": StringType(),
        "Primary Income Currency": StringType(),
        "Primary Income Verification": StringType(),
        "Revenue": DoubleType(),
        "Financial Statement Currency": StringType(),
        "Special Scheme": StringType(),
        "Product Type": StringType(),
        "Original Term": IntegerType(),
        "Origination Channel": StringType(),
        "Currency Denomination": StringType(),
        "Original Principal Balance": DoubleType(),
        "Current Principal Balance": DoubleType(),
        "Purchase Price": DoubleType(),
        "Amortisation Type": StringType(),
        "Scheduled Principal Payment Frequency": StringType(),
        "Scheduled Interest Payment Frequency": StringType(),
        "Payment Method": StringType(),
        "Payment Due": DoubleType(),
        "Balloon Amount": DoubleType(),
        "Down Payment Amount": DoubleType(),
        "Current Interest Rate": DoubleType(),
        "Current Interest Rate Index": StringType(),
        "Current Interest Rate Index Tenor": StringType(),
        "Current Interest Rate Margin": DoubleType(),
        "Interest Rate Reset Interval": IntegerType(),
        "Interest Rate Cap": DoubleType(),
        "Interest Rate Floor": DoubleType(),
        "Number Of Payments Before Securitisation": IntegerType(),
        "Percentage Of Prepayments Allowed Per Year": DoubleType(),
        "Prepayment Fee": DoubleType(),
        "Cumulative Prepayments": DoubleType(),
        "Manufacturer": StringType(),
        "Model": StringType(),
        "Year Of Registration": StringType(),
        "New Or Used": StringType(),
        "Energy Performance Certificate Value": StringType(),
        "Energy Performance Certificate Provider Name": StringType(),
        "Original Loan-To-Value": DoubleType(),
        "Original Valuation Amount": DoubleType(),
        "Original Residual Value Of Vehicle": DoubleType(),
        "Option To Buy Price": DoubleType(),
        "Securitised Residual Value": DoubleType(),
        "Updated Residual Value Of Vehicle": DoubleType(),
        "Arrears Balance": DoubleType(),
        "Number Of Days In Arrears": IntegerType(),
        "Account Status": StringType(),
        "Reason for Default or Foreclosure": StringType(),
        "Default Amount": DoubleType(),
        "Allocated Losses": DoubleType(),
        "Residual Value Losses": DoubleType(),
        "Cumulative Recoveries": DoubleType(),
        "Sale Price": DoubleType(),
        "Deposit Amount": DoubleType(),
        "Original Lender Name": StringType(),
        "Original Lender Legal Entity Identifier": StringType(),
        "Original Lender Establishment Country": StringType(),
        "Originator Name": StringType(),
        "Originator Legal Entity Identifier": StringType(),
        "Originator Establishment Country": StringType()
}
    return config

def process_deal_info(df):
    new_df = df.dropDuplicates()
    return new_df

def generate_deal_details_silver(spark, bucket_name, source_prefix, target_prefix):
    logger.info("Start DEAL DETAILS SILVER job.")
    run_props = set_job_params()
    bronze_df = (
        spark.read.format("delta")
        .load(f"gs://{bucket_name}/{source_prefix}")
        .filter(F.col("iscurrent") == 1)
        .drop("valid_from", "valid_to", "checksum", "iscurrent")
    )
    logger.info("Cast data to correct types.")
    cleaned_df = cast_to_datatype(bronze_df, run_props["DEAL_DETAILS_COLUMNS"])
    logger.info("Generate deal info dataframe")
    deal_info_df = process_deal_info(cleaned_df)

    logger.info("Write dataframe")
    (
        deal_info_df.write.format("parquet")
        .mode("overwrite")
        .save(f"gs://{bucket_name}/{target_prefix}/deal_info_table")
    )
    logger.info("End DEAL DETAILS SILVER job.")
    return 0