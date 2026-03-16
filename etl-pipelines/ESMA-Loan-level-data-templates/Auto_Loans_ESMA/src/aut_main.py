import argparse
from src.utils.spark_setup import start_spark

# Bronze profile packages
from src.aut_etl_pipeline.profile_bronze_data import profile_bronze_data

# Bronze layer packages
from src.aut_etl_pipeline.generate_bronze_tables import generate_bronze_tables
from src.aut_etl_pipeline.generate_deal_details_bronze import generate_deal_details_bronze

# Silver layer packages
from src.aut_etl_pipeline.generate_asset_silver import generate_asset_silver
from src.aut_etl_pipeline.generate_bond_info_silver import generate_bond_info_silver
from src.aut_etl_pipeline.generate_deal_details_silver import generate_deal_details_silver


def run(
    raw_bucketname,
    data_bucketname,
    source_prefix,
    target_prefix,
    dl_code,
    file_key,
    stage_name,
    ingestion_date,
    app_name="DeeploansApp"
):
    """
    Orchestrates the Auto Loans ETL pipeline, executing the appropriate processing stage based on `stage_name`.

    Args:
        raw_bucketname (str): Name of the Google Cloud Storage bucket containing the raw data.
        data_bucketname (str): Name of the bucket to store processed data.
        source_prefix (str): Prefix/path in the bucket for input files.
        target_prefix (str): Prefix/path in the bucket for output files.
        dl_code (str): Deal code (mainly used in Silver layer ETL).
        file_key (str): File identifier for selective processing (used in profile stages).
        stage_name (str): Name of the ETL stage to execute. Must be one of the allowed stages.
        ingestion_date (str): Date of data ingestion (YYYY-MM-DD).
        app_name (str): Name of the Spark Application (default: "DeeploansApp").

    Returns:
        status: The return value of the ETL stage function executed.

    Raises:
        ValueError: If the stage_name is not recognized.
        Exception: Propagates exceptions raised from the ETL functions with additional context.

    Supported stages and their mapping:
        - profile_bronze_asset
        - profile_bronze_collateral
        - profile_bronze_bond_info
        - profile_bronze_amortisation
        - bronze_asset
        - bronze_collateral
        - bronze_bond_info
        - bronze_amortisation
        - bronze_deal_details
        - silver_asset
        - silver_bond_info
        - silver_deal_details
    """
    spark = start_spark(app_name)

    # Mapping of stage_name to (function, args, kwargs)
    stage_map = {
        "profile_bronze_asset":   (profile_bronze_data,   [raw_bucketname, data_bucketname, source_prefix, file_key, "assets", ingestion_date], {}),
        "profile_bronze_collateral": (profile_bronze_data,   [raw_bucketname, data_bucketname, source_prefix, file_key, "collaterals", ingestion_date], {}),
        "profile_bronze_bond_info": (profile_bronze_data,   [raw_bucketname, data_bucketname, source_prefix, file_key, "bond_info", ingestion_date], {}),
        "profile_bronze_amortisation": (profile_bronze_data,   [raw_bucketname, data_bucketname, source_prefix, file_key, "amortisation", ingestion_date], {}),

        "bronze_asset":          (generate_bronze_tables, [spark, data_bucketname, source_prefix, target_prefix, "assets", ingestion_date], {}),
        "bronze_collateral":     (generate_bronze_tables, [spark, data_bucketname, source_prefix, target_prefix, "collaterals", ingestion_date], {}),
        "bronze_bond_info":      (generate_bronze_tables, [spark, data_bucketname, source_prefix, target_prefix, "bond_info", ingestion_date], {}),
        "bronze_amortisation":   (generate_bronze_tables, [spark, data_bucketname, source_prefix, target_prefix, "amortisation", ingestion_date], {}),
        "bronze_deal_details":   (generate_deal_details_bronze, [spark, data_bucketname, source_prefix, target_prefix, ingestion_date], {}),

        "silver_asset":          (generate_asset_silver, [spark, data_bucketname, source_prefix, target_prefix, dl_code, ingestion_date], {}),
        "silver_bond_info":      (generate_bond_info_silver, [spark, data_bucketname, source_prefix, target_prefix, dl_code, ingestion_date], {}),
        "silver_deal_details":   (generate_deal_details_silver, [spark, data_bucketname, source_prefix, target_prefix, dl_code, ingestion_date], {}),
    }

    if stage_name not in stage_map:
        raise ValueError(
            f"Unrecognized stage_name '{stage_name}'. Supported stages are: {list(stage_map.keys())}"
        )

    func, args, kwargs = stage_map[stage_name]
    try:
        status = func(*args, **kwargs)
        return status
    except Exception as e:
        raise Exception(
            f"Error during execution of stage '{stage_name}': {e}"
        ) from e


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Run a specific stage of the Auto Loans ETL Pipeline.\n"
            "Each stage corresponds to a different step in the pipeline (profiling, bronze, silver).\n"
            "See the documentation for details on each stage."
        )
    )
    parser.add_argument("--raw_bucketname", required=True, help="Name of the raw data bucket (Google Cloud Storage).")
    parser.add_argument("--data_bucketname", required=True, help="Name of the bucket for processed/bronze/silver data.")
    parser.add_argument("--source_prefix", required=True, help="Prefix (path) for source files in the bucket.")
    parser.add_argument("--target_prefix", required=True, help="Prefix (path) for target/output files in the bucket.")
    parser.add_argument("--dl_code", required=False, help="Deal code for silver layer (optional).", default=None)
    parser.add_argument("--file_key", required=False, help="File key for cherry picking files (optional).", default=None)
    parser.add_argument("--stage_name", required=True, help=(
        "Stage of the ETL pipeline to execute. "
        "Allowed values: profile_bronze_asset, profile_bronze_collateral, profile_bronze_bond_info, profile_bronze_amortisation, "
        "bronze_asset, bronze_collateral, bronze_bond_info, bronze_amortisation, bronze_deal_details, "
        "silver_asset, silver_bond_info, silver_deal_details"
    ))
    parser.add_argument("--ingestion_date", required=True, help="Date of data ingestion (YYYY-MM-DD).")
    parser.add_argument("--app_name", required=False, help="Name for the Spark Application (optional).", default="DeeploansApp")
    args = parser.parse_args()

    try:
        result = run(
            args.raw_bucketname,
            args.data_bucketname,
            args.source_prefix,
            args.target_prefix,
            args.dl_code,
            args.file_key,
            args.stage_name,
            args.ingestion_date,
            args.app_name
        )
        print(f"Stage '{args.stage_name}' completed successfully. Result: {result}")
    except Exception as exc:
        print(f"[ERROR] {exc}")
        exit(1)
