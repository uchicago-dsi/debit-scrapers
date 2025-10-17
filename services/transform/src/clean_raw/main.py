"""The entrypoint for the package."""

# Standard library imports
import argparse
import logging
import os
from datetime import datetime, UTC
from pathlib import Path

# Third-party imports
import pandas as pd
import smart_open

# Package imports
from clean_raw.constants import (
    GOOGLE_CLOUD_URI_SCHEME,
    ENV,
    INPUT_DIR,
    OUTPUT_DIR,
    PROD,
)
from clean_raw.currency import convert_currencies
from clean_raw.standardize import standardize_columns
from clean_raw.utils import LoggerFactory


def main(fpath: str, is_remote: bool, logger: logging.Logger) -> None:
    """Cleans scraped project data and then saves the output in Parquet format.

    Args:
        fpath: The path to the input file. May be local or remote.

        is_remote: A flag indicating whether the input file is remote.

        logger: A standard logger instance.

    Returns:
        `None`
    """
    # Finalize path for input file
    if not is_remote:
        fpath = INPUT_DIR / fpath

    # Define data types for DataFrame
    dtypes = {
        "id": "object",
        "created_at_utc": "object",
        "updated_at_utc": "object",
        "affiliates": "object",
        "countries": "object",
        "date_actual_close": "object",
        "date_approved": "object",
        "date_disclosed": "object",
        "date_effective": "object",
        "date_last_updated": "object",
        "date_planned_close": "object",
        "date_planned_effective": "object",
        "date_revised_close": "object",
        "date_signed": "object",
        "date_under_appraisal": "object",
        "finance_types": "object",
        "fiscal_year_effective": "object",
        "name": "object",
        "number": "object",
        "sectors": "object",
        "source": "object",
        "status": "object",
        "total_amount": "float64",
        "total_amount_currency": "object",
        "total_amount_usd": "float64",
        "url": "object",
        "task_id": "object",
    }

    # Fetch dataset and read into DataFrame
    try:
        logger.info("Fetching scraped project data and reading into DataFrame.")
        with smart_open.open(fpath, "rb", encoding="utf-8") as f:
            raw_df = pd.read_csv(
                f,
                names=dtypes.keys(),
                sep="\t",
                quotechar='"',
                dtype=dtypes,
            )
        logger.info(f"{len(raw_df):,} record(s) received.")
    except FileNotFoundError as e:
        raise RuntimeError(
            f'An unexpected error occurred. Cannot find file at "{fpath}". {e}'
        ) from None

    # Standardize data columns
    try:
        logger.info("Standardizing data formatting and categorical values.")
        standardized_df = standardize_columns(raw_df, logger)
    except Exception as e:
        raise RuntimeError(f"Failed to standardize project data. {e}") from None

    # Convert project nominal currencies to USD
    try:
        logger.info("Converting project currencies to USD.")
        converted_df = convert_currencies(standardized_df)
    except Exception as e:
        raise RuntimeError(
            f"Failed to convert project currencies to USD. {e}"
        ) from None

    # Finalize order of columns for display
    try:
        logger.info("Finalizing column order.")
        clean_df = converted_df[
            [
                "id",
                "source",
                "name",
                "number",
                "status",
                "finance_types",
                "total_amount",
                "total_amount_currency",
                "total_amount_usd",
                "conversion_year",
                "conversion_rate",
                "converted_amount_usd",
                "countries",
                "sectors",
                "affiliates",
                "date_actual_close",
                "date_approved",
                "date_disclosed",
                "date_effective",
                "date_last_updated",
                "date_planned_close",
                "date_planned_effective",
                "date_revised_close",
                "date_signed",
                "date_under_appraisal",
                "fiscal_year_effective",
                "url",
            ]
        ]
    except KeyError as e:
        raise RuntimeError(
            "An unexpected error occurred in the data "
            f"cleaning pipeline. Missing column. {e}"
        )

    # Determine path for output file
    logger.info("Determining path for output file.")
    if is_remote:
        today = datetime.now(tz=UTC).strftime("%Y-%m-%d")
        input_fname = fpath.split("/")[-1]
        obj_key = f"transformation/{today}/{input_fname}.parquet"
        output_fpath = f"{GOOGLE_CLOUD_URI_SCHEME}{obj_key}"
    else:
        Path.mkdir(OUTPUT_DIR, exist_ok=True)
        output_fpath = f"{OUTPUT_DIR}/cleaned.parquet"

    # Write cleaned data to output file
    try:
        logger.info(f'Writing clean project data to "{output_fpath}".')
        with smart_open.open(output_fpath, "wb", encoding="utf-8") as f:
            clean_df.to_parquet(f, index=False, compression="gzip")
    except Exception as e:
        logger.error(f"Failed to write clean project data to file. {e}")
        exit(1)


if __name__ == "__main__":
    # Initialize logger
    logger = LoggerFactory.get("CLEAN SCRAPED DATA", level=logging.INFO)

    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("input_fpath", type=str, help="The path to the input file.")
    args = parser.parse_args()
    if not args.input_fpath:
        logger.error("Missing positional argument for input file path.")
        exit(1)

    # Parse environment variables
    try:
        is_remote = os.environ[ENV] == PROD
    except KeyError as e:
        logger.error(f"Missing expected environment variable. {e}")
        exit(1)

    # Execute main program logic
    try:
        logger.info(
            f'Received request to clean scraped data file at "{args.input_fpath}".'
        )
        main(args.input_fpath, is_remote, logger)
    except Exception as e:
        logger.error(f"Failed to clean raw development bank project data. {e}")
        exit(1)

    # Log success
    logger.info("Scraped project data cleaned successfully.")
