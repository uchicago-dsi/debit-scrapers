"""The entrypoint for the package."""

# Standard library imports
import argparse
import logging
from datetime import datetime, UTC
from pathlib import Path

# Third-party imports
import pandas as pd
import smart_open

# Package imports
from clean_raw.constants import INPUT_DIR, OUTPUT_DIR
from clean_raw.currency import convert_currencies
from clean_raw.standardize import standardize_columns
from clean_raw.utils import LoggerFactory


def main(input_fpath: str, output_fpath: str, logger: logging.Logger) -> None:
    """Cleans scraped project data and then saves the output in Parquet format.

    Args:
        input_fpath: The path to the input file. May be local or remote.

        output_fpath: The path to the output file. May be local or remote.

        logger: A standard logger instance.

    Returns:
        `None`
    """
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
        "fiscal_year_effective": "object",
    }

    # Fetch dataset and read into DataFrame
    #
    # NOTE: Here, we must explicitly specify the compression type when opening
    # the input file because smart_open can't automatically detect it. As a
    # consequence of the way bulk database exports are configured, the input
    # file is assigned an extension of "csv" even though its true MIME type is
    # "application/gzip". For a list of all available compression types, see:
    # https://github.com/piskvorky/smart_open/blob/master/smart_open/compression.py
    #
    try:
        logger.info("Fetching scraped project data and reading into DataFrame.")
        with smart_open.open(
            input_fpath, "rb", encoding="utf-8", compression=".gz"
        ) as f:
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
            "An unexpected error occurred. Cannot find "
            f'input file at "{input_fpath}". {e}'
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

    # Write cleaned data to output file
    try:
        logger.info(f'Writing clean project data to "{output_fpath}".')
        with smart_open.open(output_fpath, "wb") as f:
            clean_df.to_parquet(f, index=False, compression="gzip")
    except Exception as e:
        logger.error(f"Failed to write clean project data to file. {e}")
        exit(1)


if __name__ == "__main__":
    # Initialize logger
    logger = LoggerFactory.get("CLEAN SCRAPED DATA", level=logging.INFO)

    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "object_key",
        type=str,
        help="The path to the input file in the storage directory or bucket.",
    )
    parser.add_argument(
        "--input_bucket",
        type=str,
        help="The name of the input bucket.",
        default="",
    )
    parser.add_argument(
        "--output_bucket",
        type=str,
        help="The name of the output bucket.",
        default="",
    )
    parser.add_argument(
        "--remote",
        action="store_true",
        help="Indicates whether the input file is hosted in the cloud.",
    )
    args = parser.parse_args()

    # Validate object key argument received
    if not args.object_key:
        logger.error(
            "Missing positional argument for the path to the "
            "input file in the storage directory or bucket."
        )
        exit(1)

    # Determine path for input and output files
    if args.remote:
        # Validate bucket options present if files hosted remotely
        if not args.input_bucket:
            logger.error("Missing option for input bucket name.")
            exit(1)
        if not args.output_bucket:
            logger.error("Missing option for output bucket name.")
            exit(1)

        # Compose path to remote input file
        input_fpath = f"{args.input_bucket}/{args.object_key}"

        # Compose path to remote output file
        today = datetime.now(tz=UTC).strftime("%Y-%m-%d")
        input_fname = args.object_key.split("/")[-1]
        output_obj_key = f"transformation/{today}/{input_fname}.parquet"
        output_fpath = f"{args.output_bucket}/{output_obj_key}"
    else:
        # Compose path to local input file
        input_fpath = f"{INPUT_DIR}/{args.object_key}"

        # Compose path to local output file
        Path.mkdir(OUTPUT_DIR, exist_ok=True)
        output_fpath = f"{OUTPUT_DIR}/cleaned.parquet"

    # Execute main program logic
    try:
        logger.info(f'Received request to clean scraped data file at "{input_fpath}".')
        main(input_fpath, output_fpath, logger)
    except Exception as e:
        logger.error(f"Failed to clean raw development bank project data. {e}")
        exit(1)

    # Log success
    logger.info("Scraped project data cleaned successfully.")
