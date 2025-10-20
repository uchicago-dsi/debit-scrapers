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
from map_clean.constants import INPUT_DIR, OUTPUT_DIR, OUTPUT_FNAME
from map_clean.utils import LoggerFactory


def main(input_fpath: str, output_fpath: str, logger: logging.Logger) -> None:
    """Maps clean project data to the format required by the DeBIT website.

    Args:
        input_fpath: The path to the input file. May be local or remote.

        output_fpath: The path to the output file. May be local or remote.

        logger: A standard logger instance.

    Returns:
        `None`
    """
    # Fetch dataset and read into DataFrame
    try:
        logger.info("Fetching clean project data and reading into DataFrame.")
        with smart_open.open(input_fpath, "rb") as f:
            clean_df = pd.read_parquet(f)
        logger.info(f"{len(clean_df):,} record(s) received.")
    except FileNotFoundError as e:
        raise RuntimeError(
            "An unexpected error occurred. Cannot find "
            f'input file at "{input_fpath}". {e}'
        ) from None

    # Add "As Of" column
    logger.info("Adding 'As Of' column.")
    clean_df["as_of"] = datetime.now(tz=UTC).strftime("%Y-%m-%d")

    # Drop NBIM records
    logger.info("Dropping NBIM records.")
    clean_df = clean_df.query("source != 'NBIM'")

    # Write mapped data to output file
    try:
        logger.info(f'Writing mapped project data to "{output_fpath}".')
        with smart_open.open(output_fpath, "wb") as f:
            clean_df.to_parquet(f, index=False, compression="gzip")
    except Exception as e:
        logger.error(f"Failed to write mapped project data to file. {e}")
        exit(1)


if __name__ == "__main__":
    # Initialize logger
    logger = LoggerFactory.get("MAP CLEANED DATA", level=logging.INFO)

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
        output_fpath = f"{args.output_bucket}/{OUTPUT_FNAME}"
    else:
        # Compose path to local input file
        input_fpath = f"{INPUT_DIR}/{args.object_key}"

        # Compose path to local output file
        Path.mkdir(OUTPUT_DIR, exist_ok=True)
        output_fpath = f"{OUTPUT_DIR}/{OUTPUT_FNAME}"

    # Execute main program logic
    try:
        logger.info(
            f'Received request to map clean data file at "{input_fpath}".'
        )
        main(input_fpath, output_fpath, logger)
    except Exception as e:
        logger.error(f"Failed to map clean development bank project data. {e}")
        exit(1)

    # Log success
    logger.info("DeBIT website data file created successfully.")
