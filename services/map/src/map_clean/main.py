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
from map_clean.utils import configure_cloudflare_request_params, LoggerFactory


def main(
    input_fpath: str,
    output_fpath: str,
    transport_params: dict,
    logger: logging.Logger,
) -> None:
    """Maps clean project data to the format required by the DeBIT website.

    The input file is expected to be in Parquet format and saved within a
    Google Cloud Storage bucket. The function reads the file from the remote
    storage location, maps the clean data, and then writes the output Parquet
    file to a public Cloudflare R2 bucket.

    References:
    - https://github.com/piskvorky/smart_open

    Args:
        input_fpath: The path to the input file. May be local or remote.

        output_fpath: The path to the output file. May be local or remote.

        transport_params: The request parameters to use for object uploads.

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

    # Define local function to generate display date
    def get_display_date(row: pd.Series) -> tuple[str, str]:
        """Determines the project date to display on the website.

        Args:
            row: A row of data from the DataFrame.

        Returns:
            A two-item tuple consisting of the date field name
                (e.g., "date_signed", "date_effective") and
                value (e.g., "2023-04-19") for display on the
                website. If a project does not have any dates
                populated, a two-item tuple of empty strings
                is returned instead.
        """
        ranked_date_types = [
            "date_signed",
            "date_approved",
            "date_disclosed",
            "date_under_appraisal",
            "date_effective",
            "fiscal_year_effective",
            "date_planned_effective",
            "date_last_updated",
            "date_actual_close",
            "date_revised_close",
            "date_planned_close",
        ]
        for date_type in ranked_date_types:
            if row[date_type]:
                return date_type.upper().replace("_", " "), row[date_type]
        return "", ""

    # Add "Display Date" and "Display Date Type" columns
    logger.info("Adding display date name and type columns.")
    (
        clean_df.loc[:, ["display_date_type"]],
        clean_df.loc[:, ["display_date"]],
    ) = zip(*clean_df.apply(get_display_date, axis=1))

    # Define local function to generate "Document" column
    def get_document(row: pd.Series) -> str:
        """Generates a document column to facilitate searches.

        The field will be a space-separated string containing
        the project's source, name, number, status, affiliates,
        countries, sectors, and finance types.

        Args:
            row: A row of data from the DataFrame.

        Returns:
            A document field for the project.
        """
        return " ".join(
            [
                row["source"] or "",
                row["name"] or "",
                row["number"] or "",
                row["status"] or "",
                " ".join(row["affiliates"] if len(row["affiliates"]) else []),
                " ".join(row["countries"] if len(row["countries"]) else []),
                " ".join(row["sectors"] if len(row["sectors"]) else []),
                " ".join(row["finance_types"] if len(row["finance_types"]) else []),
            ]
        )

    # Add document column to facilitate search
    logger.info("Adding document column.")
    clean_df.loc[:, ["document"]] = clean_df.apply(get_document, axis=1)

    # Write mapped data to output file in S3-compatible storage bucket
    try:
        logger.info(f'Writing mapped project data to "{output_fpath}".')
        with smart_open.open(
            output_fpath,
            "wb",
            transport_params=transport_params,
        ) as f:
            clean_df.to_parquet(f, index=False, compression="gzip")
    except Exception as e:
        raise RuntimeError(
            f"Failed to write mapped project data to file. {e}"
        ) from None


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

        # Compose storage transport parameters
        try:
            transport_params = configure_cloudflare_request_params()
        except Exception as e:
            logger.error(f"Failed to configure S3 request parameters. {e}")
            exit(1)
    else:
        # Compose path to local input file
        input_fpath = f"{INPUT_DIR}/{args.object_key}"

        # Compose path to local output file
        Path.mkdir(OUTPUT_DIR, exist_ok=True)
        output_fpath = f"{OUTPUT_DIR}/{OUTPUT_FNAME}"

        # Compose storage transport parameters
        transport_params = {}

    # Execute main program logic
    try:
        logger.info(f'Received request to map clean data file at "{input_fpath}".')
        main(input_fpath, output_fpath, transport_params, logger)
    except Exception as e:
        logger.error(f"Failed to map clean development bank project data. {e}")
        exit(1)

    # Log success
    logger.info("DeBIT website data file created successfully.")
