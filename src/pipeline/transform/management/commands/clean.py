"""Classes used to clean scraped project records."""

# Standard library imports
import io
import json
import logging
from datetime import datetime, UTC
from pathlib import Path

# Third-party imports
import numpy as np
import pandas as pd
import smart_open
from django.conf import settings
from django.core.management.base import BaseCommand, CommandParser

# Application imports
from common.logger import LoggerFactory
from transform.currency import CurrencyConverter


class ProjectCleaningClient:
    """Formats, maps, and standardizes development project data."""

    def __init__(self, logger: logging.Logger) -> None:
        """Initializes a new instance of a `ProjectColumnMapper`.

        Args:
            logger: A standard logger instance.

        Returns:
            `None`
        """
        # Instantiate client for converting currencies
        self._currency_converter = CurrencyConverter()

        # Load mapping files
        self._country_lookup = self._load_mapping_file(
            settings.COUNTRY_MAP_FPATH
        )
        self._currency_lookup = self._load_mapping_file(
            settings.CURRENCY_MAP_FPATH
        )
        self._finance_type_lookup = self._load_mapping_file(
            settings.FINANCE_TYPE_MAP_FPATH
        )
        self._sector_lookup = self._load_mapping_file(
            settings.SECTOR_MAP_FPATH, invert=False
        )
        self._status_lookup = self._load_mapping_file(
            settings.STATUS_MAP_FPATH
        )

        # Set logger reference
        self._logger = logger

    def _convert_currencies(self, df: pd.DataFrame) -> pd.DataFrame:
        """Converts development project nominal currencies to USD.

        Args:
            df: The input DataFrame. Expected to have the columns
                "total_amount", "total_amount_currency", "date_effective",
                "date_planned_effective", "date_signed", "date_approved",
                "date_disclosed".

        Returns:
            A DataFrame with debt amounts converted to USD.
        """

        # Define local function to determine year for currency conversion
        def get_currency_year(row: pd.Series) -> str:
            """Determines the year to use for currency conversion.

            Args:
                row: A row of data from the DataFrame.

            Returns:
                The year, formatted as YYYY, or an empty string
                    if the year cannot be determined.
            """
            if row["date_signed"]:
                return row["date_signed"][:4]
            elif row["date_approved"]:
                return row["date_approved"][:4]
            elif row["date_disclosed"]:
                return row["date_disclosed"][:4]
            elif row["date_under_appraisal"][:4]:
                return row["date_under_appraisal"][:4]
            elif row["date_effective"]:
                return row["date_effective"][:4]
            elif row["fiscal_year_effective"]:
                return row["fiscal_year_effective"]
            elif row["date_planned_effective"]:
                return row["date_planned_effective"][:4]
            else:
                return ""

        # Define local function to perform conversion
        def convert(row: pd.Series) -> int | None:
            """Converts nominal currency to USD.

            Args:
                row: A row of data from the DataFrame.

            Returns:
                The exchange rate to USD, or `None`
                    if the rate cannot be determined.
            """
            if row["total_amount"] is None or row["rate_to_usd"] is None:
                return None
            return int(row["total_amount"] * row["rate_to_usd"])

        # For each project, determine year to use for currency conversion
        df["currency_year"] = df.apply(get_currency_year, axis=1)

        # Calculate exchange rates
        df["rate_to_usd"] = df.apply(
            lambda row: self._currency_converter.get_usd_exchange_rate(
                currency=row["total_amount_currency"],
                year=row["currency_year"],
            ),
            axis=1,
        )

        # Replace NaN values
        df = df.replace({np.nan: None})

        # Convert debt amounts to USD in same year
        df["est_total_amount_usd"] = df.apply(convert, axis=1)
        df["est_total_amount_usd"] = df["est_total_amount_usd"].astype(
            pd.Int64Dtype()
        )

        # Replace NA values
        df = df.replace({pd.NA: None})

        return df

    def _explode_delimited_col(
        self,
        df: pd.DataFrame,
        id_col: str,
        delimited_col: str,
        exploded_col: str,
        delimiter: str = "|",
    ) -> pd.DataFrame:
        """Explodes a delimited column into multiple rows.

        Args:
            df: The DataFrame to explode.

            id_col: The name of the column containing unique identifiers.

            delimited_col: The name of the column to explode.

            exploded_col: The name of the new column containing
                the exploded values.

            delimiter: The delimiter used to separate values
                in the delimited column. Defaults to "|".

        Returns:
            A DataFrame with the exploded column and  associated ids.
        """
        # Transform delimited strings into lists
        df.loc[:, delimited_col] = (
            df[delimited_col]
            .replace({np.nan: ""})
            .apply(lambda s: s.split(delimiter))
        )

        # Restructure DataFrame to consist of "project id - raw value" pairs
        exploded_df = (
            df.set_index([id_col])[delimited_col]  # noqa
            .apply(pd.Series)
            .stack()
            .reset_index()
        )

        # Rename columns
        exploded_df = exploded_df.rename(columns={0: exploded_col})

        return exploded_df[[id_col, exploded_col]]

    def _load_mapping_file(self, fpath: str, invert: bool = True) -> dict:
        """Loads a column mapping definition from a JSON file.

        Args:
            fpath: The path to the JSON file.

            invert: A flag indicating whether the mapping's key-value
                pairs should be inverted. Defaults to `True`.

        Returns:
            A dictionary mapping input values to standard output values.
        """
        with open(fpath, encoding="utf-8") as f:
            mapping = json.load(f)

        return (
            {
                val: output
                for output, input_lst in mapping.items()
                for val in input_lst
            }
            if invert
            else mapping
        )

    def _standardize_column(
        self,
        df: pd.DataFrame,
        id_col: str,
        value_col: str,
        mapping: dict | None = None,
        delimiter: str | None = None,
        unknown_value: str = "Unknown",
    ) -> pd.DataFrame:
        """Standardizes values in a column based on a mapping dictionary.

        Raises:
            ValueError: If the DataFrame does not contain
                the specified delimited column or id column.

        Args:
            df: the DataFrame to standardize.

            id_col: The name of column containing unique identifiers.

            value_col: The name of the column to explode.

            mapping: A dictionary mapping input values to
                standard output values. Defaults to `None`.

            delimiter: The delimiter used to separate values
                in the column, if applicable. Defaults to `None`.

            unknown_value: Value to use for unknown values.
                Defaults to "Unknown".

        Returns:
            A DataFrame with a list of standardized values
                for each unique identifier.
        """
        # Validate existence of value column
        if value_col not in df.columns:
            raise ValueError(
                f'DataFrame must contain the column "{value_col}".'
            )

        # Validate existence of id column
        if id_col not in df.columns:
            raise ValueError(f'DataFrame must contain the column "{id_col}".')

        # Subset DataFrame to relevant columns
        temp_df = df[[id_col, value_col]].copy()

        # If value column is delimited, explode
        if delimiter:
            temp_df = self._explode_delimited_col(
                temp_df,
                id_col,
                value_col,
                exploded_col=value_col,
                delimiter=delimiter,
            )

        # Clean values
        temp_df[value_col] = (
            temp_df[value_col]
            .replace({np.nan: ""})
            .apply(lambda val: val.strip().lower())
        )

        # Map values if applicable and explode again
        # NOTE: Mapped values may be delimited
        if mapping:
            temp_df[value_col] = (
                temp_df[value_col]
                .map(mapping)
                .replace({np.nan: unknown_value})
            )
            temp_df = self._explode_delimited_col(
                temp_df,
                id_col,
                value_col,
                exploded_col=value_col,
                delimiter=delimiter or "|",
            )

        # Make values uppercase
        temp_df[value_col] = temp_df[value_col].str.upper()

        # Aggregate values by id column and leave as list-like if applicable
        if delimiter:
            temp_df = (
                temp_df[[id_col, value_col]]
                .groupby(id_col)[value_col]
                .agg(lambda s: sorted(set(s)))
                .reset_index(name=value_col)
            )

        return temp_df

    def _standardize_affiliates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardizes the "affiliates" column in the project DataFrame.

        Raises:
            ValueError: If the DataFrame does not contain
                an "affiliates" column or an "id" column.

        Args:
            df: The input DataFrame.

        Returns:
            A DataFrame mapping each unique id to
                a list of standardized affiliates.
        """
        return self._standardize_column(
            df=df,
            id_col="id",
            value_col="affiliates",
            delimiter="|",
        )

    def _standardize_countries(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardizes the "countries" column in the project DataFrame.

        Raises:
            ValueError: If the DataFrame does not contain
                a "countries" column or an "id" column.

        Args:
            df: The input DataFrame.

        Returns:
            A DataFrame mapping each unique id to
                a list of standardized countries.
        """
        return self._standardize_column(
            df=df,
            id_col="id",
            value_col="countries",
            mapping=self._country_lookup,
            delimiter="|",
        )

    def _standardize_currency_codes(self, df: pd.DataFrame) -> list[list[str]]:
        """Standardizes the "total_amount_currency" column in the project DataFrame.

        Raises:
            ValueError: If the DataFrame does not contain
                a "total_amount_currency" column or an "id" column.

        Args:
            df: The input DataFrame.

        Returns:
            A DataFrame mapping each unique id to
                a list of standardized project statuses.
        """
        return self._standardize_column(
            df=df,
            id_col="id",
            value_col="total_amount_currency",
            mapping=self._currency_lookup,
        )

    def _standardize_finance_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardizes the "finance_types" column in the project DataFrame.

        Raises:
            ValueError: If the DataFrame does not contain
                a "finance_types" column or an "id" column.

        Args:
            df: The input DataFrame.

        Returns:
            A DataFrame mapping each unique id to
                a list of standardized finance types.
        """
        return self._standardize_column(
            df=df,
            id_col="id",
            value_col="finance_types",
            mapping=self._finance_type_lookup,
            delimiter="|",
        )

    def _standardize_sectors(self, df: pd.DataFrame) -> list[list[str]]:
        """Standardizes the "status" column in the project DataFrame.

        Raises:
            ValueError: If the DataFrame does not contain
                a "status" column or an "id" column.

        Args:
            df: The input DataFrame.

        Returns:
            A DataFrame mapping each unique id to
                a list of standardized project statuses.
        """
        return self._standardize_column(
            df=df,
            id_col="id",
            value_col="sectors",
            mapping=self._sector_lookup,
            delimiter="|",
        )

    def _standardize_statuses(self, df: pd.DataFrame) -> list[list[str]]:
        """Standardizes the "status" column in the project DataFrame.

        Raises:
            ValueError: If the DataFrame does not contain
                a "status" column or an "id" column.

        Args:
            df: The input DataFrame.

        Returns:
            A DataFrame mapping each unique id to
                a list of standardized project statuses.
        """
        return self._standardize_column(
            df=df,
            id_col="id",
            value_col="status",
            mapping=self._status_lookup,
        )

    def _standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardizes the columns of the input project DataFrame.

        Args:
            df: The input DataFrame.

        Returns:
            The standardized DataFrame.
        """
        # Standardize project affiliates
        self._logger.info("Standardizing project affiliates.")
        standardized_affiliates = self._standardize_affiliates(df)

        # Standardize project countries
        self._logger.info("Standardizing project countries.")
        mapped_countries = self._standardize_countries(df)

        # Standardize project finance types
        self._logger.info("Standardizing project finance types.")
        mapped_finance_types = self._standardize_finance_types(df)

        # Standardize project sectors
        self._logger.info("Standardizing project sectors.")
        mapped_sectors = self._standardize_sectors(df)

        # Standardize project statuses
        self._logger.info("Standardizing project statuses.")
        mapped_statuses = self._standardize_statuses(df)

        # Standardize project currency codes
        self._logger.info("Standardizing project currency codes.")
        mapped_currencies = self._standardize_currency_codes(df)

        # Subset columns
        self._logger.info("Subsetting columns.")
        output_cols = [
            "id",
            "updated_at_utc",
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
            "name",
            "number",
            "source",
            "total_amount",
            "total_amount_usd",
            "url",
        ]
        subset_df = df[output_cols]

        # Merge standardized affiliates
        self._logger.info("Merging standardized affiliates with projects.")
        subset_df = subset_df.merge(
            standardized_affiliates, how="left", on="id"
        )

        # Merge mapped counries
        self._logger.info("Merging mapped countries with projects.")
        subset_df = subset_df.merge(mapped_countries, how="left", on="id")

        # Merge mapped finance types
        self._logger.info("Merging mapped finance types with projects.")
        subset_df = subset_df.merge(mapped_finance_types, how="left", on="id")

        # Merge mapped sectors
        self._logger.info("Merging mapped sectors with projects.")
        subset_df = subset_df.merge(mapped_sectors, how="left", on="id")

        # Merge mapped statuses
        self._logger.info("Merging mapped statuses with projects.")
        subset_df = subset_df.merge(mapped_statuses, how="left", on="id")

        # Merge mapped currencies
        self._logger.info("Merging mapped currencies with projects.")
        subset_df = subset_df.merge(mapped_currencies, how="left", on="id")

        # Replace NaN values with empty strings in select columns
        self._logger.info("Replacing missing values.")
        date_cols = [
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
        ]
        for col in date_cols:
            subset_df[col] = subset_df[col].replace({np.nan: ""})

        number_cols = ["total_amount", "total_amount_usd"]
        for col in number_cols:
            subset_df[col] = subset_df[col].replace({np.nan: None})

        # Make name field uppercase
        self._logger.info("Making name field uppercase.")
        subset_df["name"] = subset_df["name"].str.upper()

        # Finalize column order
        self._logger.info("Finalizing column order.")
        cols = [
            "id",
            "updated_at_utc",
            "affiliates",
            "countries",
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
            "finance_types",
            "name",
            "number",
            "sectors",
            "source",
            "status",
            "total_amount",
            "total_amount_currency",
            "total_amount_usd",
            "url",
        ]
        return subset_df[cols]

    def process(self, file_obj: io.TextIOWrapper) -> pd.DataFrame:
        """Orchestrates the transformation of development project data.

        Args:
            file_obj: A file object containing project data.

        Returns:
            The cleaned project DataFrame.
        """
        # Stream project data into Pandas DataFrame
        try:
            raw_projects_df = pd.read_csv(
                file_obj,
                names=[
                    "id",
                    "created_at_utc",
                    "updated_at_utc",
                    "affiliates",
                    "countries",
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
                    "finance_types",
                    "name",
                    "number",
                    "sectors",
                    "source",
                    "status",
                    "total_amount",
                    "total_amount_currency",
                    "total_amount_usd",
                    "url",
                    "task_id",
                ],
                sep="\t",
                quotechar='"',
            )
        except Exception as e:
            raise RuntimeError(f"Failed to read project data. {e}") from None

        # Format project data, mapping to standard values when applicable
        try:
            standard_projects_df = self._standardize(raw_projects_df)
        except Exception as e:
            raise RuntimeError(
                f"Failed to standardize project data. {e}"
            ) from None

        # Convert project nominal currencies to USD
        try:
            final_projects_df = self._convert_currencies(standard_projects_df)
        except Exception as e:
            raise RuntimeError(
                f"Failed to convert project currencies to USD. {e}"
            ) from None

        return final_projects_df


class Command(BaseCommand):
    """The Django management command."""

    help_text = (
        "Reads a compressed data file with scraped development projects "
        "from the cloud or local storage, decompresses and cleans the"
        "projects, and then writes the output to a new storage location."
    )

    def add_arguments(self, parser: CommandParser) -> None:
        """Configures command line arguments.

        Args:
            parser: The command line argument parser.

        Returns:
            `None`
        """
        parser.add_argument(
            "input_fpath",
            type=str,
            help="The path to the input file.",
        )

    def handle(self, **options) -> None:
        """Generates cleaned project records.

        Args:
            **options: A dictionary of the command's positional
                and optional arguments. Includes `input_fpath`,
                the path to the input file.

        Returns:
            `None`
        """
        # Configure logger
        logger = LoggerFactory.get("CLEAN")

        # Parse command line options
        try:
            logger.info("Parsing command line options.")
            input_fpath = options["input_fpath"]
        except KeyError as e:
            logger.error(f'Missing expected command line option "{e}".')
            exit(1)

        # Intialize cleaning client
        try:
            logger.info("Initializing project cleaning client.")
            client = ProjectCleaningClient(logger)
        except Exception as e:
            logger.error(f"Failed to initialize cleaning client. {e}")
            exit(1)

        # Fetch and clean project data
        try:
            logger.info("Initiating process to load and clean project data.")
            with smart_open.open(input_fpath, "rb", encoding="utf-8") as f:
                cleaned_df = client.process(f)
            logger.info(
                f"{len(cleaned_df):,} clean project record(s) created."
            )
        except Exception as e:
            logger.error(
                f'Failed to clean project data from "{input_fpath}". {e}'
            )
            exit(1)

        # Determine path for output file
        fname = input_fpath.split("/")[-1]
        today = datetime.now(tz=UTC).strftime("%Y-%m-%d")
        base_fpath = f"transformation/{today}/{fname}.parquet"
        if settings.DEBUG:
            Path.mkdir(settings.TRANSFORM_OUTPUT_DIR, exist_ok=True)
            output_fpath = f"{settings.TRANSFORM_OUTPUT_DIR}/{base_fpath}"
        else:
            output_fpath = f"{settings.GOOGLE_CLOUD_URI_SCHEME}{base_fpath}"

        # Write cleaned data to output file
        try:
            logger.info(f'Writing clean project data to "{output_fpath}".')
            with smart_open.open(output_fpath, "wb") as f:
                cleaned_df.to_parquet(f, index=False, compression="gzip")
        except Exception as e:
            logger.error(f"Failed to write clean project data to file. {e}")
            exit(1)

        # Log success
        logger.info("Scraped project data cleaned successfully.")
