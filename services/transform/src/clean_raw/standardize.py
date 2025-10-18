"""Module for standardizing development project data."""

# Standard library imports
import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Literal

# Third-party imports
import numpy as np
import pandas as pd

# Package imports
from clean_raw.constants import (
    COUNTRY_MAP_FPATH,
    CURRENCY_MAP_FPATH,
    FINANCE_TYPE_MAP_FPATH,
    SECTOR_MAP_FPATH,
    STATUS_MAP_FPATH,
)


class DataTransformationStep(ABC):
    """Abstract base class for a data column transformation step."""

    def __init__(self) -> None:
        """Initializes a new instance of a `ColumnTransformationStep`.

        Args:
            `None`

        Returns:
            `None`
        """
        self._next = None

    @property
    def next(self) -> "DataTransformationStep":
        """The next transformation step in the chain."""
        return self._next

    def set_next(self, step: "DataTransformationStep") -> None:
        """Sets the next transformation step in the chain."""
        self._next = step

    @abstractmethod
    def execute(self, df: pd.DataFrame, id_col: str, value_col: str) -> pd.DataFrame:
        """Executes the transformation step.

        Args:
            df: The input DataFrame.

            id_col: The name of id column, used for groupings.

            value_col: The name of the column to transform.

        Returns:
            The transformed DataFrame.
        """
        raise NotImplementedError


class ExplodeDelimitedColumn(DataTransformationStep):
    """Explodes a delimited column into multiple rows."""

    def __init__(self, delimiter: str = "|") -> None:
        """Initializes a new instance of the `ExplodeDelimitedColumn` class.

        Args:
            delimiter: The delimiter used to separate values
                within columns. Defaults to "|".

        Returns:
            `None`
        """
        self._delimiter = delimiter

    def execute(
        self,
        df: pd.DataFrame,
        id_col: str,
        value_col: str,
    ) -> pd.DataFrame:
        """Executes the transformation step.

        Args:
            df: The input DataFrame.

            id_col: The name of id column, used for groupings.

            value_col: The name of the column to transform.

        Returns:
            The transformed DataFrame.
        """
        # Transform delimited strings into lists
        df.loc[:, value_col] = (
            df[value_col]
            .replace({np.nan: ""})
            .apply(lambda s: s.split(self._delimiter))
        )

        # Restructure DataFrame to consist of "project id - raw value" pairs
        exploded_df = (
            df.set_index([id_col])[value_col]  # noqa
            .apply(pd.Series)
            .stack()
            .reset_index()
        )

        # Rename columns
        exploded_df = exploded_df.rename(columns={0: value_col})

        # Subset columns
        df = exploded_df[[id_col, value_col]]

        # Apply next transformation step or return output if steps complete
        return self.next.execute(df, id_col, value_col) if self.next else df


class FormatValues(DataTransformationStep):
    """Formats raw values by replacing missing values and standardizing case."""

    def execute(
        self,
        df: pd.DataFrame,
        id_col: str,
        value_col: str,
    ) -> pd.DataFrame:
        """Executes the transformation step.

        Args:
            df: The input DataFrame.

            id_col: The name of id column, used for groupings.

            value_col: The name of the column to transform.

        Returns:
            The transformed DataFrame.
        """
        df.loc[:, value_col] = (
            df[value_col].replace({np.nan: ""}).apply(lambda val: val.strip().upper())
        )
        return self.next.execute(df, id_col, value_col) if self.next else df


class MakeListColumn(DataTransformationStep):
    """Makes the given value column list-like by aggregating by id."""

    def execute(self, df: pd.DataFrame, id_col: str, value_col: str) -> pd.DataFrame:
        """Executes the transformation step.

        Args:
            df: The input DataFrame.

            id_col: The name of id column, used for groupings.

            value_col: The name of the column to transform.

        Returns:
            The transformed DataFrame.
        """
        df = (
            df[[id_col, value_col]]
            .groupby(id_col)[value_col]
            .agg(lambda s: sorted(set(s)))
            .reset_index(name=value_col)
        )
        return self.next.execute(df, id_col, value_col) if self.next else df


class MapValues(DataTransformationStep):
    """Maps raw column values to standard values defined in a config file."""

    def __init__(
        self,
        mapping_fpath: Path,
        mapping_schema: Literal[
            "standard-to-raw-list", "raw-to-standard"
        ] = "standard-to-raw-list",
        unknown_value: str = "Unknown",
    ):
        """Initializes a new instance of `MapValues`.

        Args:
            mapping_file: The path to the mapping file.
                Assumed to be JSON.

            mapping_schema: The schema of the mapping file.
                Defaults to "standard-to-raw-list".

            unknown_value: Value to use for unknown values.
                Defaults to "Unknown".

        Returns:
            `None`
        """
        # Load mapping file
        try:
            with open(mapping_fpath, encoding="utf-8") as f:
                mapping = json.load(f)
        except FileNotFoundError:
            raise RuntimeError(
                "Error fetching mapping file. The file does not exist."
            ) from None
        except json.JSONDecodeError:
            raise RuntimeError("Error parsing mapping file into JSON.") from None

        # Parse file based on schema
        self._mapping = (
            mapping
            if mapping_schema == "raw-to-standard"
            else (
                {
                    val: output
                    for output, input_lst in mapping.items()
                    for val in input_lst
                }
            )
        )

        # Set unknown value
        self._unknown_value = unknown_value

    def execute(
        self,
        df: pd.DataFrame,
        id_col: str,
        value_col: str,
    ) -> pd.DataFrame:
        """Executes the transformation step.

        Args:
            df: The input DataFrame.

            id_col: The name of id column, used for groupings.

            value_col: The name of the column to transform.

        Returns:
            The transformed DataFrame.
        """
        df.loc[:, value_col] = (
            df[value_col].map(self._mapping).replace({np.nan: self._unknown_value})
        )
        return self.next.execute(df, id_col, value_col) if self.next else df


class PreprocessValuesForMapping(DataTransformationStep):
    """Pre-processes raw values ahead of their mapping to standard values."""

    def execute(
        self,
        df: pd.DataFrame,
        id_col: str,
        value_col: str,
    ) -> pd.DataFrame:
        """Executes the transformation step.

        Args:
            df: The input DataFrame.

            id_col: The name of id column, used for groupings.

            value_col: The name of the column to transform.

        Returns:
            The transformed DataFrame.
        """
        df.loc[:, value_col] = (
            df[value_col].replace({np.nan: ""}).apply(lambda val: val.strip().lower())
        )
        return self.next.execute(df, id_col, value_col) if self.next else df


class SubsetColumns(DataTransformationStep):
    """Subsets a column to include only the id and value columns."""

    def execute(
        self,
        df: pd.DataFrame,
        id_col: str,
        value_col: str,
    ) -> pd.DataFrame:
        """Executes the transformation step.

        Args:
            df: The input DataFrame.

            id_col: The name of id column, used for groupings.

            value_col: The name of the column to transform.

        Returns:
            The transformed DataFrame.
        """
        copy = df[[id_col, value_col]].copy()
        return self.next.execute(copy, id_col, value_col) if self.next else copy


class PostprocessValuesAfterMapping(DataTransformationStep):
    """Formats column values after mapping."""

    def execute(
        self,
        df: pd.DataFrame,
        id_col: str,
        value_col: str,
    ) -> pd.DataFrame:
        """Executes the transformation step.

        Args:
            df: The input DataFrame.

            id_col: The name of id column, used for groupings.

            value_col: The name of the column to transform.

        Returns:
            The transformed DataFrame.
        """
        df.loc[:, value_col] = df[value_col].str.upper()
        return self.next.execute(df, id_col, value_col) if self.next else df


class ValidateColumns(DataTransformationStep):
    """Validates the existence of value and id columns."""

    def execute(
        self,
        df: pd.DataFrame,
        id_col: str,
        value_col: str,
    ) -> pd.DataFrame:
        """Executes the transformation step.

        Args:
            df: The input DataFrame.

            id_col: The name of id column, used for groupings.

            value_col: The name of the column to transform.

        Returns:
            The transformed DataFrame.
        """
        # Validate existence of value column
        if value_col not in df.columns:
            raise ValueError(f'DataFrame must contain the column "{value_col}".')

        # Validate existence of id column
        if id_col not in df.columns:
            raise ValueError(f'DataFrame must contain the column "{id_col}".')

        return self.next.execute(df, id_col, value_col) if self.next else df


def execute_steps(
    df: pd.DataFrame,
    id_col: str,
    value_col: str,
    steps: list[DataTransformationStep],
) -> pd.DataFrame:
    """Executes a list of transformation steps in sequence.

    Args:
        df: The input DataFrame.

        id_col: The name of id column, used for groupings.

        value_col: The name of the column to transform.

        steps: A list of transformation steps.

    Returns:
        The transformed DataFrame.
    """
    for i in range(len(steps) - 1):
        steps[i].set_next(steps[i + 1])
    return steps[0].execute(df, id_col, value_col)


def standardize_affiliates(
    df: pd.DataFrame, id_col: str, value_col: str
) -> pd.DataFrame:
    """Standardizes the affiliates column.

    Args:
        df: The input DataFrame.

        id_col: The name of id column, used for groupings.

        value_col: The name of the column to transform.

    Returns:
        A DataFrame consisting of the ids and transformed values.
    """
    return execute_steps(
        df,
        id_col,
        value_col,
        steps=[
            ValidateColumns(),
            SubsetColumns(),
            ExplodeDelimitedColumn(),
            FormatValues(),
            MakeListColumn(),
        ],
    )


def standardize_countries(
    df: pd.DataFrame, id_col: str, value_col: str
) -> pd.DataFrame:
    """Standardizes the countries column.

    Args:
        df: The input DataFrame.

        id_col: The name of id column, used for groupings.

        value_col: The name of the column to transform.

    Returns:
        A DataFrame consisting of the ids and transformed values.
    """
    return execute_steps(
        df,
        id_col,
        value_col,
        steps=[
            ValidateColumns(),
            SubsetColumns(),
            ExplodeDelimitedColumn(),
            PreprocessValuesForMapping(),
            MapValues(COUNTRY_MAP_FPATH),
            PostprocessValuesAfterMapping(),
            MakeListColumn(),
        ],
    )


def standardize_currency_codes(
    df: pd.DataFrame, id_col: str, value_col: str
) -> pd.DataFrame:
    """Standardizes the currency codes column.

    Args:
        df: The input DataFrame.

        id_col: The name of id column, used for groupings.

        value_col: The name of the column to transform.

    Returns:
        A DataFrame consisting of the ids and transformed values.
    """
    return execute_steps(
        df,
        id_col,
        value_col,
        steps=[
            ValidateColumns(),
            SubsetColumns(),
            PreprocessValuesForMapping(),
            MapValues(CURRENCY_MAP_FPATH),
            PostprocessValuesAfterMapping(),
        ],
    )


def standardize_finance_types(
    df: pd.DataFrame, id_col: str, value_col: str
) -> pd.DataFrame:
    """Standardizes the finance types column.

    Args:
        df: The input DataFrame.

        id_col: The name of id column, used for groupings.

        value_col: The name of the column to transform.

    Returns:
        A DataFrame consisting of the ids and transformed values.
    """
    return execute_steps(
        df,
        id_col,
        value_col,
        steps=[
            ValidateColumns(),
            SubsetColumns(),
            ExplodeDelimitedColumn(),
            PreprocessValuesForMapping(),
            MapValues(FINANCE_TYPE_MAP_FPATH),
            PostprocessValuesAfterMapping(),
            MakeListColumn(),
        ],
    )


def standardize_sectors(df: pd.DataFrame, id_col: str, value_col: str) -> pd.DataFrame:
    """Standardizes the sectors column.

    Args:
        df: The input DataFrame.

        id_col: The name of id column, used for groupings.

        value_col: The name of the column to transform.

    Returns:
        A DataFrame consisting of the ids and transformed values.
    """
    return execute_steps(
        df,
        id_col,
        value_col,
        steps=[
            ValidateColumns(),
            SubsetColumns(),
            ExplodeDelimitedColumn(),
            PreprocessValuesForMapping(),
            MapValues(SECTOR_MAP_FPATH, "raw-to-standard"),
            ExplodeDelimitedColumn(),
            PostprocessValuesAfterMapping(),
            MakeListColumn(),
        ],
    )


def standardize_statuses(df: pd.DataFrame, id_col: str, value_col: str) -> pd.DataFrame:
    """Standardizes the currency codes column.

    Args:
        df: The input DataFrame.

        id_col: The name of id column, used for groupings.

        value_col: The name of the column to transform.

    Returns:
        A DataFrame consisting of the ids and transformed values.
    """
    return execute_steps(
        df,
        id_col,
        value_col,
        steps=[
            ValidateColumns(),
            SubsetColumns(),
            PreprocessValuesForMapping(),
            MapValues(STATUS_MAP_FPATH),
            PostprocessValuesAfterMapping(),
        ],
    )


def standardize_columns(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """Standardizes the columns of the input project DataFrame.

    Args:
        df: The input DataFrame.

        logger: An instance of a standard logger.

    Returns:
        The standardized DataFrame.
    """
    # Make name column uppercase
    logger.info("Applying uppercase format to name column.")
    df["name"] = df["name"].str.upper()

    # Standardize project affiliates
    logger.info("Standardizing project affiliates.")
    standardized_affiliates = standardize_affiliates(df, "id", "affiliates")

    # Standardize project countries
    logger.info("Standardizing project countries.")
    mapped_countries = standardize_countries(df, "id", "countries")

    # Standardize project finance types
    logger.info("Standardizing project finance types.")
    mapped_finance_types = standardize_finance_types(df, "id", "finance_types")

    # Standardize project sectors
    logger.info("Standardizing project sectors.")
    mapped_sectors = standardize_sectors(df, "id", "sectors")

    # Standardize project statuses
    logger.info("Standardizing project statuses.")
    mapped_statuses = standardize_statuses(df, "id", "status")

    # Standardize project currency codes
    logger.info("Standardizing project currency codes.")
    mapped_currencies = standardize_currency_codes(df, "id", "total_amount_currency")

    # Subset columns
    logger.info("Subsetting columns prior to merge.")
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
        "fiscal_year_effective",
        "name",
        "number",
        "source",
        "total_amount",
        "total_amount_usd",
        "url",
    ]
    subset_df = df[output_cols]

    # Merge standardized affiliates
    logger.info("Merging standardized affiliates with projects.")
    subset_df = subset_df.merge(standardized_affiliates, how="left", on="id")

    # Merge mapped counries
    logger.info("Merging mapped countries with projects.")
    subset_df = subset_df.merge(mapped_countries, how="left", on="id")

    # Merge mapped finance types
    logger.info("Merging mapped finance types with projects.")
    subset_df = subset_df.merge(mapped_finance_types, how="left", on="id")

    # Merge mapped sectors
    logger.info("Merging mapped sectors with projects.")
    subset_df = subset_df.merge(mapped_sectors, how="left", on="id")

    # Merge mapped statuses
    logger.info("Merging mapped statuses with projects.")
    subset_df = subset_df.merge(mapped_statuses, how="left", on="id")

    # Merge mapped currencies
    logger.info("Merging mapped currencies with projects.")
    subset_df = subset_df.merge(mapped_currencies, how="left", on="id")

    # Replace NaN values with empty strings in date columns
    logger.info("Replacing missing values.")
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
        "fiscal_year_effective",
    ]
    for col in date_cols:
        subset_df[col] = subset_df[col].replace({np.nan: ""})

    # Replace NaN values with None in number columns
    number_cols = ["total_amount", "total_amount_usd"]
    for col in number_cols:
        subset_df[col] = subset_df[col].replace({np.nan: None})

    return subset_df
