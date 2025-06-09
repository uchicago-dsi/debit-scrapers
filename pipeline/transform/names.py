"""Provides classes to standardize values.
"""

import json
import numpy as np
import pandas as pd
from json.decoder import JSONDecodeError
from pipeline.constants import CONFIG_DIR_PATH
from typing import Dict, List, Tuple


class NameStandardizer:
    """Standardizes names by referencing configured data stores
    (e.g., for countries, business sectors, and project statuses).
    """

    def __init__(self) -> None:
        """Initializes a new instance of the `NameStandardizer`.
        
        Args:
            `None`

        Returns:
            `None`
        """
        # Load country mappings
        country_store = self._build_data_store("countries.json")
        self._standard_countries, self._country_map = country_store

        # Load project status mappings
        status_store = self._build_data_store("statuses.json")
        self._standard_sectors, self._status_map = status_store

        # Load business sector mappings
        sector_store = self._build_data_store("sectors.json")
        self._standard_sectors, self._sectors_map = sector_store

        # Define constant for values not found in any store
        self._UNKNOWN_VALUE = 'Unknown'

    def _build_data_store(self, file_name: str) -> Tuple[List[str], Dict]:
        """Reads a mapping file and then parses to return lists,
        and mappings between, standard and non-standard values.

        Args:
            file_name (`str`): The name of the file to load
                under the configuration directory.

        Returns:
            ((`list` of `str`, `dict`,)): A two-item tuple consisting
                of a list of standard/reference values and a mapping
                from non-standard values (aliases) to their corresponding
                standard value.
        """
        # Load file mapping standard names to names found "in the wild"
        try:
            with open(f"{CONFIG_DIR_PATH}/{file_name}") as f:
                map = json.load(f)
        except FileNotFoundError as e:
            raise RuntimeError("Failed to load required mappings from "
                               f"file \"{file_name}\". {e}") from e
        except JSONDecodeError as e:
            raise RuntimeError(f"Contents of file \"{file_name}\" "
                               f"could not be decoded to JSON. {e}") from e
        
        # Restructure map as "alias"-"standard name" key-value pairs
        val_map = { 
            v:key 
            for key, values in map.items()
            for v in (values if isinstance(values, list) else values["aliases"])
        }

        # Store reference to standard values in map
        standard_vals = list(map.keys())

        return standard_vals, val_map
    
    def _map_list_col(
        self,
        raw_df: pd.DataFrame,
        id_field: str,
        list_field: str,
        mapping: Dict) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Breaks a column consisting of lists of values into a column with
        one row per value. Then maps each value to a standard representation.

        Args:
            raw_df (`pd.DataFrame`): A Pandas DataFrame containing at least
                two columns: one containing a unique primary key for each
                row and the other holding the column of lists.

            id_field (`str`): The name of the DataFrame column holding unique
                identifiers for the data entity.

            list_field (`str`): The name of the DataFrame column holding
                country names.

        Returns:
            (`pd.DataFrame`, `pd.DataFrame`): A two-item tuple. The first result
                is a DataFrame consisting of id and standardized value pairs.
                (For example, a project may be associated with zero, one, or
                more countries, so each row would consist of a project id and
                a country id.) The second item is a DataFrame consisting of
                ids and a list of standardized values (e.g., for project
                countries, the project id as one column and the other column
                holding values like "China, Nepal, Venezuela").
        """
        # Shorten column references
        id = id_field
        list_col = list_field

        # Format list column to be lowercase and of type list
        format_list_col = lambda s: s.lower().split(',') if s else None
        raw_df.loc[:, list_col] = (raw_df[list_col]
            .replace({np.nan : None})
            .apply(format_list_col))
        
        # Restructure DataFrame to consist of "project id - raw value" pairs
        expanded_df = (raw_df
            .set_index([id])[list_col]
            .apply(pd.Series)
            .stack()
            .reset_index())

        # Subset and rename columns to get project id and list column value only
        expanded_df = expanded_df[[id, 0]]
        expanded_df.columns = [id, list_col]

        # Concatenate with project records lacking list col values,
        # which were dropped when creating the expanded DataFrame
        no_values_df = raw_df.query(f'{list_col} != {list_col}')[[id, list_col]]
        mapped_df = pd.concat([no_values_df, expanded_df])

        # Map current country names to standard names
        def map_to_standard_name(raw_name: str):
            """Looks up the standard name for the given country."""
            if not raw_name:
                return self._UNKNOWN_VALUE
            return mapping.get(raw_name.strip(), self._UNKNOWN_VALUE)

        mapped_df[list_col] = mapped_df[list_col].apply(map_to_standard_name)

        # Sort by id, followed by standardized country name value
        mapped_df = mapped_df.sort_values(by=[id, list_col], ascending=True)

        # Create alternative DataFrame with one or more standard names as a string
        mapped_list_df = (mapped_df
            .groupby(id)[list_col]
            .apply(list)
            .apply(lambda lst: ', '.join(sorted(lst)))
            .reset_index())
        mapped_list_df.columns = [id, list_col]

        return mapped_df, mapped_list_df

    def map_country_names(
        self,
        raw_countries_df: pd.DataFrame,
        id_field: str,
        country_field: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Standardizes country names.

        Args:
            raw_countries_df (`pd.DataFrame`): A Pandas DataFrame containing
                at least two columns: one containing data entity identifiers
                and another with country names. The country name column is
                expected to be of `str` type and contain multiple values
                separated by commas in some rows.

            id_field (`str`): The name of the DataFrame column holding unique
                identifiers for the data entity associated with countries
                (e.g., projects, complaints).

            country_field (`str`): The name of the DataFrame column holding
                country names.

        Returns:
            (`pd.DataFrame`, `pd.DataFrame`): A two-item tuple. The first result
                is a DataFrame consisting of project id and standardized
                country name pairs. (A project may be associated with
                zero, one, or more countries.) The second item is a DataFrame
                consisting of project ids and their corresponding standardized
                country names as a string (e.g., "China, Nepal, Venezuela").
        """
        return self._map_list_col(
            raw_df=raw_countries_df,
            id_field=id_field,
            list_field=country_field,
            mapping=self._country_map
        )

    def map_project_sectors(
        self,
        raw_sectors_df: pd.DataFrame,
        id_field: str,
        sector_field: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Standardizes sector names.

        Args:
            raw_sectors_df (`pd.DataFrame`): A Pandas DataFrame containing
                at least two columns: one containing data entity identifiers
                and another with sector names. The sector name column is
                expected to be of `str` type and contain multiple values
                separated by commas in some rows.

            id_field (`str`): The name of the DataFrame column holding unique
                identifiers for the data entity associated with sectors
                (e.g., projects).

            sector_field (`str`): The name of the DataFrame column holding
                sector names.

        Returns:
            (`pd.DataFrame`, `pd.DataFrame`): A two-item tuple. The first result
                is a DataFrame consisting of project id and standardized
                sector name pairs. (A project may be associated with
                zero, one, or more sectors.) The second item is a DataFrame
                consisting of project ids and their corresponding standardized
                sector names as a string (e.g., "Agribusiness, Education").
        """
        return self._map_list_col(
            raw_df=raw_sectors_df,
            id_field=id_field,
            list_field=sector_field,
            mapping=self._sectors_map
        )

    def map_project_statuses(self, raw_statuses: pd.Series) -> pd.Series:
        """Maps project statuses to one of four values:
        "Cancelled", "Completed", "Ongoing", or "Pending".

        Args:
            raw_statuses (`pd.Series`): The project statuses.

        Returns:
            (`pd.Series`): The mapped/standardized project statuses. 
        """
        func = lambda s: self._status_map.get(s.lower(), self._UNKNOWN_VALUE)
        return raw_statuses.apply(func)

