"""Jobs for transforming (i.e., cleaning, standardizing,
reshaping, and data augmenting) staged database records.
"""

import json
import numpy as np
import pandas as pd
from abc import ABC, abstractmethod
from datetime import datetime
from json.decoder import JSONDecodeError
from pipeline.constants import (
    COMPLETED_STATUS,
    COUNTRIES_FPATH,
    DEV_BANK_PROJECTS_JOB_TYPE,
    ERROR_STATUS,
    FORM_13F_JOB_TYPE,
    IN_PROGRESS_STATUS
)
from pipeline.services.database import DbClient
from pipeline.services.logger import logging
from pipeline.transform.finance import CurrencyClient, StocksClient
from pipeline.transform.names import NameStandardizer
from werkzeug.exceptions import BadRequest, InternalServerError
from typing import Callable, Dict, Tuple


class DataTransformClient(ABC):
    """An abstract representation of a data transformation
    client, which orchestrates cleaning, standardization, 
    and merging of staged data.
    """

    def __init__(
        self,
        db_client: DbClient,
        logger: logging.Logger) -> None:
        """Initializes a new instance of a `DataTransformClient`.

        Args:
            db_client (`db_client`): An instance of the database client.

            logger (`logging.Logger`): An instance of a standard logger.

        Returns:
            `None`
        """
        self._logger = logger
        self._db_client = db_client

    def _load_dataset(
        self,
        name: str, 
        load_func: Callable,
        **kwargs) -> pd.DataFrame:
        """Fetches the dataset with the given name from 
        the database and then loads it into a DataFrame.

        Args:
            name (`str`): The name of the dataset. Used
                for logging purposes.

            load_func (`Callable`): The function use to load
                the dataset into memory as a Pandas DataFrame.

        Returns:
            (`pd.DataFrame`): The loaded data.
        """
        try:
            self._logger.info(
                f"Retrieving {name} records from database "
                "and reading into DataFrame."
            )
            records = load_func(**kwargs)
            if not records:
                raise RuntimeError(f"No {name} records found.")
            return pd.DataFrame(records)
        except Exception as e:
            raise InternalServerError(
                f"Error retrieving {name} records from database "
                f"and reading into DataFrame. {e}"
            ) from None

    @abstractmethod
    def transform_data(self) -> None:
        """Transforms a dataset collection and persists
        the result in the database.

        Args:
            `None`

        Returns:
            `None`
        """
        raise NotImplementedError

class ProjectTransformClient(DataTransformClient):
    """A client to transform raw project records into cleaned
    projects, project-countries, and project-sectors.
    """

    def __init__(
        self,
        currency_client: CurrencyClient,
        name_standardizer: NameStandardizer,
        db_client: DbClient, 
        logger: logging.Logger) -> None:
        """Initializes a new instance of a `ProjectTransformClient`.

        Args:
            currency_client (`CurrencyClient`): An instance of
                a currency client.

            name_standardizer (`NameStandardizer`): An instance of a 
                client used to standardize names.

            db_client (`db_client`): An instance of the 
                database client.

            logger (`logging.Logger`): An instance of 
                a standard logger.

        Returns:
            `None`
        """
        # Load file mapping standard country names to ISO-2 codes
        try:
            with open(COUNTRIES_FPATH) as f:
                self._country_codes = json.load(f)
        except FileNotFoundError as e:
            raise RuntimeError("Failed to load country codes from path "
                               f"\"{COUNTRIES_FPATH}\". {e}") from e
        except JSONDecodeError as e:
            raise RuntimeError(f"Contents of country codes file "
                               f"could not be decoded to JSON. {e}") from e
        
        # Initialize other clients
        self._currency_client = currency_client
        self._name_standardizer = name_standardizer        
        super().__init__(db_client, logger)

    def build_projects(
        self,
        staged_proj_df: pd.DataFrame,
        banks_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Cleans staged project records and then merges them with bank
        records retrieved from the database to create a set of
        projects and project-country associations ready for database load.

        Args:
            staged_proj_df (`pd.DataFrame`): Staged project records
                generated during the web scraping process.

            banks_df (`pd.DataFrame`): All development bank records
                retrieved from the database.

        Returns:
            (`pd.DataFrame`, `pd.DataFrame`) -> A two-item tuple 
                consisting of the project and project-country 
                (association) records.
        """
        # Mark start of staged project processing
        self._logger.info(f"Retrieved {len(staged_proj_df)} "
                         "staged project(s).")

        # Standardize project statuses
        self._logger.info("Standardizing project statuses.")
        statuses = staged_proj_df["status"]
        staged_proj_df.loc[:, "status"] = \
            self._name_standardizer.map_project_statuses(statuses)

        # Standardize project country names
        self._logger.info("Standardizing project country names.")
        _, proj_country_lists_df = self._name_standardizer.map_country_names(
            raw_countries_df=staged_proj_df[["id", "countries"]],
            id_field="id",
            country_field="countries")
        proj_country_lists_df.columns = ["id", "country_list"]
        staged_proj_df = \
            staged_proj_df.merge(proj_country_lists_df, on="id", how="left")
        
        # Define local function to normalize project loan amounts
        def normalize_currencies(row: pd.Series):
            """Converts a project's loan amount to 2017 U.S. dollars
            by applying a foreign exchange rate conversion 
            followed by a GDP price deflator.
            """
            try:
                if not row["country_list"]:
                    raise ValueError("No countries listed in record.")
                
                countries = row["country_list"].split(", ")
                country_code = self._country_codes[countries[0]]["iso2_code"]
                
                return self._currency_client.normalize(
                    year=row["year"],
                    country_code=country_code,
                    currency_code=row["loan_amount_currency"],
                    amount=row["loan_amount"]
                )
            except Exception as e:
                self._logger.warning("Unable to normalize currency "
                                     f"for record {str(row)}. \"{e}\".")
                return None

        # Convert currencies
        self._logger.info("Normalizing project loan amounts.")
        staged_proj_df.loc[:, "loan_amount_in_usd"] = \
            staged_proj_df.apply(normalize_currencies, axis="columns")

        # Retrieve standard project sector names
        self._logger.info("Standardizing project sector names.")
        _, project_sector_lsts_df = self._name_standardizer.map_project_sectors(
            raw_sectors_df=staged_proj_df[["id", "sectors"]],
            id_field="id",
            sector_field="sectors")
        project_sector_lsts_df.columns = ["id", "sector_list"]
        staged_proj_df = \
            staged_proj_df.merge(project_sector_lsts_df, on="id", how="left")

        # Merge with banks
        self._logger.info("Merging projects with bank data.")
        col_map = {"id": "bank_id", "abbrev_name": "bank"}
        banks_df = banks_df.rename(columns=col_map)
        banks_df["bank"] = banks_df["bank"].str.upper()
        staged_proj_df = staged_proj_df.merge(
            right=banks_df[["bank_id", "bank", "ac_name"]],
            on="bank",
            how="left")

        # Subset columns to finalize project DataFrame
        self._logger.info("Subsetting columns to create "
                          "final projects DataFrame.")
        staged_proj_df = staged_proj_df[[
            "bank_id",
            "number",
            "name",
            "ac_name",
            "status",
            "year",
            "month",
            "day",
            "loan_amount",
            "loan_amount_currency",
            "loan_amount_in_usd",
            "sectors",
            "sector_list",
            "companies",
            "countries",
            "country_list",
            "url"
        ]]

        # Rename fields
        col_mapping = {
            "countries": "country_list_raw",
            "country_list": "country_list_stnd",
            "sectors": "sector_list_raw",
            "sector_list": "sector_list_stnd"
        }
        staged_proj_df = staged_proj_df.rename(columns=col_mapping)

        # Remove NaN values
        staged_proj_df = staged_proj_df.replace(np.nan, None)

        # Finalize data types
        staged_proj_df["year"] = staged_proj_df["year"].astype("Int64")
        staged_proj_df["month"] = staged_proj_df["month"].astype("Int64")
        staged_proj_df["day"] = staged_proj_df["day"].astype("Int64")

        # De-dupe project records based on url
        staged_proj_df = \
            staged_proj_df.drop_duplicates(subset=["url"], keep=False)
        self._logger.info(f"{len(staged_proj_df)} project record(s) "
                    "remaining after dropping dupes.")

        # Replace new line characters with whitespace
        self._logger.info("Replacing special characters \\t, "
                    "\\n, and \\r with whitespace.")
        strip_cols = [
            "name",
            "companies",
            "country_list_raw",
            "sector_list_raw"
        ]
        for col in strip_cols:
            params = Dict(to_replace=r"\n|\t|\r", value=" ", regex=True)
            staged_proj_df[col] = staged_proj_df[col].replace(**params)

        return staged_proj_df

    def build_project_countries(
        self,
        upserted_projects_df: pd.DataFrame,
        countries_df: pd.DataFrame) -> pd.DataFrame:
        """Creates a DataFrame consisting of 
        "project id"-"country id" pairs.

        Args:
            upserted_projects_df (`pd.DataFrame`): 
                The upserted project records.

            countries_df (`pd.DataFrame`): All country records
                retrieved from the database. (Only `id` and
                `name` fields expected.)

        Returns:
            (`pd.DataFrame`): The association pairs.
        """
        # Convert country name column from delimited strings to list type
        upserted_projects_df["country_list_stnd"] = \
            upserted_projects_df["country_list_stnd"].str.split(", ")
        
        # Explode country list column to produce many rows
        proj_country_pairs_df = (upserted_projects_df
                                .loc[["id", "country_list_stnd"]]
                                .explode("country_list_stnd"))

        # Merge project-country pairs with country ids from database
        id_p = "project_id"
        id_c = "country_id"
        proj_country_pairs_df = (proj_country_pairs_df
            .merge(
                right=countries_df[["id", "name"]],
                left_on="country_list_stnd",
                right_on="name",
                how="left",
                suffixes=("_p", "_c")
            )
            .rename(columns={
                "id_p": id_p,
                "id_c": id_c
            }))

        # Clean DataFrame
        proj_country_pairs_df = proj_country_pairs_df[[id_p, id_c]]
        proj_country_pairs_df = proj_country_pairs_df.dropna(axis="index")
        proj_country_pairs_df[id_p] = proj_country_pairs_df[id_p].astype(int)
        proj_country_pairs_df[id_c] = proj_country_pairs_df[id_c].astype(int)

        return proj_country_pairs_df

    def build_project_sectors(
        self,
        upserted_projects_df: pd.DataFrame,
        sectors_df: pd.DataFrame) -> pd.DataFrame:
        """Creates a DataFrame consisting of project id-sector id pairs.

        Args:
            upserted_projects_df (`pd.DataFrame`): The upserted project records.

            sectors_df (`pd.DataFrame`): All sector records
                retrieved from the database. (Only `id` and
                `name` fields expected.)

        Returns:
            (`pd.DataFrame`): The association pairs.
        """
        # Convert sector column from delimited strings to list type
        upserted_projects_df["sector_list_stnd"] = \
            upserted_projects_df["sector_list_stnd"].str.split(", ")
        
        # Explode sector list column to produce many rows
        proj_sector_pairs_df = (upserted_projects_df
                                .loc[["id", "sector_list_stnd"]]
                                .explode("sector_list_stnd"))
        
        # Merge sector pairs with sector ids from database
        id_p = "project_id"
        id_s = "sector_id"
        proj_sector_pairs_df = (proj_sector_pairs_df
            .merge(
                right=sectors_df[["id", "name"]],
                left_on="sector_list_stnd",
                right_on="name",
                how="left",
                suffixes=("_p", "_s")
            )
            .rename(columns={
                "id_p": id_p,
                "id_s": id_s
            }))

        # Clean DataFrame
        proj_sector_pairs_df = proj_sector_pairs_df[[id_p, "sector_id"]]
        proj_sector_pairs_df = proj_sector_pairs_df.dropna(axis="index")
        proj_sector_pairs_df[id_p] = proj_sector_pairs_df[id_p].astype(int)
        proj_sector_pairs_df[id_s] = proj_sector_pairs_df[id_s].astype(int)

        return proj_sector_pairs_df

    def transform_data(self) -> None:
        """Transforms staged project records.

        Args:
            `None`

        Returns:
            `None`
        """        
        # Load initial datasets for merge
        banks_df = self._load_dataset(
            name="bank",
            load_func=self._db_client.get_banks
        )
        countries_df = self._load_dataset(
            name="country", 
            load_func=self._db_client.get_countries
        )
        sectors_df = self._load_dataset(
            name="sector",
            load_func=self._db_client.get_sectors
        )

        while True:
            # Retrieve batch of staged projects from database.
            staged_projects_df = self._load_dataset(
                name="staged project", 
                load_func=self._db_client.get_staged_projects,
                limit=5000)
            
            # Stop processing if no staged projects remaining
            if staged_projects_df is None:
                break

            # Build projects dataset
            try:
                self._logger.info("Cleaning staged project records and then "
                            "merging them with bank records to produce "
                            "final projects dataset.")
                projects_df = self.build_projects(
                    staged_projects_df,
                    banks_df,
                )
            except Exception as e:
                raise InternalServerError("Failed to clean staged "
                                        f"project records. {e}")
            
            # Upsert projects to database
            try:
                self._logger.info("Upserting finalized project "
                                  "records into database.")
                proj_records = projects_df.to_dict(orient="records")
                upserted_projects, _ = \
                    self._db_client.bulk_upsert_finalized_projects(proj_records)
                self._logger.info(f"{len(upserted_projects)} records "
                            "upserted successfully.")
            except Exception as e:
                raise InternalServerError("Error saving clean project "
                                        f"records to database table. {e}")

            # Create project-countries
            try:
                self._logger.info("Creating project-countries.")
                upserted_projects_df = pd.DataFrame(upserted_projects)
                project_countries_df = self.build_project_countries(
                    upserted_projects_df,
                    countries_df)
            except Exception as e:
                raise InternalServerError(f"Failed to "
                    f"create project-country id pairs. {e}") from None
            
            # Upsert project-countries to database
            try:
                self._logger.info("Inserting project-country "
                                  "pairs into database.")
                proj_ctries = \
                    project_countries_df.to_dict(orient="records")
                inserted_pctries, status = \
                    self._db_client.bulk_insert_project_countries(proj_ctries)
                if status == 200:
                    self._logger.info("Records already exist. "
                                "No new records were added.")
                if status == 201:
                    self._logger.info(f"{len(inserted_pctries)} record(s) "
                                "inserted successfully.")
            except Exception as e:
                raise InternalServerError("Error saving clean "
                    f"project-country records to database table. {e}") from None

            # Create project-sectors
            try:
                self._logger.info("Creating project-sectors.")
                project_sectors_df = self.build_project_sectors(
                    upserted_projects_df,
                    sectors_df)
            except Exception as e:
                raise InternalServerError("Failed to create project-sector "
                                        f"id pairs. {e}") from None
            
            # Upsert project-sector records to database
            try:
                self._logger.info("Inserting project-sector "
                                  "pairs into database.")
                psector_records = project_sectors_df.to_dict(orient="records")
                inserted_psectors, status = \
                    self._db_client.bulk_insert_project_sectors(psector_records)
                if status == 200:
                    self._logger.info("Records already exist. "
                                "No new records were added.")
                if status == 201:
                    self._logger.info(f"{len(inserted_psectors)} record(s) "
                                "inserted successfully.")
            except Exception as e:
                raise InternalServerError("Error saving clean "
                    f"project-sector records to database table. {e}") from None

            # Delete staged project records from table
            try:
                self._logger.info(f"Deleting {len(staged_projects_df)} "
                    "staged projects from table.")
                ids = staged_projects_df["id"].tolist()
                num_deleted_records = \
                    self._db_client.delete_staged_projects_by_id(ids)
                self._logger.info(f"{num_deleted_records} record(s) "
                            "successfully deleted.")
            except Exception as e:
                raise InternalServerError("Error deleting staged "
                                        f"investments. {e}") from None

class InvestmentTransformClient(DataTransformClient):
    """A client to transform staged Form 13F data into
    company, form, and investment records. 
    """

    def __init__(
        self,
        stocks_client: StocksClient,
        db_client: DbClient, 
        logger: logging.Logger) -> None:
        """Initializes a new instance of an `InvestmentTransformJob`.

        Args:
            stocks_client (`StocksClient`): A client used for
                fetching stock metadata.

            db_client (`db_client`): An instance of the database client.

            logger (`logging.Logger`): An instance of a standard logger.

        Returns:
            `None`
        """
        self._stocks_client = stocks_client
        super().__init__(db_client, logger)

    def build_companies(
        self, 
        staged_investments_df: pd.DataFrame,) -> pd.DataFrame:
        """Parses unique company records from
        stagd Form 13F investment records.

        Args:
            staged_investments_df (`pd.DataFrame`): 
                The staged Form 13F investments.

        Returns:
            (`pd.DataFrame`): The company records.
        """
        companies_df = (staged_investments_df
            .loc[:, ["company_cik", "company_name"]]
            .drop_duplicates()
            .sort_values(by=["company_cik"])
            .rename(columns={"company_cik": "cik", "company_name": "name"}))
        return companies_df

    def build_forms(
        self,
        staged_investments_df: pd.DataFrame,
        upserted_companies_df: pd.DataFrame) -> pd.DataFrame:
        """Parses unique form submissions from staged investments
        and then merges the submissions with newly-created 
        company database identifiers.
        
        Args:
            staged_investments_df (`pd.DataFrame`): The
                staged Form 13F investments.

            upserted_companies_df (`pd.DataFrame`): The
                set of associated companies, fetched from
                the database.

        Returns:
            (`pd.DataFrame`): The finalized forms.
        """
        self._logger.info("Parsing unique forms from staged "
            "investments and merging with newly-created company ids.")
        form_df = (staged_investments_df
            .loc[[
                "company_cik",
                "company_name",
                "form_name",
                "form_accession_number",
                "form_report_period",
                "form_filing_date",
                "form_acceptance_date",
                "form_effective_date",
                "form_url"
            ]]
            .drop_duplicates()
            .rename(columns={"company_cik": "cik", "company_name": "name"})
            .merge(upserted_companies_df, on=["cik", "name"])
            .drop(columns=["cik", "name"], axis=1)
            .rename(columns={
                "id": "company_id",
                "form_name": "name",
                "form_accession_number": "accession_number",
                "form_report_period": "report_period",
                "form_filing_date": "filing_date",
                "form_acceptance_date": "acceptance_date",
                "form_effective_date": "effective_date",
                "form_url": "url"
            }))

        return form_df[[
            "company_id",
            "name",
            "accession_number",
            "report_period",
            "filing_date",
            "acceptance_date",
            "effective_date",
            "url"
        ]]

    def build_investments(
        self, 
        staged_investments_df: pd.DataFrame,
        upserted_forms_df: pd.DataFrame) -> pd.DataFrame:
        """Merges staged Form 13F investments with their
        corresponding form metadata fetched from the database.

        Args:
            staged_investments_df (`pd.DataFrame`): The
                staged Form 13F investments.

            upserted_forms (`pd.DataFrame`): The
                set of associated forms, fetched from
                the database.

        Returns:
            (`pd.DataFrame`): The investments.
        """
        investments_df = (staged_investments_df[[
            "form_accession_number",
            "stock_issuer_name",
            "stock_title_class",
            "stock_cusip",
            "stock_value_x1000",
            "stock_shares_prn_amt",
            "stock_sh_prn",
            "stock_put_call",
            "stock_investment_discretion",
            "stock_manager",
            "stock_voting_auth_sole",
            "stock_voting_auth_shared",
            "stock_voting_auth_none"
        ]]
        .rename(columns={"form_accession_number": "accession_number"}))
                    
        investments_df = (investments_df
            .merge(upserted_forms_df, on="accession_number")
            .drop(["accession_number"], axis=1)
            .rename(columns={
                "id": "form_id",
                "stock_issuer_name": "issuer_name",
                "stock_title_class": "title_class",
                "stock_cusip": "cusip",
                "stock_value_x1000": "value_x1000",
                "stock_shares_prn_amt": "shares_prn_amt",
                "stock_sh_prn": "sh_prn",
                "stock_put_call": "put_call",
                "stock_investment_discretion": "investment_discretion",
                "stock_manager": "manager",
                "stock_voting_auth_sole": "voting_auth_sole",
                "stock_voting_auth_shared": "voting_auth_shared",
                "stock_voting_auth_none": "voting_auth_none"
            }))
        
        return investments_df

    def transform_data(self) -> None:
        """Transforms staged Form 13F investments. Extracts fields
        to produce company and form submission entities to upsert 
        into the `form_13f_company` and `form_13f_submission` tables,
        respectively. Then merges the remaining investment fields
        with stock metadata retrieved from the Open FIGI API and
        upserts the resulting records into the `form_13f_investment`
        table.

        Args:
            `None`

        Returns:
            `None`
        """     
        while True:
            # Retrieve batch of staged investments from database
            staged_investments_df = self._load_dataset(
                name="staged investment", 
                load_func=self._db_client.get_staged_investments,
                limit=5000)
            
            # Stop processing if no staged investments remaining
            if staged_investments_df is None:
                break
            
            # Parse unique companies from staged investments.
            try:
                self._logger.info("Parsing unique companies from "
                                  "staged investments.")
                companies_df = self.build_companies(staged_investments_df)
            except Exception as e:
                raise InternalServerError("Error creating company records. "
                                          f"{e}") from None
            
            # Upsert staged companies into database
            try:
                self._logger.info("Upserting company records to database.")
                company_records = companies_df.to_dict(orient="records")
                upserted_companies_df, _ = \
                    self._db_client.bulk_upsert_companies(company_records)
                self._logger.info(f"{len(upserted_companies_df)} records "
                                  "upserted successfully.")
            except Exception as e:   
                raise InternalServerError("Error upserting companies "
                                          f"to database table. {e}")
            
            # Parse unique forms from staged investments
            try:
                self._logger.info("Parsing unique forms from "
                                  "staged investments.")
                forms_df = self.build_forms(
                    staged_investments_df, 
                    upserted_companies_df
                )
            except Exception as e:
                raise InternalServerError("Error creating form records. "
                                          f"{e}") from None
            
            # Upsert forms to database
            try:
                self._logger.info("Upserting finalized forms to database.")
                form_records = forms_df.to_dict(orient="records")
                upserted_forms_df, _ = \
                    self._db_client.bulk_upsert_forms(form_records)
                self._logger.info(f"{len(upserted_forms_df)} records "
                                  "upserted successfully.")
            except Exception as e:
                raise InternalServerError("Error saving forms "
                                          f"to database table. {e}") from None

            # Subset staged investment data and merge with new form ids
            try:
                self._logger.info("Subsetting investments from staged "
                    "investment records and merging with newly-created "
                    "Form 13F identifiers.")
                investments_df = self.build_investments(
                    staged_investments_df, 
                    upserted_forms_df)
            except Exception as e:
                raise InternalServerError("Error creating investment "
                                          f"data. {e}") from None

            # Retrieve stock metadata from Open FIGI API 
            # according to unique CUSIP fields present in 
            # staged investments
            try:
                self._logger.info("Retrieving stock metadata "
                                  "from the Open FIGI API.")
                cusips = investments_df["cusip"].unique()
                stocks_df = self._stocks_client.fetch_stock_metadata(cusips)
            except Exception as e:
                raise InternalServerError("Error retrieving stock "
                                          f"metadata. {e}") from None

            # Merge investments with retrieved stock metadata
            # and finalize columns to match database table schema.
            try:
                self._logger.info("Merging investments with stock "
                                  "metadata from Open FIGI API.")
                stocks_df["cusip"] = list(cusips)
                investments_df = (investments_df
                    .merge(
                        right=stocks_df, 
                        how="right", 
                        on="cusip", 
                        suffixes=("_l", "_r")
                    )
                    .rename(columns={
                        "marketSector": "market_sector",
                        "securityType": "security_type",
                        "exchCode": "exchange_code"
                    }))

                self._logger.info("Updating investments with "
                                  "final column schema.")
                investments_df = investments_df[[
                    "form_id",
                    "exchange_code",
                    "issuer_name",
                    "cusip",
                    "title_class",
                    "market_sector",
                    "security_type",
                    "ticker",
                    "value_x1000",
                    "shares_prn_amt",
                    "sh_prn",
                    "put_call",
                    "investment_discretion",
                    "manager",
                    "voting_auth_sole",
                    "voting_auth_shared",
                    "voting_auth_none",
                ]]
            except Exception as e:
                raise InternalServerError("Error merging investment data "
                                          f"with stock metadata. {e}")

            # Clean database columns
            try:
                self._logger.info("Cleaning columns to prepare "
                                  "for database upsert.")
                investments_df["manager"] = investments_df["manager"].apply(
                    lambda m: "" if not m else m)
                investments_df = investments_df.replace(
                    to_replace=["\n", "\t", "\r"],
                    value="",
                    regex=True)
                self._logger.info("Removing dupes.")
                prior_length = len(investments_df)
                investments_df = investments_df.drop_duplicates(
                    subset=["form_id", "cusip", "manager"])
                final_length = len(investments_df)
                self._logger.info(f"Dropped {prior_length - final_length} "
                                  "record(s) before upsert.")
            except Exception as e:
                raise InternalServerError("Error cleaning investment "
                                          f"data. {e}") from None

            # Upsert finalized records into database.
            try:
                self._logger.info("Upserting finalized investments "
                                  "into database.")
                investment_records = investments_df.to_dict(orient="records")
                upserted_investments, _ = \
                    self._db_client.bulk_upsert_investments(investment_records)
                self._logger.info(f"{len(upserted_investments)} "
                                  "records upserted successfully.")
            except Exception as e:
                raise InternalServerError("Error upserting "
                                          f"investment data. {e}") from None

            # Delete staged investments
            self._db_client.delete_staged_investments(staged_investments_df)
      
class TransformJobHandler:
    """Handles a transformation job request by updating the
    status of the job in the database and passing the work
    to the correct client (e.g., projects, investments).
    """

    def __init__(self, logger: logging.Logger) -> None:
        """Initializes a new instance of a `TransformJobHandler`.

        Args:
            logger (`logging.Logger`): An instance of a
                standard logger.

        Returns:
            `None`
        """
        self._logger = logger
        self._db_client = DbClient(logger)
        self._stocks_client = StocksClient(logger)
        self._currency_client = CurrencyClient()
        self._name_standardizer = NameStandardizer()

    def handle(self, job_id: int) -> str:
        """Handles a dataset cleaning request.

        Args:
            job_id (`id`): The unique identifier of the
                pipeline job entering the cleaning stage.

        Return:
            (`str`): The job completion/success message.
        """
        try:
            # Mark job start in database
            self._logger.info("Updating job in database to "
                "signal start of data cleaning and merging.")
            job_update = {
                "id": job_id,
                "data_clean_stage": IN_PROGRESS_STATUS,
                "data_clean_start_utc": datetime.utcnow()
            }
            job = self._db_client.update_job(job_update)

            # Parse job type from updated job representation
            job_type = job["job_type"]

            # Fetch transformation client correponding to job type
            if job_type == DEV_BANK_PROJECTS_JOB_TYPE:
                client = ProjectTransformClient(
                    self._currency_client,
                    self._name_standardizer,
                    self._db_client,
                    self._logger
                )
            elif job_type == FORM_13F_JOB_TYPE:
                client = InvestmentTransformClient(
                    self._stocks_client,
                    self._db_client,
                    self._logger
                )
            else:
                raise BadRequest("Failed to complete "
                    "requested cleaning job. Received an "
                    f"unexpected job type of \"{job['job_type']}\".")
            
            # Transform data
            client.transform_data()

            # Mark job as success in database
            try:
                self._logger.info("Marking job as success in database.")
                job_update = {
                    "id": job_id,
                    "data_clean_stage": COMPLETED_STATUS,
                    "data_clean_end_utc": datetime.utcnow()
                }
                self._db_client.update_job(job_update)
            except Exception as e:
                self._logger.error(f"Failed to update")

            # Return job completion message
            return ("Successfully transformed records for job "
                    f"\"{job_id}\" of type \"{job_type}\".")
        
        except Exception as e:
            # Log error
            self._logger.error(f"Data transform failed for job {job_id}. {e}")

            # Record job error in database
            if job_id:
                job_update = {
                    "id": job_id,
                    "data_clean_stage": ERROR_STATUS
                }
                self._db_client.update_job(job_update)

            # Bubble up error
            raise
