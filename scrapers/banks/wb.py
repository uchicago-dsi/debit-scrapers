"""Web scrapers for the World Bank (WB). Currently
downloads project records as an Excel file.
"""

import numpy as np
import pandas as pd
from logging import Logger
from scrapers.abstract.project_download_workflow import ProjectDownloadWorkflow
from scrapers.constants import WB_ABBREVIATION
from scrapers.services.data_request import DataRequestClient
from scrapers.services.database import DbClient


class WbDownloadWorkflow(ProjectDownloadWorkflow):
    """Downloads project records directly from the World Bank's
    website and then cleans and saves the data to a database
    using the `execute` method defined in its superclass.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of a `WbDownloadWorkflow`.

        Args:
           data_request_client (`DataRequestClient`): A client
                for making HTTP GET requests while adding
                random delays and rotating user agent headers.

            db_client (`DbClient`): A client for inserting and
                updating tasks in the database.

            logger (`Logger`): An instance of the logging class.

        Returns:
            None
        """
        super().__init__(data_request_client, db_client, logger)


    @property
    def download_url(self) -> str:
        """The URL containing all project records.
        """
        return "http://search.worldbank.org/api/projects/all.csv"


    def get_projects(self) -> pd.DataFrame:
        """Retrieves all development bank projects by downloading an
        Excel file hosted on the World Bank's website. The request
        may take a few minutes to complete due to the large file size.

        Args:
            None
        
        Returns:
            (`pd.DataFrame`): The raw project records.
        """
        try:
            return pd.read_excel(self.download_url, skiprows=2, engine='xlrd')
            # return pd.read_html(self.download_url, flavor='bs4')
        except Exception as e:
            try:
                return pd.read_csv(self.download_url, skiprows=2)
            except Exception as ee:
                raise Exception(f"Error retrieving Excel project data "
                    f"from the World Bank. {e}")


    def clean_projects(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans World Bank project records to conform to
        an expected schema.

        Args:
            df (`pd.DataFrame`): The raw project records.

        Returns:
            (`pd.DataFrame`): The cleaned records.
        """
        try:
            # Map column names
            col_mapping = {
                'id': 'number',
                'project_name': 'name',
                'projectstatusdisplay': 'status',
                'boardapprovaldate': 'date',
                'grantamt':'loan_amount',
                'countryname': 'countries',
                'impagency':'companies',
                'lendinginstr': 'sectors',
                'url':'url',
            }
            df = df.rename(columns=col_mapping)

            # Standardize fields
            df['bank'] = WB_ABBREVIATION.upper()
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df['year'] = df['date'].dt.year
            df['month'] = df['date'].dt.month
            df['day'] = df['date'].dt.day
            df['loan_amount_currency'] = 'USD'
            df['loan_amount_usd'] = df['loan_amount']

            # Correct country names
            def correct_country_name(name: str) -> str:
                """
                Rearranges a formal country name to remove
                its comma (e.g., "China, People's Republic
                of" becomes "People's Republic of China").
                At the time of writing, only one country
                is listed per project record for ADB, so
                combining different countries into one string
                is not a concern.

                Args:
                    name (str): The country name.

                Returns:
                    (str): The formatted name.
                """
                if not name or name is np.nan:
                    return None

                name_parts = name.split(',')
                uses_formal_name = len(name_parts) == 2
                if uses_formal_name:
                    return f"{name_parts[1].strip()} {name_parts[0]}"
                    
                return name

            df.loc[:, 'countries'] = df['countries'].apply(correct_country_name)

            # Set final column schema
            col_mapping = {
                'bank': 'object',
                'number': 'object',
                'name': 'object',
                'status': 'object',
                'year': 'Int64',
                'month':'Int64',
                'day': 'Int64',
                'loan_amount': 'Float64',
                'loan_amount_currency': 'object',
                'loan_amount_usd': 'Float64',
                'sectors': 'object',
                'countries': 'object',
                'companies': 'object',
                'url': 'object'
            }

            return df[col_mapping.keys()].astype(col_mapping)

        except Exception as e:
            raise Exception(f"Error cleaning World Bank Project data. {e}")


if __name__ == "__main__":
    # Test 'DownloadWorkflow'
    w = WbDownloadWorkflow(None, None, None)
    raw_df = w.get_projects()
    clean_df = w.clean_projects(raw_df)
    print(f"Found {len(clean_df)} record(s).")
    print(clean_df.head())
