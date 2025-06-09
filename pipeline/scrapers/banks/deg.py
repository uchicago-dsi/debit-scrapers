"""Web scrapers for the German Investment Corporation, also
known as Deutsche Investitions- und Entwicklungsgesellschaft
(DEG), a subsdiary of development bank KFW (Kreditanstalt 
für Wiederaufbau). Currently downloads project data as JSON.
"""

import pandas as pd
import requests
from logging import Logger
from pipeline.constants import DEG_ABBREVIATION
from pipeline.scrapers.abstract import ProjectDownloadWorkflow
from pipeline.services.web import DataRequestClient
from pipeline.services.database import DbClient


class DegDownloadWorkflow(ProjectDownloadWorkflow):
    """Downloads project records directly from DEG's website and
    then cleans and saves the data to a database using the
    `execute` method defined in its superclass.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of a `DegDownloadWorkflow`.

        Args:
            data_request_client (`DataRequestClient`): A client
                for making HTTP GET requests while adding
                random delays and rotating user agent headers.

            db_client (`DbClient`): A client for inserting and
                updating tasks in the database.

            logger (`Logger`): An instance of the logging class.

        Returns:
            `None`
        """
        super().__init__(data_request_client, db_client, logger)

    @property
    def download_url(self) -> str:
        """The URL containing all project records.
        """
        return "https://deginvest-investments.de/?tx_deginvests_rest%5Baction%5D=list&tx_deginvests_rest%5Bcontroller%5D=Rest&cHash=f8602c3bfb7e71d9760e1412bc0c8bb5"

    @property
    def project_detail_base_url(self) -> str:
        """The base URL for individual project pages.
        """
        return "https://deginvest-investments.de"

    def get_projects(self) -> pd.DataFrame:
        """Retrieves all development bank projects as JSON from
        DEG's website.

        Args:
            None

        Returns:
            (`pd.DataFrame`): The raw project records.
        """
        try:
            response = requests.get(self.download_url)
            return pd.DataFrame.from_dict(response.json())
        except Exception as e:
            raise Exception(f"Error retrieving or parsing DEG project JSON. {e}")

    def clean_projects(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans DEG project records to conform to an expected schema.

        Args:
            df (`pd.DataFrame`): The raw project records.

        Returns:
            (`pd.DataFrame`): The cleaned records.
        """
        try:
            # Parse date column to UTC
            df['date'] = pd.to_datetime(df['signingDate'], errors='coerce', utc=True)

            # Create year, month, and day columns
            df['year'] = df['date'].dt.year.astype('Int64')
            df['month'] = df['date'].dt.month.astype('Int64')
            df['day'] = df['date'].dt.day.astype('Int64')

            # Define additional columns
            df['bank'] = DEG_ABBREVIATION.upper()
            df['number'] = df['uid']
            df['name'] = None
            df['status'] = None
            df['loan_amount'] = df['financingSum']
            df['loan_amount_currency'] = df['currency'].str['code']
            df['loan_amount_usd'] = None
            df['sectors'] = df['sector'].str['title']
            df['countries'] = df['country'].str['title']
            df['companies'] = df['title']
            df['url'] = self.project_detail_base_url + df['detailUrl']

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
            raise Exception(f"Error cleaning DEG projects. {e}")
