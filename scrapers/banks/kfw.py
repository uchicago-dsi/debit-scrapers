"""Web scrapers for the development bank 
KFW (Kreditanstalt für Wiederaufbau). Data
currently retrieved by downloading project JSON.
"""

import pandas as pd
from logging import Logger
from scrapers.abstract.project_download_workflow import ProjectDownloadWorkflow
from scrapers.constants import KFW_ABBREVIATION
from scrapers.services.data_request import DataRequestClient
from scrapers.services.database import DbClient


class KfwDownloadWorkflow(ProjectDownloadWorkflow):
    """Downloads project records directly from KFW's website
    and then cleans and saves the data to a database using
    the `execute` method defined in its superclass.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """
        Initializes a new instance of a `KfwDownloadWorkflow`.

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
        return 'https://www.kfw-entwicklungsbank.de/ipfz/Projektdatenbank/download/json'


    @property
    def projects_base_url(self) -> str:
        """The base URL for individual KFW project pages.
        """
        return 'https://www.kfw-entwicklungsbank.de/ipfz/Projektdatenbank'


    def get_projects(self) -> pd.DataFrame:
        """Retrieves all development bank projects as JSON from KFW's website.

        Args:
            None
        
        Returns:
            (pd.DataFrame): The raw project records.
        """
        try:
            return pd.read_json(self.download_url)
        except Exception as e:
            raise Exception(f"Error retrieving JSON project data from KFW. {e}")


    def clean_projects(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans KFW project records to conform to an expected schema.

        Args:
            df (`pd.DataFrame`): The raw project records.

        Returns:
            (`pd.DataFrame`): The cleaned records.
        """
        try:
            # Rename existing columns
            col_mapping = {
                "projnr": "number",
                "title": "name",
                "status": "status",
                "amount": "loan_amount",
                "focus": "sectors",
                "country": "countries",
                "responsible": "companies"
            }
            df = df.rename(columns=col_mapping)

            # Construct project URLs
            def create_project_url(row: pd.Series):
                """
                Constructs a URL for a KFW project page.

                Args:
                    row (pd.Series): A row of data from the DataFrame.

                Returns:
                    (`str`): The URL.
                """
                return (
                    f"{self.projects_base_url}/"
                    f"{row['name'].replace(' ', '-')}-"
                    f"{row['number']}.htm"
                )
            df['url'] = df.agg(lambda row: create_project_url(row), axis='columns')

            # Create new date column
            df['date'] = pd.to_datetime(df['hostDate'], errors='coerce')

            # Define other new columns
            df['bank'] = KFW_ABBREVIATION.upper()
            df['year'] = df['date'].dt.year
            df['month'] = df['date'].dt.month
            df['day'] = df['date'].dt.day
            df['loan_amount'] = df['loan_amount'] * 10**6
            df['loan_amount_currency'] = 'EUR'
            df['loan_amount_usd'] = None

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
            raise Exception(f"Error cleaning KFW project data. {e}")
