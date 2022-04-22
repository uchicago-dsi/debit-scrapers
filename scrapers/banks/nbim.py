'''
nbim.py

A web scraper for Norges Bank Investment Management (NBIM).
Data retrieved by downloading JSON data for each investment
project type and year.
'''
import pandas as pd
import requests
from logging import Logger
from scrapers.abstract.project_download_workflow import ProjectDownloadWorkflow
from scrapers.constants import NBIM_ABBREVIATION
from scrapers.services.data_request import DataRequestClient
from scrapers.services.database import DbClient
from urllib.parse import quote


class NbimDownloadWorkflow(ProjectDownloadWorkflow):
    '''
    Downloads project records directly from NBIM and then cleans
    and saves the data to a database using the `execute` method
    defined in its superclass.
    '''

    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: DbClient,
        logger: Logger) -> None:
        '''
        The public constructor.

         Parameters:
            data_request_client (DataRequestClient): A client
                for making HTTP GET requests while adding
                random delays and rotating user agent headers.

            db_client (DbClient): A client for inserting and
                updating tasks in the database.

            logger (Logger): An instance of the logging class.

        Returns:
            None
        '''
        super().__init__(data_request_client, db_client, logger)


    @property
    def investments_base_url(self) -> str:
        '''
        The base URL for NBIM investments.
        '''
        return 'https://www.nbim.no/en/the-fund/investments#'


    @property
    def download_url(self) -> str:
        '''
        The URL containing all project records.
        '''
        return 'https://www.nbim.no/api/investments/history.json?year={}'


    @property
    def project_start_year(self) -> int:
        '''
        The inclusive start year to use when querying NBIM projects.
        '''
        return 1998


    @property
    def project_end_year(self) -> int:
        '''
        The inclusive end year to use when querying NBIM projects.
        '''
        return 2022


    def get_projects(self) -> pd.DataFrame:
        '''
        Retrieves all development bank projects by downloading
        JSON data from NBIM's website for each year.
        
        Parameters:
            None

        Returns:
            (pd.DataFrame): The raw project records.
        '''
        try:
            projects_df = None

            for year in range(self.project_start_year, self.project_end_year + 1):

                # Query API for projects in given year
                projects_url = self.download_url.format(year)
                response = requests.get(projects_url)
                if not response.ok:
                    raise Exception(f"HTTP GET request for '{projects_url}' failed "
                        f"with status code '{response.status_code}'.")
                
                # Retrieve JSON from HTTP response body if available
                try:
                    data = response.json()
                except Exception:
                    continue

                if not data:
                    continue

                # Extract data from JSON
                equities = []
                fixed_income = []
                real_estate = []
                for continent in data['re']:
                    for country in continent['ct']:
                        if 'eq' in country.keys():
                            equities.extend(country['eq']['cp'])
                        if 'fi' in country.keys():
                            fixed_income.extend(country['fi']['cp'])
                        if 're' in country.keys():
                            real_estate.extend(country['re']['cp'])
                
                # Update projects DataFrame
                equities_df = pd.DataFrame(equities)
                equities_df['type'] = 'equities'

                fixed_income_df = pd.DataFrame(fixed_income)
                fixed_income_df['type'] = 'fixed-income'

                real_estate_df = pd.DataFrame(real_estate)
                real_estate_df['type'] = 'real-estate'

                year_df = pd.concat([equities_df, fixed_income_df, real_estate_df], sort=True)
                year_df['year'] = year

                projects_df = year_df if projects_df is None else pd.concat([projects_df, year_df])

            return projects_df

        except Exception as e:
            raise Exception(f"Error retrieving NBIM investment data. {e}")


    def clean_projects(self, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Cleans NBIM project records to conform to an expected schema.

        Parameters:
            df (pd.DataFrame): The raw project records.

        Returns:
            (pd.DataFrame): The cleaned records.
        '''
        try:
            # Rename existing columns
            df = df.rename(columns={
                's': 'sectors',
                'ic': 'countries',
                'n': 'companies'
            })

            # Construct project URLs
            def create_project_url(row: pd.Series):
                '''
                Constructs a URL for an NBIM project page.

                Parameters:
                    row (pd.Series): A row of data from the DataFrame.

                Returns:
                    (str): The URL.
                '''
                return (
                    f"{self.investments_base_url}/"
                    f"{row['year']}/"
                    "investments/"
                    f"{row['type']}/"
                    f"{int(row['id'])}/"
                    f"{quote(row['companies'])}"
                )
            df['url'] = df.agg(lambda row: create_project_url(row), axis='columns')

            # Define other new columns
            df['bank'] = NBIM_ABBREVIATION.upper()
            df['number'] = df['id'].astype(int)
            df['name'] = None
            df['status'] = None
            df['month'] = None
            df['day'] = None
            df['loan_amount'] = df['h'].str['v']
            df['loan_amount_currency'] = 'NOK'
            df['loan_amount_usd'] = df['h'].str['vu']

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

            # Replace blanks with None
            df = df.replace([''], None, regex=True)

            return df[col_mapping.keys()].astype(col_mapping)
        
        except Exception as e:
            raise Exception(f"Error cleaning NBIM investment data. {e}")



if __name__ == "__main__":
    # Test 'DownloadWorkflow'
    w = NbimDownloadWorkflow(None, None, None)
    raw_df = w.get_projects()
    clean_df = w.clean_projects(raw_df)
    print(f"Found {len(clean_df)} record(s).")
    print(clean_df.head())