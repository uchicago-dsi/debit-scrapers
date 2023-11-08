"""Web scrapers for the International Finance
Corporation (IFC). Data currently retrieved by 
querying an external API for lists of project records.
"""

import json
import numpy as np
import pandas as pd
import re
import requests
from io import BytesIO
from logging import Logger
from scrapers.abstract.seed_urls_workflow import SeedUrlsWorkflow
from scrapers.abstract.project_scrape_workflow import ProjectScrapeWorkflow
from scrapers.constants import IFC_ABBREVIATION, PROJECT_PAGE_WORKFLOW
from scrapers.services.data_request import DataRequestClient
from scrapers.services.database import DbClient
from scrapers.services.pubsub import PubSubClient
from typing import Dict, List


class IfcSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Retrieves the first set of IFC URLs to download.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        pubsub_client: PubSubClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of an `IfcSeedUrlsWorkflow`.

        Args:
            data_request_client (`DataRequestClient`): A client
                for making HTTP GET requests while adding
                random delays and rotating user agent headers.

            pubsub_client (`PubSubClient`): A wrapper client for the 
                Google Cloud Platform Pub/Sub API. Configured to
                publish messages to the appropriate 'tasks' topic.

            db_client (`DbClient`): A client used to insert and
                update tasks in the database.

            logger (`Logger`): An instance of the logging class.

        Returns:
            None
        """
        super().__init__(data_request_client, pubsub_client, db_client, logger)


    @property
    def first_project_index(self) -> int:
        """The starting index to use when downloading
        project records.
        """
        return 0


    @property
    def next_workflow(self) -> str:
        """The name of the workflow to execute after this
        workflow has finished.
        """
        return PROJECT_PAGE_WORKFLOW


    @property
    def num_projects_per_download(self) -> int:
        """The number of projects to download from the API at once.
        """
        return 1000


    @property
    def project_download_base_url(self) -> str:
        """The base URL for a development bank project search
        results page on the IFC's website. Should be formatted
        with an offset value and number of rows.
        """
        return 'https://externalsearch.ifc.org/spi/api/searchxls?qterm=*&start={}&srt=disclosed_date&order=desc&rows={}'

    
    @property
    def search_results_base_url(self) -> str:
        """The base URL for a development bank project search
        results page on the IFC's website.
        """
        return 'https://disclosuresservice.ifc.org/api/searchprovider/searchenterpriseprojects'


    def generate_seed_urls(self) -> List[str]:
        """Generates the URLs used for downloading IFC project data.

        Args:
            None

        Returns:
            (list of str): The download URLs.
        """
        try:
            # Determine number of downloads necessary to retrieve
            # all development project data
            num_projects = self.get_num_projects()
            num_download_batches = (
                (num_projects // self.num_projects_per_download) +
                (1 if num_projects % self.num_projects_per_download > 0 else 0)
            )

            # Generate download URLs, specifying the number of
            # projects that can be obtained from IFC at once
            start = self.first_project_index
            end = num_download_batches * self.num_projects_per_download
            increment = nrows = self.num_projects_per_download

            download_urls = []
            for offset in range(start, end, increment):
                url = self.project_download_base_url.format(offset, nrows)
                download_urls.append(url)

            return download_urls

        except Exception as e:
            raise Exception(f"Failed to generate IFC project download URLs. {e}")


    def get_num_projects(self) -> int:
        """Retrieves the total number of development bank
        projects available on the site, as given by the
        search results page.

        Args:
            None

        Returns:
            (int): The search result count.
        """
        try:
            # Make IFC search results page request
            request_headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.106 Safari/537.36'}
            request_body = { "projectNumberSearch" : "*&$srt=disclosed_date$order=desc" }
            response = requests.post(
                url=self.search_results_base_url,
                data=request_body,
                headers=request_headers
            )

            # Parse JSON response to retrieve total number of projects
            payload = response.json()
            results_metadata = payload['SearchResult']['data']['results']['header']
            num_results = int(results_metadata['listInfo']['totalRows'])
            return num_results

        except Exception as e:
            raise Exception("Error retrieving number of IFC projects from "
                f"search results page '{self.search_results_base_url}'. {e}")


class IfcProjectScrapeWorkflow(ProjectScrapeWorkflow):
    """Queries project records from IFC's API and then
    cleans and saves the data to a database using 
    the `execute` method defined in its superclass.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of an `IfcProjectScrapeWorkflow`.

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
    def project_detail_base_url(self) -> str:
        """The base URL for individual project pages.
        """
        return 'https://disclosures.ifc.org/project-detail'
    
    
    def scrape_project_page(self, url) -> List[Dict]:
        """Queries an IFC endpoint for development project
        records in Excel/CSV format and reads those
        records into a Pandas DataFrame. Then maps the
        records to the expected output format.

        Args:
            url (`str`): The project download URL.

        Returns:
            (pd.DataFrame): The project records.
        """
        try:
            # Fetch project records and read into DataFrame
            project_records = requests.get(url, timeout=None)
            file_stream = BytesIO(project_records.content)
            df = pd.read_csv(file_stream, encoding='iso-8859-1')
        except Exception as e:
            raise Exception("Error retrieving IFC project records from "
                f"'{url}' and reading into Pandas DataFrame. {e}")

        try:
            # Map column names
            col_mapping = {
                "Project Number": "number",
                "Project Name": "name",
                "Status Description": "status",
                "Investment": "loan_amount",
                "Sector": "sectors",
                "Country Description": "countries",
                "Company Name": "companies"
            }
            df = df.rename(columns=col_mapping)

            # Extract loan amount value and currency type
            df['loan_amount_currency'] = df['loan_amount'].str.extract('\((.*?)\)')
            df['loan_amount'] = df['loan_amount'].str.extract('(.*?) million')
            df['loan_amount'] = pd.to_numeric(df['loan_amount'], errors='coerce')
            df['loan_amount'] = df['loan_amount'] * 10**6
            df['loan_amount_usd'] = None

            # Create additional fields
            df['bank'] = IFC_ABBREVIATION.upper()
            df['date'] = pd.to_datetime(df['Disclosed Date'], errors='coerce')
            df['year'] = df['date'].dt.year
            df['month'] = df['date'].dt.month
            df['day'] = df['date'].dt.day

            # Map document types as first step towards generating project URLs
            doc_type_mapping = {
                'Advisory Services': 'AS',
                'Environmental Review Summary': 'ERS',
                'Summary of Investment Information (AIP Policy 2012)': 'SII', 
                'Summary of Proposed Investment (Disclosure Policy 2006)': 'SPI',
                'Environmental Documents': 'ESRS',
                'Early Disclosure': 'ED',
                'Summary of InfraVentures Project': 'SIVP'
            }
            df['doc_type'] = np.where(
                df['Type Description'] =='Advisory Services',
                'Advisory Services',
                df['Document Type Description']
            )
            df['doc_type'] = df['doc_type'].replace(doc_type_mapping)

            # Build project URLs using project name, number, and document type
            def generate_project_detail_url(row: pd.Series):
                """Builds a complete URL to an IFC project detail page.

                Args:
                    row (pd.Series): The DataFrame row.

                Returns:
                    (`str`): The URL.
                """
                # Compose URL fragment containing project name
                regex = '[()\"#/@;:<>{}`+=~|.!?,]'
                substitute = row['name'].lower().replace(' ', '-').replace('---', '-')
                proj_name_url_frag = re.sub(regex, '', substitute)

                # Parse other needed fields into str types
                doc_type = str(row['doc_type'])
                proj_num = str(row['number'])

                return f"{self.project_detail_base_url}/{doc_type}/{proj_num}/{proj_name_url_frag}"

            df['url'] = df.apply(generate_project_detail_url, axis='columns')

           # Set final column schema
            cols_to_keep = [
                'bank',
                'number',
                'name',
                'status',
                'year',
                'month',
                'day',
                'loan_amount',
                'loan_amount_currency',
                'loan_amount_usd',
                'sectors',
                'countries',
                'companies',
                'url'
            ]
            df = df[cols_to_keep]

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
                    name (`str`): The country name.

                Returns:
                    (`str`): The formatted name.
                """
                if not name or name is np.nan:
                    return None
                    
                name_parts = name.split(',')
                uses_formal_name = len(name_parts) == 2
                if uses_formal_name:
                    return f"{name_parts[1].strip()} {name_parts[0]}"
                    
                return name

            df.loc[:, 'countries'] = df['countries'].apply(correct_country_name)
            
            # Drop NaN values
            records = json.loads(df.to_json(orient="records"))

            return records

        except Exception as e:
            raise Exception(f"Error mapping IFC project records "
                f"to expected output schema. {e}")
