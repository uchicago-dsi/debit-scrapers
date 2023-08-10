"""Web scrapers for the European Investment Bank.
"""

import numpy as np
import requests
from datetime import datetime
from logging import Logger
from scrapers.abstract.project_scrape_workflow import ProjectScrapeWorkflow
from scrapers.abstract.seed_urls_workflow import SeedUrlsWorkflow
from scrapers.constants import EIB_ABBREVIATION, PROJECT_PAGE_WORKFLOW
from scrapers.services.data_request import DataRequestClient
from scrapers.services.database import DbClient
from scrapers.services.pubsub import PubSubClient
from typing import Dict, List


class EibSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Retrieves the first set of EIB URLs to scrape.
    """

    def __init__(
        self,
        pubsub_client: PubSubClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of an `EibSeedUrlsWorkflow`.

        Args:
            pubsub_client (`PubSubClient`): A wrapper client for the 
                Google Cloud Platform Pub/Sub API. Configured to
                publish messages to the appropriate 'tasks' topic.

            db_client (`DbClient`): A client used to insert and
                update tasks in the database.

            logger (`Logger`): An instance of the logging class.

        Returns:
            None
        """
        super().__init__(pubsub_client, db_client, logger)
 

    @property
    def first_page_num(self) -> str:
        """The starting page number for development project search results.
        """
        return 0


    @property
    def next_workflow(self) -> str:
        """The name of the workflow to execute after this
        workflow has finished.
        """
        return PROJECT_PAGE_WORKFLOW


    @property
    def num_results_per_page(self) -> int:
        """The number of search result items to return per page. 
        """
        return 500


    @property
    def search_results_base_url(self) -> str:
        """The base URL for a development bank project search
        results page provided by EIB's API.
        """
        return 'https://www.eib.org/page-provider/projects/list?pageNumber={page_num}&itemPerPage={items_per_page}&pageable=true&sortColumn=id'


    def generate_seed_urls(self) -> List[str]:
        """Generates the first set of URLs to scrape.

        Args:
            None

        Returns:
            (list of str): The unique list of search result pages.
        """
        try:
            last_page_num = self.find_last_page()
            result_page_urls = [
                self.search_results_base_url.format(
                    page_num=n, 
                    items_per_page=self.num_results_per_page
                )
                for n in range(self.first_page_num, last_page_num + 1)
            ]
            return result_page_urls
        except Exception as e:
            raise Exception(f"Failed to generate search result pages to crawl. {e}")


    def find_last_page(self) -> int:
        """Retrieves the number of the last page of
        development bank projects from the API.
        
        Args:
            None
        
        Returns:
            (int): The page number.
        """
        try:
            first_results_page_url = self.search_results_base_url.format(
                page_num=self.first_page_num,
                items_per_page=self.num_results_per_page
            )
            response = requests.get(first_results_page_url)
            data = response.json()
            total_num_items = int(data['totalItems'])
            return (
                total_num_items // self.num_results_per_page +
                1 if total_num_items % self.num_results_per_page > 0 else 0
            )
        except Exception as e:
            raise Exception("Error determining last page number from API "
                f"payload retrieved from '{first_results_page_url}'. {e}")


class EibProjectScrapeWorkflow(ProjectScrapeWorkflow):
    """Retrieves and parses a list of EIB project records for data.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of an `EibProjectScrapeWorkflow`.

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
    def loan_project_base_url(self) -> str:
        """The base API URL for an EIB financed project.
        """
        return 'https://www.eib.org/en/projects/loans/all/{project_id}'


    @property
    def pipeline_project_base_url(self) -> str:
        """The base API URL for an EIB pipeline project
        (i.e., one to be financed).
        """
        return 'https://www.eib.org/en/pipelines/loans/all/{project_id}'


    def scrape_project_page(self, url: str) -> List[Dict]:
        """Queries EIB's API for a page of development bank
        project record(s) and then maps the fields of the
        resulting payload to the expected schema.

        Args:
            url (str): The URL for the results page.

        Returns:
            (list of dict): The raw record(s).
        """
        try:
            response = requests.get(url)
            projects = response.json()
            return [self.map_project_record(p) for p in projects['data']]
        except Exception as e:
            raise Exception(f"Failed to parse EIB projects from '{url}'. {e}")


    def map_project_record(self, project: Dict) -> Dict:
        """Maps an EIB project record to an expected schema.

        Args:
            project (dict): The project record retrieved from the API.

        Returns:
            (dict): The mapped project record.
        """
        # Create local function to correct country names
        def correct_country_name(name: str) -> str:
            """Rearranges a formal country name to remove
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

        # Extract and format project countries and sectors
        countries = []
        sectors = []
        for tag in project['primaryTags']:
            if tag['subType'] == 'countries':
                corrected_name = correct_country_name(tag['label'])
                countries.append(corrected_name)
            if tag['subType'] == 'sectors':
                sectors.append(tag['label'])

        # Extract project status and loan amount data from additional information section
        status, status_date, proposed_amt, financed_amt = project['additionalInformation']

        # Determine first date associated with project status
        parsed_date = datetime.strptime(status_date, "%d/%m/%Y")

        # From status, derive project type (loan or pipeline)
        is_pipeline = status in ('Approved', 'Under appraisal')
   
        # Determine loan amount
        proposed_amt = float(proposed_amt) if proposed_amt else None
        financed_amt = float(financed_amt) if financed_amt else None
        loan_amount_value = proposed_amt if is_pipeline else financed_amt

        # Determine project url
        base_url = self.pipeline_project_base_url if is_pipeline else self.loan_project_base_url
        url = base_url.format(project_id=project['url'])
        
        return {
            "bank": EIB_ABBREVIATION.upper(),
            "number": project['id'],
            "name": project['title'],
            "status": status,
            "year": parsed_date.year,
            "month": parsed_date.month,
            "day": parsed_date.day,
            "loan_amount": loan_amount_value,
            "loan_amount_currency": 'EUR',
            "loan_amount_in_usd": None,
            "sectors": ', '.join(sectors),
            "countries": ', '.join(countries) if countries else None,
            "companies": None,
            "url": url
        }
