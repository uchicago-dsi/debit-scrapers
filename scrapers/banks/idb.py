"""Web scrapers for the Inter-American Development Bank (IDB).
"""

import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from logging import Logger
from scrapers.abstract.project_scrape_workflow import ProjectScrapeWorkflow
from scrapers.abstract.results_scrape_workflow import ResultsScrapeWorkflow
from scrapers.abstract.seed_urls_workflow import SeedUrlsWorkflow
from scrapers.constants import IDB_ABBREVIATION, RESULTS_PAGE_WORKFLOW
from scrapers.services.data_request import DataRequestClient
from scrapers.services.database import DbClient
from scrapers.services.pubsub import PubSubClient
from typing import Dict, List
import time, random

USER_AGENT_HEADER = headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'
}

class IdbSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Retrieves the first set of IBD URLs to scrape.
    """

    def __init__(
        self,
        pubsub_client: PubSubClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of a `IdbSeedUrlsWorkflow`.

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
        return RESULTS_PAGE_WORKFLOW


    @property
    def search_results_base_url(self) -> str:
        """The base URL for a development bank project search
        results page on IDB's website. Should be formatted
        with the page number.
        """
        return 'https://www.iadb.org/en/projects-search?country=&sector=&status=&query=&page={page_num}'
    

    def generate_seed_urls(self) -> List[str]:
        """Generates the first set of URLs to scrape.

        Args:
            None

        Returns:
            (list of str): The unique list of search result pages.
        """
        print("Generating seed URL's for IDB")
        try:
            last_page_num = self.find_last_page()
            result_page_urls = [
                self.search_results_base_url.format(page_num=str(n))
                for n in range(self.first_page_num, last_page_num + 1)
            ]
            print(f"Resulting pages : {result_page_urls}")
            return result_page_urls
        except Exception as e:
            raise Exception("Failed to generate search "
                            f"result pages to crawl. {e}")
    

    def find_last_page(self) -> int:
        """Retrieves the number of the last page of development
        bank projects on the website.
        
        Args:
            None
        
        Returns:
            (int): The page number.
        """
        print("Finding last page for IDB")
        try:
            first_results_page_url = self.search_results_base_url.format(
                page_num=self.first_page_num)
            print(f"URL for first IDB page : {first_results_page_url}")
            html = requests.get(first_results_page_url, headers = USER_AGENT_HEADER).text
            print(f"html for first page of results from IDB : {html}")
            soup = BeautifulSoup(html, "html.parser")
            last_page_item = soup.find('li', {"class":"pager__item--last"})
            print(f"Text from last pager item for IDB : {last_page_item.text}")
            last_page_url = last_page_item.find('idb-button')['button-url']
            last_page_number = int(last_page_url.split('=')[-1])
            return last_page_number
        except Exception as e:
            raise Exception(f"Error retrieving last page number. {e}")


class IdbResultsScrapeWorkflow(ResultsScrapeWorkflow):
    """Scrapes an IDB search results page for development bank project URLs.
    """
    
    def __init__(
        self,
        data_request_client: DataRequestClient,
        pubsub_client: PubSubClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of an `IdbResultsScrapeWorkflow`.

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
    def project_page_base_url(self) -> str:
        """The base URL for individual EBRD project pages.
        """
        return 'https://www.iadb.org'


    def scrape_results_page(self, results_page_url: str) -> List[str]:
        """Scrapes all development project page URLs from a given
        search results page on IDB's website.

        Args:
            results_page_url (str): The URL to a search results
                page containing lists of development projects.

        Returns:
            (list of str): The list of scraped project page URLs.
        """
        try:
            html = requests.get(results_page_url).text
            soup = BeautifulSoup(html, "html.parser")
            urls = []
            for project in soup.find_all('tr', {'class':['odd','even']}):
                project_link = project.find('a')['href']
                urls.append(self.project_page_base_url + project_link)
            return urls
        except Exception as e:
            raise Exception(f"Error scraping search page '{results_page_url}' for " + 
                f"project page URLs. {e}")


class IdbProjectScrapeWorkflow(ProjectScrapeWorkflow):
    """Scrapes an IDB project page for development bank project data.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of an `IdbProjectScrapeWorkflow`.

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


    def scrape_project_page(self, url: str) -> List[Dict]:
        """Scrapes an IDB project page for data.

        Args:
            url (str): The URL for a project.

        Returns:
            (list of dict): The project records.
        """
        try:
            # Request and parse page into BeautifulSoup object
            time.sleep(.2) # site breaks if requests come too fast
            response = self._data_request_client.get(
                url, 
                use_random_user_agent=True, 
                # custom_headers=USER_AGENT_HEADER
            )
            soup = BeautifulSoup(response.text, 'html.parser')
            # Abort process if no project data available
            project_title = soup.find("idb-section-wrapper")["heading"]
            if not project_title or project_title.strip() == ":":
                return
            # Parse project detail and information sections
            project_info_table = soup.find('idb-project-table')
            table_info = {}
            for row in project_info_table.findAll('idb-project-table-row'):
                fact, value, *_ = [p.text for p in row.findAll('p')]
                table_info[fact] = value

            # Define local function for extracting data from a project section

            # Retrieve fields
            number = table_info['Project Number']
            name = project_title.split(":")[1].strip() if ":" in project_title else project_title
            status = table_info['Project Status']
            date = table_info['Approval Date']
            loan_amount = table_info['Total Cost']
            sectors = table_info['Sector']
            subsectors = table_info['Subsector']
            countries = table_info['Country']

            # Parse project approval date to retrieve year, month, and day
            if date:
                parsed_date = datetime.strptime(date, "%B %d, %Y")
                year = parsed_date.year
                month = parsed_date.month
                day = parsed_date.day
            else:
                year = month = day = None

            # Parse loan amount field to rerieve value and currency type
            if loan_amount:
                loan_amount_currency, loan_amount_value = loan_amount.split()
                loan_amount_value = float(loan_amount_value.replace(',', ''))
            else:
                loan_amount_currency = loan_amount_value = None

            # Compose final project record schema
            return [{
                "bank": IDB_ABBREVIATION.upper(),
                "number": number,
                "name": name,
                "status": status,
                "year": year,
                "month": month,
                "day": day,
                "loan_amount": loan_amount_value,
                "loan_amount_currency": loan_amount_currency,
                "loan_amount_in_usd": None,
                "sectors": subsectors if subsectors else sectors,
                "countries": countries,
                "companies": None,
                "url": url
            }]
        except Exception as e:
            raise Exception(f"Failed to parse project page '{url}' for data. {e}")



if __name__ == "__main__":
    import json
    import yaml
    from scrapers.constants import CONFIG_DIR_PATH
    
    # Set up DataRequestClient to rotate HTTP headers and add random delays
    with open(f"{CONFIG_DIR_PATH}/user_agent_headers.json", "r") as stream:
        try:
            user_agent_headers = json.load(stream)
            data_request_client = DataRequestClient(user_agent_headers)
        except yaml.YAMLError as e:
            raise Exception(f"Failed to open configuration file. {e}")

    # Test 'SeedUrlsWorkflow'
    w = IdbSeedUrlsWorkflow(None, None, None)
    print(w.generate_seed_urls())

    # Test 'ResultsScrapeWorkflow'
    w = IdbResultsScrapeWorkflow(data_request_client, None, None, None)
    url = 'https://www.iadb.org/en/projects-search?country=&sector=&status=&query=&page=120'
    print(w.scrape_results_page(url))

    # Test 'ProjectScrapeWorkflow'
    w = IdbProjectScrapeWorkflow(data_request_client, None, None)
    url = 'https://www.iadb.org/en/project/TC9409295'
    print(w.scrape_project_page(url))
