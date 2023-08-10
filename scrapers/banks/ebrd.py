"""A web scraper for the European Bank for Reconstruction
and Development. Data retrieved by first scraping all
individual project page URLs from search result pages
and then scraping details from each project page.
"""

import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from logging import Logger
from scrapers.abstract.project_scrape_workflow import ProjectScrapeWorkflow
from scrapers.abstract.results_scrape_workflow import ResultsScrapeWorkflow
from scrapers.abstract.seed_urls_workflow import SeedUrlsWorkflow
from scrapers.constants import EBRD_ABBREVIATION, RESULTS_PAGE_WORKFLOW
from scrapers.services.data_request import DataRequestClient
from scrapers.services.database import DbClient
from scrapers.services.pubsub import PubSubClient
from typing import Dict, List


class EbrdSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Retrieves the first set of EBRD URLs to scrape.
    """
    
    def __init__(
        self,
        pubsub_client: PubSubClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of an `EbrdSeedUrlsWorkflow`.

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
        return 1


    @property
    def next_workflow(self) -> str:
        """The name of the workflow to execute after this
        workflow has finished.
        """
        return RESULTS_PAGE_WORKFLOW

    
    @property
    def search_results_base_url(self) -> str:
        """The base URL for a development bank project search
        results page on EBRD's website.
        """
        return 'https://www.ebrd.com/cs/Satellite?c=Page&cid=1395238314964&d=&pagename=EBRD/Page/SolrSearchAndFilterPSD&page={}&safSortBy=PublicationDate_sort&safSortOrder=descending'
    

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
                self.search_results_base_url.format(n)
                for n in range(self.first_page_num, last_page_num + 1)
            ]
            return result_page_urls
        except Exception as e:
            raise Exception(f"Failed to generate search result pages to crawl. {e}")


    def find_last_page(self) -> int:
        """Retrieves the number of the last page of development
        bank projects on the website.
        
        Args:
            None
        
        Returns:
            (int): The page number.
        """
        try:
            first_results_page_url = self.search_results_base_url.format(self.first_page_num)
            html = requests.get(first_results_page_url).text
            soup = BeautifulSoup(html, "html.parser")
            max_page_input = soup.find("input", {"id": "maxPage"})
            return int(max_page_input['value'])

        except Exception as e:
            raise Exception("Error retrieving last page number at "
                f"'{first_results_page_url}'. {e}")


class EbrdResultsScrapeWorkflow(ResultsScrapeWorkflow):
    """Scrapes an EBRD search results page for development bank project URLs.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        pubsub_client: PubSubClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of an `EbrdResultsScrapeWorkflow`.

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
        return 'https://www.ebrd.com'

    
    def scrape_results_page(self, url: str) -> List[str]:
        """Scrapes all development project page URLs from a given
        search results page on EBRD's website.

        Args:
            results_page_url (str): The URL to a search results page
                containing lists of development projects.

        Returns:
            (list of str): The list of scraped project page URLs.
        """
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.text, features='html')

            project_urls = []
            for project in soup.find_all("tr", {"class":"post"}):
                url = project.find('a')['href']
                if url.startswith(self.project_page_base_url):
                    project_urls.append(url)

            return project_urls

        except Exception as e:
            raise Exception(f"Error scraping EBRD project page URLs from '{url}'. {e}")


class EbrdProjectScrapeWorkflow(ProjectScrapeWorkflow):
    """Scrapes an EBRD project page for development bank project data.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of a `EbrdProjectScrapeWorkflow`.

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
        """Scrapes an EBRD project page for data.

        Args:
            url (str): The URL for a project.

        Returns:
            None
        """
        # Retrieve HTML
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract data from page
        def get_field(detail_name: str):
            """Extracts a field value corresponding to a label tag.
            """
            try:
                label = soup.find(string=detail_name)
                legend = label.findParent()
                paragraph = legend.find_next_sibling("p")
                return paragraph.text
            except AttributeError:
                return None

        number = get_field("Project number:")
        name = soup.find("h1").text.strip()
        status = get_field("Status:")
        date = get_field("PSD disclosed:")
        loan_amount = get_field(re.compile(r"EBRD Finance Summary(.*)"))
        sectors = get_field("Business sector:")
        countries = get_field("Location:")
        companies = get_field("Client Information")

        # Parse date field to retrieve year, month, and day
        if date:
            parsed_date = datetime.strptime(date, "%d %b %Y")
            year = parsed_date.year
            month = parsed_date.month
            day = parsed_date.day
        else:
            year = month = day = None

        # Strip extra characters from company field
        if companies:
            companies = re.sub('[\r\n\t]', '', companies)

        # Parse loan amount field to retrieve value and currency type
        if loan_amount and '\xa0' in loan_amount:
            loan_amount = loan_amount.strip("\r\n\t")
            loan_amount_currency, loan_amount_value = loan_amount.split('\xa0')
            loan_amount_value = int(float(loan_amount_value.replace(',', '')))
        else:
            loan_amount_currency = loan_amount_value = None

        # Compose final project record schema
        return [{
            "bank": EBRD_ABBREVIATION.upper(),
            "number": number,
            "name": name,
            "status": status,
            "year": year,
            "month": month,
            "day": day,
            "loan_amount": loan_amount_value,
            "loan_amount_currency": loan_amount_currency,
            "loan_amount_in_usd": None,
            "sectors": sectors,
            "countries": countries,
            "companies": companies,
            "url": url
        }]
