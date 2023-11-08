"""Web scrapers for the Belgian Investment Company
for Developing Countries (BIO).
"""

import re
from bs4 import BeautifulSoup
from datetime import datetime
from logging import Logger
from scrapers.abstract.project_partial_scrape_workflow import ProjectPartialScrapeWorkflow
from scrapers.abstract.results_multiscrape_workflow import ResultsMultiScrapeWorkflow
from scrapers.abstract.seed_urls_workflow import SeedUrlsWorkflow
from scrapers.constants import (
    BIO_ABBREVIATION,
    RESULTS_PAGE_MULTISCRAPE_WORKFLOW
)
from scrapers.services.data_request import DataRequestClient
from scrapers.services.database import DbClient
from scrapers.services.pubsub import PubSubClient
from typing import Dict, List, Tuple


class BioSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Retrieves the first set of BIO URLs to scrape.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        pubsub_client: PubSubClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of a `BioSeedUrlsWorkflow`.

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
    def next_workflow(self) -> str:
        """The name of the workflow to execute after this
        workflow has finished.
        """
        return RESULTS_PAGE_MULTISCRAPE_WORKFLOW

    
    @property
    def first_page_num(self) -> int:
        """The starting number for project search result pages.
        """
        return 1


    @property
    def num_projects_per_page(self) -> int:
        """The number of projects displayed on each search results page.
        """
        return 9


    @property
    def search_results_base_url(self) -> str:
        """The base URL for a development bank project search
        results page on BIO's website. Should be formatted
        with the page number.
        """
        return 'https://www.bio-invest.be/en/investments/p{}?search='


    def generate_seed_urls(self) -> List[str]:
        """Generates the first set of URLs to scrape.

        Args:
            None

        Returns:
            (list of str): The unique list of search result pages.
        """
        try:
            last_page_num = self.find_last_page()
            result_pages = [
                self.search_results_base_url.format(n) 
                for n in range(self.first_page_num, last_page_num + 1)
            ]
            return result_pages
        except Exception as e:
            raise Exception(f"Failed to generate BIO search result pages to crawl. {e}")

    
    def find_last_page(self) -> int:
        """Retrieves the number of the last page of development
        bank projects on the website.
        
        Args:
            None
        
        Returns:
            (int): The page number.
        """
        try:
            first_results_page = self.search_results_base_url.format(self.first_page_num)
            html = self._data_request_client.get(first_results_page).text
            soup = BeautifulSoup(html, "html.parser")

            results_div = soup.find("div", {"class" : "js-filter-results"})
            num_results_text = results_div.find("small").text
            num_results = int(num_results_text.split(' ')[0])

            last_page_num = (num_results // self.num_projects_per_page) + \
                (1 if num_results % self.num_projects_per_page > 0 else 0)

            return last_page_num

        except Exception as e:
            raise Exception("Error retrieving last page number at "
                f"'{first_results_page}'. {e}")


class BioResultsMultiScrapeWorkflow(ResultsMultiScrapeWorkflow):
    """Scrapes a BIO search results page for both
    development bank project URLs and project data.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        pubsub_client: PubSubClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """
        Initializes a new instance of a `BioResultsMultiScrapeWorkflow`.

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


    def scrape_results_page(self, url: str) -> Tuple[List[str], List[Dict]]:
        """Scrapes development project data and project page URLs
        from a given search results page on BIO's website.

        Args:
            results_page_url (`str`): The URL to a search results page
                containing lists of development projects.

        Returns:
            ((list of str, list of dict)): A tuple consisting of the list of
                scraped project page URLs and list of project records.
        """
        # Retrieve search results page
        response = self._data_request_client.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Scrape page for both project data and project page URLs
        project_page_urls = []
        projects = []
        for div in soup.find_all('div', {'class':'card'}):

            # Extract project name and URL
            card_header = div.find('h3', {'class': 'card__title'})
            name = card_header.text.strip()
            url = card_header.findChild('a')['href']

            # Extract project date
            try:
                date = div.find(class_="icon--calendar").findParent().text.strip()
                parsed_date = datetime.strptime(date, "%d/%m/%Y")
                year =  parsed_date.year
                month = parsed_date.month
                day = parsed_date.day
            except AttributeError:
                year = month = day = None

            # Extract project countries
            try:
                country_div =  div.find(class_="icon--location").findParent()
                country_arr = [c.strip() for c in country_div.text.split(',')]
                countries = ', '.join(country_arr)
            except AttributeError:
                countries = None
       
            # Extract loan amount (EUR)
            try:
                loan_amount_str = div.find(class_="icon--euro").findParent().text.strip()
                loan_amount_match = re.search(r"([\d,\.]+)", loan_amount_str).groups(0)[0]
                loan_amount_value = float(loan_amount_match.replace(',', ''))
                loan_amount_currency = 'EUR'
            except AttributeError:
                loan_amount_value = loan_amount_currency = None
            
            # Append results
            project_page_urls.append(url)
            projects.append({
                "bank": BIO_ABBREVIATION.upper(),
                "number": None,
                "name": name,
                "status": None,
                "year": year,
                "month": month,
                "day": day,
                "loan_amount": loan_amount_value,
                "loan_amount_currency": loan_amount_currency,
                "loan_amount_in_usd": None,
                "sectors": None,
                "countries": countries,
                "companies": None,
                "url": url
            })

        return project_page_urls, projects


class BioProjectPartialScrapeWorkflow(ProjectPartialScrapeWorkflow):
    """Scrapes a BIO project page for development bank project data.
    """
    
    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """
        Initializes a new instance of a `BioProjectPartialScrapeWorkflow`.

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

 
    def scrape_project_page(self, url) -> List[Dict]:
        """Scrapes a BIO project page for data.

        Args:
            url (`str`): The URL for a project.

        Returns:
            (`list` of `dict`): The project records.
        """
        # Retrieve HTML
        response = self._data_request_client.get(url)
        soup = BeautifulSoup(response.text, "html.parser")

        # Retrieve project companies
        try:
            company_div = soup.find(string="Organisation").parent
            companies = company_div.find_next_sibling("p").text
        except AttributeError:
            companies = None

        # Retrieve investment field type
        try:
            inv_field_div = soup.find(string="Investment field").parent
            inv_field = inv_field_div.find_next_sibling("p").text
        except AttributeError:
            inv_field = None            

        # Retrieve investment activity type
        try:
            inv_activity_div = soup.find(string="Activity").parent
            inv_activity = inv_activity_div.find_next_sibling("div").find("p").text
        except AttributeError:
            inv_activity = None

        # Derive project sector type
        if inv_field.lower() in ("investment companies & funds", "financial institutions"):
            sectors = "Finance"
        else:
            sectors = f"{inv_field}: {inv_activity}"

        # Compose partial project record schema
        return [{
            "bank": BIO_ABBREVIATION.upper(),
            "sectors": sectors,
            "companies": companies,
            "url": url
        }]
