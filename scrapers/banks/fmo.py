"""Web scrapers for the Dutch entrepreneurial development bank (FMO).
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime
from logging import Logger
from scrapers.abstract.project_scrape_workflow import ProjectScrapeWorkflow
from scrapers.abstract.results_scrape_workflow import ResultsScrapeWorkflow
from scrapers.abstract.seed_urls_workflow import SeedUrlsWorkflow
from scrapers.constants import FMO_ABBREVIATION, RESULTS_PAGE_WORKFLOW
from scrapers.services.data_request import DataRequestClient
from scrapers.services.database import DbClient
from scrapers.services.pubsub import PubSubClient
from typing import Dict, List


class FmoSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Retrieves the first set of FMO URLs to scrape.
    """

    def __init__(
        self,
        pubsub_client: PubSubClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of a `FmoSeedUrlsWorkflow`.

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
    def first_page_num(self) -> int:
        """The number of the first search results page.
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
        results page on FMO's website. Should be formatted
        with a page number.
        """
        return 'https://www.fmo.nl/worldmap?page={}'


    def generate_seed_urls(self) -> List[str]:
        """Generates the first set of URLs to scrape.

        Args:
            None

        Returns:
            (list of str): The unique list of search result pages.
        """
        try:
            last_page_num = self.find_last_page()
            results_page_urls = [
                self.search_results_base_url.format(n)
                for n in range(1, last_page_num + 1)
            ]
            return results_page_urls
        except Exception as e:
            raise Exception(f"Failed to generate search result pages to crawl. {e}")


    def find_last_page(self) -> int:
        """Retrieves the number of the last page of
        development bank projects on the website.
        
        Args:
            None
        
        Returns:
            (int): The page number.
        """
        try:
            first_page_url = self.search_results_base_url.format(self.first_page_num)
            html = requests.get(first_page_url).text
            soup = BeautifulSoup(html, "html.parser")

            pager = soup.find('div', {"class":"pbuic-pager-container"})
            last_page_num = int(pager.find_all("a")[-2]["href"].split('=')[-1])
            return last_page_num
        except Exception as e:
            raise Exception(f"Error retrieving last page number. {e}")


class FmoResultsScrapeWorkflow(ResultsScrapeWorkflow):
    """Scrapes an FMO search results page for development bank project URLs.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        pubsub_client: PubSubClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of a `FmoResultsScrapeWorkflow`.

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


    def scrape_results_page(self, results_page_url: str) -> List[str]:
        """Scrapes all development project page URLs from a given
        search results page on FMO's website.

        Args:
            results_page_url (str): The URL to a search results page
                containing lists of development projects.

        Returns:
            (list of str): The list of scraped project page URLs.
        """
        try:
            source = requests.get(results_page_url).text
            soup = BeautifulSoup(source, "html.parser")
            urls = [
                proj["href"] for proj in 
                soup.find_all('a', {"class":"ProjectList__projectLink"})
            ]
            return urls
        except Exception as e:
            raise Exception(f"Error scraping '{results_page_url}' for project URLs. {e}")


class FmoProjectScrapeWorkflow(ProjectScrapeWorkflow):
    """Scrapes an FMO project page for development bank project data.
    """
    
    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of a `FmoProjectScrapeWorkflow`.

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
        """Scrapes a FMO project page for data.

        Args:
            url (str): The URL for a project.

        Returns:
            (list of dict): The project records.
        """
        try:            
            # Retrieve HTML
            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')

            # Extract data from page
            def extract_field(html, field_name):
                try:
                    html = html.find(text=field_name)
                    parent = html.findParent().findNextSibling()
                    return parent.text.strip()
                except AttributeError:
                    return None

            number = url.split("/")[-1] if url else None
            name = soup.find("h2", {"class": "ProjectDetail__title"}).text.strip()
            sectors = extract_field(soup, "Sector")
            countries = extract_field(soup, "Country")

            # Correct formal country names to remove comma
            if countries:
                name_parts = countries.split(',')
                uses_formal_name = len(name_parts) == 2
                if uses_formal_name:
                    countries = f"{name_parts[1].strip()} {name_parts[0]}"

            # Parse financing field for loan amount and currency type
            financing = extract_field(soup, "Total FMO financing")
            if financing and financing != 'n.a.':
                loan_amount_currency, loan_amount, _ = financing.split(' ')
                loan_amount = float(loan_amount) * 10**6
            else:
                loan_amount_currency = loan_amount = None

            # Parse date to retrieve year, month, and day
            date = extract_field(soup, "Signing date")
            if not date:
                year = month = day = None
            else:
                parsed_date = datetime.strptime(date, "%m/%d/%Y") 
                year = parsed_date.year
                month = parsed_date.month
                day = parsed_date.day

            # Compose final project record schema
            return [{
                "bank": FMO_ABBREVIATION.upper(),
                "number": number,
                "name": name,
                "status": None,
                "year": year,
                "month": month,
                "day": day,
                "loan_amount": loan_amount,
                "loan_amount_currency": loan_amount_currency,
                "loan_amount_in_usd": None,
                "sectors": sectors,
                "countries": countries,
                "companies": None,
                "url": url
            }]

        except Exception as e:
            raise Exception(f"Error scraping project page '{url}'. {e}")
