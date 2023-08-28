"""Web scrapers for the development bank Proparco. Data retrieved
by scraping the list of project pages from the site and then
iteratively scraping details from each page. 
"""

import re
import requests
from bs4 import BeautifulSoup
from logging import Logger
from scrapers.abstract.project_scrape_workflow import ProjectScrapeWorkflow
from scrapers.abstract.seed_urls_workflow import SeedUrlsWorkflow
from scrapers.constants import PRO_ABBREVIATION, PROJECT_PAGE_WORKFLOW
from scrapers.services.data_request import DataRequestClient
from scrapers.services.database import DbClient
from scrapers.services.pubsub import PubSubClient
from typing import Dict, List


class ProSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Retrieves the first set of Proparco URLs to scrape.
    """

    def __init__(
        self,
        pubsub_client: PubSubClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of a `ProSeedUrlsWorkflow`.

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
    def next_workflow(self) -> str:
        """The name of the workflow to execute after this
        workflow has finished.
        """
        return PROJECT_PAGE_WORKFLOW


    @property
    def search_results_base_url(self) -> str:
        """The base URL for development bank project search
        results page on Proparco's website.
        """
        return 'https://www.proparco.fr/en/carte-des-projets-list?page=all&query=%2A&view=start'


    @property
    def site_base_url(self) -> str:
        """The base URL for Proparco's website.
        """
        return "https://www.proparco.fr"


    def generate_seed_urls(self) -> List[str]:
        """Generates the first set of URLs to scrape.

        Args:
            None

        Returns:
            (list of str): The project page URLs.
        """
        print("Seeding URLs for PRO")
        try:
            response = requests.get(self.search_results_base_url)
            html = response.text
            soup = BeautifulSoup(html, "lxml")

            search_results = soup.find("div", class_="ctsearch-result-list")
            anchors = search_results.find_all("a")
            links = [f"{self.site_base_url}{a['href']}" for a in anchors if a.parent.name == 'h3']
            return links

        except Exception as e:
            raise Exception(f"Error retrieving list of project page URLs. {e}")


class ProProjectScrapeWorkflow(ProjectScrapeWorkflow):
    """Scrapes a Proparco project page for development bank project data.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of a `ProProjectScrapeWorkflow`.

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
        """Scrapes a Proparco project page for data.

        Args:
            url (str): The URL for a project.

        Returns:
            (list of dict): The project record(s).
        """
        # Retrieve HTML
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract project name
        name = soup.find("h1", class_="title").text.replace("\n", "").strip()

        # Extract project signature date
        def date_part_to_int(date_css_class: str):
            """A local function to convert a date component
            (i.e., year, month, or day) formatted as a
            string with a leading zero into an integer.

            Args:
                part (str): The date part.

            Returns:
                (int): The parsed date part.
            """
            date_part = date_field_div.find("span", class_=date_css_class).text
            digits = date_part[:-1] if date_part.endswith('/') else date_part
            return int(digits[1:]) if digits.startswith('0') else int(digits)

        try:
            date_field_div = soup.find("div", class_="date start").find("div", class_="value")
            year = date_part_to_int("year")
            month = date_part_to_int("month")
            day = date_part_to_int("day")
        except AttributeError:
            year = month = day = None

        # Extract project loan amount (EUR)
        try:
            loan_amount_div = soup.find("div", class_="funding-amount")
            loan_amount_str = loan_amount_div.find("div", class_="amount").text.replace(" ", "")
            loan_amount_match = re.search(r"([\d,\.]+)", loan_amount_str).groups(0)[0]
            loan_amount_value = float(loan_amount_match)
            loan_amount_currency = 'EUR'
        except AttributeError:
            loan_amount_value = loan_amount_currency = None

        # Extract project sectors
        try:
            sectors = soup.find("div", class_="sector").find("div", class_="field__item").text
        except AttributeError:
            sectors = None

        # Extract project countries
        try:
            country_div = soup.find("div", class_="city").find("div", class_="value")
            countries = ', '.join(c.text.strip() for c in country_div.find_all("span"))
            if not countries:
                countries = None
        except AttributeError:
            countries = None

        # Extract project companies
        try:
            companies = soup.find("div", class_="field--name-field-client-name").text
        except AttributeError:
            companies = None

        # Compose final project record schema
        return [{
            "bank": PRO_ABBREVIATION.upper(),
            "number": None,
            "name": name,
            "status": None,
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



if __name__ == "__main__":
    # Test 'StartScrapeWorkflow'
    w = ProSeedUrlsWorkflow(None, None, None)
    print(w.generate_seed_urls())

    # Test 'ProjectScrapeWorkflow'
    w = ProProjectScrapeWorkflow(None, None, None)
    url = "https://www.proparco.fr/en/carte-des-projets/ecobank-trade-finance"
    print(w.scrape_project_page(url))