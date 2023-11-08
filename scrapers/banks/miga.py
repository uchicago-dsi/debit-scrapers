"""Web scrapers for the Multilateral Investment Guarantee Agency
(MIGA). Data retrieved by generating a list of all possible
search result pages, scraping individual project URLs from
those pages, scraping fields from each project page, and then
mapping the fields to an expected output schema.
"""

import re
import requests
from bs4 import BeautifulSoup
from logging import Logger
from scrapers.abstract.project_scrape_workflow import ProjectScrapeWorkflow
from scrapers.abstract.results_scrape_workflow import ResultsScrapeWorkflow
from scrapers.abstract.seed_urls_workflow import SeedUrlsWorkflow
from scrapers.constants import MIGA_ABBREVIATION, RESULTS_PAGE_WORKFLOW
from scrapers.services.data_request import DataRequestClient
from scrapers.services.database import DbClient
from scrapers.services.pubsub import PubSubClient
from typing import Dict, List


class MigaSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Retrieves the first set of MIGA URLs to scrape.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        pubsub_client: PubSubClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of a `MigaSeedUrlsWorkflow`.

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
        return RESULTS_PAGE_WORKFLOW


    @property
    def search_results_base_url(self) -> str:
        """The base URL for a development bank project search
        results page on IDB's website. Should be formatted
        with the page number.
        """
        return "https://www.miga.org/projects?page={page_number}"


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
                self.search_results_base_url.format(page_number=n)
                for n in range(last_page_num + 1)
            ]
            return result_pages
        except Exception as e:
            self._logger.error(f"Failed to generate search result pages to crawl. {e}")


    def find_last_page(self) -> int:
        """Retrieves the number of the last page of development
        bank projects on the website.
        
        Args:
            None
        
        Returns:
            (int): The page number.
        """
        try:
            r = self._data_request_client.get(self.search_results_base_url)
            r.raise_for_status()
            html = r.text
            soup = BeautifulSoup(html, "html.parser")
            last = soup.find('li', {"class":"pager__item pager__item--last"})
            last_page = int(last.find("a")["href"].split('=')[1])
            return last_page
        except Exception as e:
            self._logger.error("Error retrieving last page number at "
                f"'{self.search_results_base_url}'. {e}")


class MigaResultsScrapeWorkflow(ResultsScrapeWorkflow):
    """Scrapes a MIGA search results page for development bank project URLs.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        pubsub_client: PubSubClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of a `MigaResultsScrapeWorkflow`.

        Args:
            pubsub_client (`PubSubClient`): A wrapper client for the 
                Google Cloud Platform Pub/Sub API. Configured to
                publish messages to the appropriate 'tasks' topic. 

            logger (`Logger`): An instance of the logging class.

        Returns:
            None
        """
        super().__init__(data_request_client, pubsub_client, db_client, logger)


    @property
    def ifc_disclosures_base_url(self) -> str:
        """The base URL for project disclosures from the International
        Finance Corporation (IFC). Some MIGA search result pages
        contain links to IFC projects, a fellow member of the
        World Bank Group.
        """
        return "https://disclosures.ifc.org"


    @property
    def miga_projects_base_url(self) -> str:
        """The base URL for MIGA projects.
        """
        return "https://www.miga.org"

        
    def scrape_results_page(self, results_page_url: str) -> List[str]:
        """Scrapes all development project page URLs from a given
        search results page on MIGA's website. Skips URLs that
        are linked to the IFC, which has its own scraper in
        this project.

        Args:
            results_page_url (`str`): The URL to a search results page
                containing lists of development projects.

        Returns:
            (list of str): The list of scraped project page URLs.
        """
        try:
            response = self._data_request_client.get(results_page_url)
            source = response.text
            soup = BeautifulSoup(source, "html.parser")
            projects_ctr_div = soup.find("div", {"class": "featured-projects"})
            projects_div = projects_ctr_div.find("div", {"class": "view-content"}, recursive=False)

            urls = []
            for project in projects_div.find_all('h5', {"class":"page-title"}):
                href = project.find("a")["href"]
                if href.startswith(self.ifc_disclosures_base_url):
                    continue
                project_url = href if "http" in href else self.miga_projects_base_url + href
                urls.append(project_url)
            return urls
        except Exception as e:
            self._logger.error(f"Error scraping project page URLs from '{results_page_url}'. {e}")


class MigaProjectScrapeWorkflow(ProjectScrapeWorkflow):
    """Scrapes a MIGA project page for development bank project data.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of a `MigaProjectScrapeWorkflow`.

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
        """Scrapes a MIGA project page for data.

        Args:
            url (`str`): The URL for a project.

        Returns:
            (`list` of `dict`): The project record(s).
        """
        try:
            html = requests.get(url).text
            soup = BeautifulSoup(html, 'html.parser')
            def safe_nav(func):
                try:
                    html = func(soup)
                    return html.text.strip()
                except AttributeError:
                    return None

            country = safe_nav(lambda s: s.find("div", class_="field--name-field-host-country"))
            name = safe_nav(lambda s: s.find("h1"))
            status = safe_nav(lambda s: s.find("div", {"class" : "field--name-field-project-status"}))
            fiscal_year = safe_nav(lambda s: s.find("div", class_="field--name-field-fiscal-year").find("div", class_="field--item"))
            company = safe_nav(lambda s: s.find("div", class_="field--name-field-guarantee-holder-term").find("div", class_="field--item"))
            project_number = safe_nav(lambda s: s.find("div", class_="field--name-field-project-id").find("div", class_="field--item"))
            sector = safe_nav(lambda s: s.find("div", class_="field--name-field-sector").find("div", class_="field--item"))
            amount = safe_nav(lambda s: s.find("div", class_="field--name-field-gross-exposure-up-to"))

            record = {
                "bank": MIGA_ABBREVIATION.upper(),
                "number": project_number,
                "name": name,
                "status": status,
                "year": int(fiscal_year),
                "month": None,
                "day": None,
                "loan_amount": amount,
                "loan_amount_currency": None,
                "loan_amount_in_usd": None,
                "sectors": sector,
                "countries": country,
                "companies": company,
                "url": url
            }

            r = record.copy()

            # Format project number
            r['number'] = r['number'].replace(',', '').replace(' ', ',')

            # Format country name
            if r['countries']:
                countries = re.sub('[\r\n\t]', ' ', r['countries'])
                countries = countries.split('and')
                final_countries = []
                for c in countries:
                    name_parts = c.split(',')
                    uses_formal_country_name = len(name_parts) == 2
                    if uses_formal_country_name:
                        final_countries.append(f"{name_parts[1].strip()} {name_parts[0].strip()}")
                    else:
                        final_countries.append(name_parts[0].strip())

                r['countries'] = ','.join(final_countries)
            
            # Set loan amount currency type
            if r['loan_amount']:
                if r['loan_amount'].startswith("$EUR") or r['loan_amount'].startswith('€'):
                    r['loan_amount_currency'] = 'EUR'

                if r['loan_amount'].startswith("$"):
                    r['loan_amount_currency'] = 'USD'

                # Make loan amount numeric
                leading_decimal = re.search(r'(\d+\.*\d*)', r['loan_amount']).group(1)
                r['loan_amount'] = float(leading_decimal) * 10**6

            return [r]

        except Exception as e:
            raise Exception(f"Failed to scrape MIGA project page {url}. {e}")
