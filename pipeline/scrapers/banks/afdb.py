"""Web scrapers for the African Development Bank Group (AFDB).
"""

import re
import time
from bs4 import BeautifulSoup
from datetime import datetime
from logging import Logger
from pipeline.constants import AFDB_ABBREVIATION, RESULTS_PAGE_WORKFLOW
from pipeline.scrapers.abstract import (
    ProjectScrapeWorkflow,
    ResultsScrapeWorkflow,
    SeedUrlsWorkflow
)
from pipeline.services.database import DbClient
from pipeline.services.web import DataRequestClient, HeadlessBrowser
from pipeline.services.pubsub import PublisherClient
from typing import Dict, List


class AfdbSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Retrieves the set of AFDB search result page URLs to scrape.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        pubsub_client: PublisherClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of an `AfdbSeedUrlsWorkflow`.

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
            `None`
        """
        super().__init__(data_request_client, pubsub_client, db_client, logger)

    @property
    def next_workflow(self) -> str:
        """The name of the workflow to execute after this
        workflow has finished.
        """
        return RESULTS_PAGE_WORKFLOW

    @property
    def search_results_base_url(self) ->str:
        """The base URL for a development bank project search
        results page on ADB's website. Should be formatted
        with a result offset variable, "offset", and a maximum
        results returned variable, "max".
        """
        return "https://projectsportal.afdb.org/dataportal/VProject/list?countryId=&source=&status=&sector=&sovereign=&year=&specialBond=&covidBox=&offset={offset}&max={max}&sort=startDate&order=desc"

    @property
    def seconds_wait_page_load(self) -> int:
        """The number of seconds to wait for the project list page to load.
        """
        return 20
    
    @property
    def results_per_page(self) -> int:
        """The number of results to display per page.
        """
        return 100

    def generate_seed_urls(self) -> List[str]:
        """Generates the set of result page URLs to scrape.

        Args:
            None

        Returns:
            (`list` of `str`): The unique list of URLs.
        """
        # Launch headless Chrome browser and wait until fully loaded
        try:
            browser = HeadlessBrowser.launch_chrome()
            max= self.results_per_page
            url = self.search_results_base_url.format(offset=0, max=max)
            browser.get(url)
            time.sleep(self.seconds_wait_page_load)
        except Exception as e:
            raise RuntimeError(f"Failed to fetch webpage \"{url}\" "
                                "displaying list of AFDB projects. "
                                f"\"{e}\".") from None
        
        # Parse HTML into BeautifulSoup object
        try:
            soup = BeautifulSoup(browser.page_source, 'lxml')
        except Exception as e:
            raise RuntimeError(f"Failed to parse page HTML. \"{e}\".") from None

        # Quit browser
        browser.quit()

        # Find last page
        try:
            page_items = soup.find("ul", {"class": "pagination"})
            last_page_item = page_items.findChildren(recursive=False)[-2]
            last_page_num = int(last_page_item.find("a").text)
        except Exception as e:
            raise RuntimeError("Error parsing last page number from "
                               f"URL \"{url}\". \"{e}\".") from None
        
        # Compose project search result page URLs
        urls = [
            self.search_results_base_url.format(offset=o, max=max)
            for o in range(0, last_page_num * max, max)
        ]

        return urls

class AfdbResultsScrapeWorkflow(ResultsScrapeWorkflow):
    """Scrapes an AFDB search results page for development bank project URLs.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        pubsub_client: PublisherClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of an `AfdbResultsScrapeWorkflow`.

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
            (`None`)
        """
        super().__init__(data_request_client, pubsub_client, db_client, logger)

    @property
    def project_page_base_url(self) -> str:
        """The base URL for individual AFDB project pages.
        Should be formatted with a "project_id" variable.
        """
        return "https://projectsportal.afdb.org/dataportal/VProject/show/{project_id}"

    @property
    def seconds_wait_page_load(self) -> int:
        """The number of seconds to wait for the project list page to load.
        """
        return 20

    def scrape_results_page(self, results_page_url: str) -> List[str]:
        """Scrapes all development project page URLs from a given
        search results page on AFDB's website. NOTE: Delays must
        be placed in between requests to avoid throttling.

        Args:
            results_page_url (`str`): The URL to a search results page
                containing lists of development projects.

        Returns:
            (`list` of `str`): The list of scraped project page URLs.
        """
        # Launch headless Chrome browser and wait until fully loaded
        try:
            browser = HeadlessBrowser.launch_chrome()
            browser.get(results_page_url)
            time.sleep(self.seconds_wait_page_load)
        except Exception as e:
            raise RuntimeError(f"Failed to fetch webpage "
                               f"\"{results_page_url}\" displaying "
                               f"list of AFDB projects. \"{e}\".") from None
        
        # Parse HTML into BeautifulSoup object
        try:
            soup = BeautifulSoup(browser.page_source, 'lxml')
        except Exception as e:
            raise RuntimeError(f"Failed to parse page HTML. \"{e}\".") from None
        
        # Quit browser
        browser.quit()

        # Scrape page for project links
        try:
            urls = []
            projects_div = soup.find("div", {"id": "divContent"})
            projects_table = projects_div.find("tbody")
            for project in projects_table.find_all("tr"):
                try:
                    first_cell = project.findChildren("td")[0]
                    id = first_cell.find("a")["href"].split("/")[-1]
                    url = self.project_page_base_url.format(project_id=id)
                    urls.append(url)
                except TypeError as e:
                    continue
        except Exception as e:
            raise RuntimeError("Failed to scrape page for "
                                f"project links. \"{e}\".")
        
        return urls
            
class AfdbProjectScrapeWorkflow(ProjectScrapeWorkflow):
    """Scrapes an AFDB project page for development bank project data.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of an `AfdbProjectScrapeWorkflow`.

        Args:
            data_request_client (`DataRequestClient`): A client
                for making HTTP GET requests while adding
                random delays and rotating user agent headers.

            db_client (`DbClient`): A client for inserting and
                updating tasks in the database.

            logger (`Logger`): An instance of the logging class.

        Returns:
            `None`
        """
        super().__init__(data_request_client, db_client, logger)

    @property
    def seconds_wait_page_load(self) -> int:
        """The number of seconds to wait for the project page to load.
        """
        return 20
    
    def scrape_project_page(self, url: str) -> List[Dict]:
        """Scrapes an AFDB project page for data.

        Args:
            url (`str`): The URL for a project.

        Returns:
            (`list` of `dict`): The list of project records.
        """
        # Launch headless Chrome browser and wait until fully loaded
        try:
            browser = HeadlessBrowser.launch_chrome()
            browser.get(url)
            time.sleep(self.seconds_wait_page_load)
        except Exception as e:
            raise RuntimeError("Failed to fetch webpage for list "
                                f"of projects \"{e}\".") from None
        
        # Parse HTMl into BeautifulSoup object
        soup = BeautifulSoup(browser.page_source, 'lxml')

        # End browser session
        browser.quit()

        # Extract project number from URL
        number = url.split('/')[-1]

        # Extract project name from page title
        name = soup.find("h2", {"class": "title"}).text.strip()

        # Define local function to parse section table
        def parse_section_table(section_name: str) -> Dict:
            """Extracts "field name, field value" pairs
            from an HTML table given the name of the
            HTML h3 section header preceding the table.
            The table is assumed to have two columns,
            with field names in the left column
            and field values in the right column.

            Args:
                section_name (`str`): The section name.

            Returns:
                (`dict`): The table field names and values.
            """
            section_h3 = soup.find(string=section_name).findParent()
            section_table = section_h3.find_next_sibling("table")
            data = {}
            for row in section_table.find_all("tr"):
                name, value = row.find_all("td")
                field_name = name.text.strip()
                field_value = value.text.strip() if value else None
                data[field_name] = field_value

            return data

        # Parse HTML tables
        project_data = parse_section_table("Project Summary")
        geo_data = parse_section_table("Geographic Location")

        # Extract project approval date
        if project_data["Approval Date"]:
            date = datetime.strptime(project_data["Approval Date"], "%d %b %Y")
            year = date.year
            month = date.month
            day = date.day
        else:
            year = month = day = None

        # Extract project loan amount
        if project_data['Commitment']:
            raw_loan_amount = project_data['Commitment'].split()[1]
            match_obj = re.search(r"([\d,\.]+)", raw_loan_amount)
            loan_amount_str = match_obj.groups()[0].replace(',', '')
            loan_amount = float(loan_amount_str)
        else:
            loan_amount = None

        # Extract project country, sector, and status
        countries = geo_data["Country"]
        sectors = project_data["Sector"]
        status = project_data["Status"]

        # Extract associated agencies
        org_h3 = soup.find(string="Participating Organization").findParent()
        org_table = org_h3.find_next_sibling("table")
        divs = org_table.find_all('div', {"class": "row"})
        orgs = []
        for div in divs:
            if not div.find(string=re.compile(r"(Funding)")):
                span = div.find("span")
                org = span.text.strip()
                orgs.append(org)
        companies = ', '.join(orgs) if orgs else None

        # Compose final project record schema
        return [{
            "bank": AFDB_ABBREVIATION.upper(),
            "number": number,
            "name": name,
            "status": status,
            "year": year,
            "month": month,
            "day": day,
            "loan_amount": loan_amount,
            "loan_amount_currency": 'UAC' if loan_amount else None,
            "loan_amount_in_usd": None,
            "sectors": sectors,
            "countries": countries,
            "companies": companies,
            "url": url
        }]
