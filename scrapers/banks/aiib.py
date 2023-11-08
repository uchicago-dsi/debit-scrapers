"""Web scrapers for the Asian Infrastructure Investment Bank
(AIIB). Data retrieved by requesting a JavaScript file
containing a list of all project page URLs and then requesting
and scraping details from each project page.
"""

import bs4
import json
import re
from bs4.element import NavigableString
from datetime import datetime
from logging import Logger
from scrapers.abstract.project_scrape_workflow import ProjectScrapeWorkflow
from scrapers.abstract.seed_urls_workflow import SeedUrlsWorkflow
from scrapers.constants import AIIB_ABBREVIATION, PROJECT_PAGE_WORKFLOW
from scrapers.services.database import DbClient
from scrapers.services.data_request import DataRequestClient
from scrapers.services.pubsub import PubSubClient
from typing import Dict, List


class AiibSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Retrieves the first set of AIB URLs to scrape.
    """
    
    def __init__(
        self,
        data_request_client: DataRequestClient,
        pubsub_client: PubSubClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of an `AiibSeedUrlsWorkflow`.

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
    def projects_base_url(self) -> str:
        """The base URL for an individual AIIB project page.
        """
        return 'https://www.aiib.org'


    @property
    def next_workflow(self) -> str:
        """The name of the workflow to execute after this
        workflow has finished.
        """
        return PROJECT_PAGE_WORKFLOW


    @property
    def partial_projects_url(self) -> str:
        """The URL containing all partial project records as JSON.
        """
        return 'https://www.aiib.org/en/projects/list/.content/all-projects-data.js'

    
    def generate_seed_urls(self) -> List[str]:
        """Generates the first set of URLs to scrape.

        Args:
            None

        Returns:
            (list of str): The unique list of search result pages.
        """
        try:
            # Request JavaScript file containing list of projects
            response = self._data_request_client.get(
                url=self.partial_projects_url,
                use_random_user_agent=True,
                use_random_delay=True
            )

            # Extract project data as string from response body
            stripped_doc = re.sub("[\t\n\r]", '', response.text)
            projects_str = re.findall("(\[.*\])", stripped_doc)[0]
            projects_formatted_str = ''.join(projects_str.rsplit(',', 1))

            # Parse project string into Python dictionary
            data = json.loads(projects_formatted_str)
         
            # Generate list of project urls
            return [self.projects_base_url + proj['path'] for proj in data]
                
        except Exception as e:
            raise Exception(f"Failed to generate AIIB search pages to scrape. {e}")


class AiibProjectScrapeWorkflow(ProjectScrapeWorkflow):
    """Scrapes an AIIB project page for development bank project data.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """
        Initializes a new instance of an `AiibProjectScrapeWorkflow`.

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
        """Scrapes an AIIB project page for data.

        Args:
            url (`str`): The URL for a project.

        Returns:
            (`list` of `dict`): The project records.
        """
        # Retrieve the project page
        response = self._data_request_client.get(
            url,
            use_random_user_agent=True,
            use_random_delay=False)
        soup = bs4.BeautifulSoup(response.text, 'html.parser')

        # Get project summary metadata
        def get_project_summary_field(field_name: str):
            """Locates the given AIIB project summary field within the
            HTML document and then returns the text of the adjacent div.

            Args:
                field_name (`str`): The field name.

            Returns:
                (`str`): The extracted text if it exists.
            """
            try:
                div = soup.find(string=field_name).find_next('div')
                return div.text
            except AttributeError:
                return None

        number = get_project_summary_field("PROJECT NUMBER")
        name = soup.find("h1", {"class": "project-name"}).text
        status = get_project_summary_field("STATUS")
        date = get_project_summary_field("CONCEPT REVIEW")
        proposed_funding_amount = get_project_summary_field("PROPOSED FUNDING AMOUNT")
        approved_funding = get_project_summary_field("APPROVED FUNDING")
        sectors = get_project_summary_field("SECTOR")
        countries = get_project_summary_field("MEMBER")

        # Extract project borrower and implementing entity data
        def get_project_contact_field(field_name: str):
            """Retrieves project borrower and implementer contact
            information from an AIIB project page.

            Args:
                field_name (`str`): The name of the contact field to scrape.

            Returns:
                (`str`): The contact information.
            """
            try:
                contact_div = soup.find("h2", string=field_name).findNextSibling("div")
                contact_fields = []
                nbsp = '\xa0'
                for p in contact_div.find_all("p"):
                    for c in p.contents:
                        if c and isinstance(c, NavigableString) and c != nbsp:
                            contact_fields.append(c.strip(', '))
                return ', '.join(contact_fields)

            except AttributeError:
                return None
            
        borrower = get_project_contact_field("BORROWER")
        implementer = get_project_contact_field("IMPLEMENTING ENTITY")
        if borrower and implementer:
            companies = f"{borrower}; {implementer}"
        elif borrower:
            companies = borrower
        else:
            companies = implementer

        # Parse date field to retrieve project year, month, and day
        try:
            parsed_date = datetime.strptime(date, "%B %d, %Y")
            year = parsed_date.year
            month = parsed_date.month
            day = parsed_date.day
        except Exception:
            year = month = day = None

        # Parse loan amount field to retrieve value and currency type
        if not proposed_funding_amount and not approved_funding:
            loan_amount_currency = loan_amount = None
        else:
            loan_str = proposed_funding_amount if proposed_funding_amount else approved_funding
            loan_amount_currency = loan_str[:3]
            loan_amount_match = re.search(r"([\d,\.]+)", loan_str).groups(0)[0]
            loan_amount = float(loan_amount_match.replace(',', '')) * 10**6

        # Compose final project record schema
        return [{
            "bank": AIIB_ABBREVIATION.upper(),
            "number": number,
            "name": name,
            "status": status,
            "year": year,
            "month": month,
            "day": day,
            "loan_amount": loan_amount,
            "loan_amount_currency": loan_amount_currency,
            "loan_amount_in_usd": None,
            "sectors": sectors,
            "countries": countries,
            "companies": companies,
            "url": url
        }]
