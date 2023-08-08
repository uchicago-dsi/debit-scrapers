"""Web scrapers for the African Development Bank Group (AFDB).
Individual projects are viewable in the "Data Portal" section
of the site, but a list view of all projects is not publicly
available. To circumvent this limitation, project ids are
collected from the older "Project Portfolio" section and
newer "Map" tool, which represent projects approved from
April 21, 1967, through June 19, 2019, inclusive, and 2002
through the present, respectively. The collected ids are then
used to generate URLs to the "Data Portal" project pages,
which are requested and scraped for data.
"""

import pandas as pd
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from io import BytesIO
from logging import Logger
from scrapers.abstract.project_scrape_workflow import ProjectScrapeWorkflow
from scrapers.abstract.seed_urls_workflow import SeedUrlsWorkflow
from scrapers.constants import AFDB_ABBREVIATION, PROJECT_PAGE_WORKFLOW
from scrapers.services.database import DbClient
from scrapers.services.data_request import DataRequestClient
from scrapers.services.pubsub import PubSubClient
from typing import Dict, List


class AfdbSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Retrieves the set of AFDB project page URLs to scrape.
    """

    def __init__(
        self,
        pubsub_client: PubSubClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """
        Initializes a new instance of an `AfdbSeedUrlsWorkflow`.

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
        """
        The name of the workflow to execute after this
        workflow has finished.
        """
        return PROJECT_PAGE_WORKFLOW


    @property
    def project_download_url(self) ->str:
        """
        The URL containing all project records.
        """
        return "https://projectsportal.afdb.org/dataportal/VProject/exportProjectList?_format=XLS&_name=&_file=dataPortal_project_list&reportName=dataPortal_project_list"


    @property
    def project_page_base_url(self) -> str:
        """
        The base URL for an individual project page.
        Should be formatted with the project id.
        """
        return 'https://projectsportal.afdb.org/dataportal/VProject/show/{project_id}'


    def generate_seed_urls(self) -> List[str]:
        """Generates the set of project page URLs to scrape.

        References:
        - https://pandas.pydata.org/docs/whatsnew/v1.2.0.html
        - https://stackoverflow.com/a/65266497

        Args:
            None

        Returns:
            (list of str): The unique list of URLs.
        """
        # Retrieve Excel data
        try:
            response = requests.get(self.project_download_url, timeout=120)
            response.raise_for_status()
            df = pd.read_excel(
                io=BytesIO(response.content),
                engine='openpyxl',
                sheet_name='dataPortal_project_list',
                skipfooter=2)
        except Exception as e:
            raise Exception(f"Failed to seed AfDB urls. Error "
                "requesting and parsing Excel project data from "
                "the African Development Bank into Pandas "
                f"DataFrame. {e}")

        # Compose project page URLs
        try:
            urls = [
                self.project_page_base_url.format(project_id=id)
                for id in df['Project Code'].unique().tolist()
            ]
        except Exception as e:
            raise Exception(f"Failed to seed AfDB urls. Error "
                "extracting unique project identifiers from "
                f"Pandas DataFrame. {e}")

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
            None
        """
        super().__init__(data_request_client, db_client, logger)

    
    def scrape_project_page(self, url: str) -> List[Dict]:
        """Scrapes an AFDB project page for data.

        Args:
            url (str): The URL for a project.

        Returns:
            (list of dict): The list of project records.
        """
        # Retrieve HTML
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'lxml')

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
                section_name (str): The section name.

            Returns:
                (dict): The table field names and values.
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


if __name__ == "__main__":
    # Test 'SeedUrlsWorkflow'
    # NOTE: Performs a download that takes
    # several seconds to complete.
    w = AfdbSeedUrlsWorkflow(None, None, None)
    print(w.generate_seed_urls())

    # Test 'ProjectScrapeWorkflow'
    w = AfdbProjectScrapeWorkflow(None, None, None)
    url = 'https://projectsportal.afdb.org/dataportal/VProject/show/P-Z1-FAB-030'
    print(w.scrape_project_page(url))
