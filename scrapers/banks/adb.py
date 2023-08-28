"""Web scrapers for the Asian Development Bank (ADB). Data
retrieved by scraping all individual project page URLs
from search result pages and then scraping details from
each project page.
"""

import re
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from logging import Logger
from scrapers.abstract.project_scrape_workflow import ProjectScrapeWorkflow
from scrapers.abstract.results_scrape_workflow import ResultsScrapeWorkflow
from scrapers.abstract.seed_urls_workflow import SeedUrlsWorkflow
from scrapers.constants import ADB_ABBREVIATION, RESULTS_PAGE_WORKFLOW
from scrapers.services.database import DbClient
from scrapers.services.data_request import DataRequestClient
from scrapers.services.pubsub import PubSubClient
from typing import Dict, List


class AdbSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Retrieves the first set of ADB URLs to scrape.
    """

    def __init__(
        self,
        pubsub_client: PubSubClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of an `AdbSeedUrlsWorkflow`.

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
        results page on ADB's website. Should be formatted
        with a page number variable, "page_num".
        """
        return 'https://www.adb.org/projects?page={page_num}'


    def generate_seed_urls(self) -> List[str]:
        """Generates the first set of URLs to scrape.

        Args:
            None

        Returns:
            (list of str): The unique list of search result pages.
        """
        try:
            last_page_num = self.find_last_page()
            logging.debug(f"Last page num found for ADB : {last_page_num}")
            result_pages = [
                self.search_results_base_url.format(page_num=n) 
                for n in range(self.first_page_num, last_page_num + 1)
            ]
            # print(f"result pages list : {result_pages}")
            return result_pages
        except Exception as e:
            raise Exception("Failed to generate ADB search "
                            f"result pages to crawl. {e}")


    def find_last_page(self) -> int:
        """Retrieves the number of the last page of
        development bank projects on the website.
        
        Args:
            None
        
        Returns:
            (int): The page number.
        """
        try:
            first_results_page: str = self.search_results_base_url.format(
                page_num=self.first_page_num)
            html = requests.get(first_results_page).text
            # print(f"Got text from results page, type :  {type(html)}")
            # print(f"Got text from results page, length :  {len(html)}")
            soup = BeautifulSoup(html, "html.parser")

            last_page_btn = soup.find('li', {"class": "pager__item--last"})
            # print(f"Got last page button : {last_page_btn}")
            last_page_num = int(last_page_btn.find("a")["href"].split('=')[-1])
            # print("Last page number :", last_page_num)
            return last_page_num

        except Exception as e:
            raise Exception("Error retrieving last page number at "
                f"'{first_results_page}'. {e}")


class AdbResultsScrapeWorkflow(ResultsScrapeWorkflow):
    """Scrapes an ADB search results page for development bank project URLs.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        pubsub_client: PubSubClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of an `AdbResultsScrapeWorkflow`.

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
        """The base URL for individual ADB project pages.
        """
        return 'https://www.adb.org'


    def scrape_results_page(self, results_page_url: str) -> List[str]:
        """Scrapes all development project page URLs from a given
        search results page on ADB's website. NOTE: Delays must
        be placed in between requests to avoid throttling.

        Args:
            results_page_url (str): The URL to a search results page
                containing lists of development projects.

        Returns:
            (list of str): The list of scraped project page URLs.
        """
        try:
            response = self._data_request_client.get(
                url=results_page_url,
                use_random_user_agent=True,
                use_random_delay=True,
                min_random_delay=1,
                max_random_delay=3
            )
            soup = BeautifulSoup(response.text, features='html.parser')
            projects_table = soup.find('div', {'class': 'list'})

            project_page_urls = []
            for project in projects_table.find_all('div', {'class': 'item'}):
                try:
                    link = project.find('a')
                    project_page_urls.append(self.project_page_base_url + link['href'])
                except TypeError:
                    continue
            
            return project_page_urls
            
        except Exception as e:
            raise Exception(f"Error scraping project page URLs from '{results_page_url}'. {e}")


class AdbProjectScrapeWorkflow(ProjectScrapeWorkflow):
    """Scrapes an ADB project page for development bank project data.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of an `AdbProjectScrapeWorkflow`.

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
        """Scrapes an AFDB project page for data. NOTE: Delays
        must be placed in between requests to avoid throttling.

        Args:
            url (str): The URL for a project. Has the form:
                'https://www.adb.org/projects/{project_id}/main'.

        Returns:
            (list of dict): The list of project records.
        """
        # Request page and parse HTML
        # print(f"URL to scrape : {url}")
        response = self._data_request_client.get(
            url=url,
            use_random_user_agent=True,
            use_random_delay=True,
            min_random_delay=1,
            max_random_delay=4
        )
        soup = BeautifulSoup(response.text, features='html.parser')

        # Find first project table holding project background details
        table = soup.find('article')
        # print(f"Table here : {table}")

        # Extract project name, number, and status
        def get_field(detail_name):
            try:
                element = table.find(string=detail_name)
                parent = element.findParent()
                sibling_cell = parent.find_next_siblings('td')[0]
                raw_text = sibling_cell.text
                field = raw_text.strip(' \n')
                return None if field == "" else field
            except AttributeError:
                return None
            
        name = get_field("Project Name")
        number = get_field("Project Number")
        status = get_field("Project Status")
        # print(f"table : {table}")

        # Extract and format countries
        country_label = table.find(string="Country / Economy")
        # print(f"country label : {country_label}")
        if not country_label:
            country_label = table.find(string="Country")
        parent = country_label.findParent()
        sibling_cell = parent.find_next_siblings('td')[0]
        contents = sibling_cell.contents
        countries = []

        for c in contents:
            if isinstance(c, str) and c not in (' ', '\n', '\t', '\r'):
                country_parts = c.split(',')
                uses_formal_name = len(country_parts) == 2
                if uses_formal_name:
                    country = f"{country_parts[1].strip()} {country_parts[0]}"
                else:
                    country = c
                countries.append(country)

        countries = ', '.join(countries)
            
        # Extract ADB funding amount
        total_amount = 0
        finance_tables = soup.find_all("table", {"class": "financing"})
        for t in finance_tables:

            # Determine whether table represents loan or technical assistance (TA)
            loan_plan_str = t.find(string="Financing Plan")
            tech_assist_plan_str = t.find(string="Financing Plan/TA Utilization")

            # Parse loan amount (always in millions USD) and add to total
            if loan_plan_str:
                rows = t.find_all('tr', class_=lambda c: c != 'subhead')
                for r in rows:
                    loan_label = r.find(string='ADB')
                    if not loan_label:
                        continue
                    loan_cell = loan_label.findParent().find_next_sibling('td')
                    loan_str = re.search(r"([\d,\.]+)", loan_cell.text).groups(0)[0]
                    loan_amount = float(loan_str.replace(',', '')) * 10**6
                    total_amount += loan_amount

            # Parse technical assistance amount and add to total
            if tech_assist_plan_str:
                ta_row = t.find_all('tr')[-1]
                ta_cell = ta_row.find('td')
                ta_amount_str = re.search(r"([\d,\.]+)", ta_cell.text).groups(0)[0]
                ta_amount = float(ta_amount_str.replace(',', ''))
                total_amount += ta_amount

        # Extract sectors
        sector_header_str = table.find(string="Sector / Subsector")
        sector_row = sector_header_str.findParent()
        sector_names = sector_row.find_next_sibling('td')
        sector_strongs = sector_names.find_all("strong", {"class": "sector"})
        sectors = None if not sector_strongs else ', '.join(s.text for s in sector_strongs)

        # Extract companies
        try:
            agency_text = soup.find(string="Implementing Agency")
            if not agency_text:
                agency_text = soup.find(string="Executing Agencies")
            parent = agency_text.findParent()
            agency_cell = parent.find_next_siblings("td")[0]
            company_spans = agency_cell.find_all("span", {"class": "address-company"})
            companies = ', '.join(c.text.strip(' \n') for c in company_spans if c.text)
        except Exception:
            companies = None

        # Extract project approval date and parse year, month, and day
        try:
            approval_text = soup.find(string="Approval")
            parent = approval_text.findParent()
            approval_cell = parent.find_next_siblings("td")[0]
            parsed_date = datetime.strptime(approval_cell.text, "%d %b %Y")
            year = parsed_date.year
            month = parsed_date.month
            day = parsed_date.day
        except Exception:
            year = month = day = None

        # Compose final project record schema
        return [{
            "bank": ADB_ABBREVIATION.upper(),
            "number": number,
            "name": name,
            "status": status,
            "year": year,
            "month": month,
            "day": day,
            "loan_amount": total_amount,
            "loan_amount_currency": "USD" if total_amount else None,
            "loan_amount_in_usd": total_amount,
            "sectors": sectors,
            "countries": countries,
            "companies": companies,
            "url": url.replace('/print', '')
        }]



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
    seed_workflow = AdbSeedUrlsWorkflow(None, None, None)
    print(seed_workflow.generate_seed_urls())

    # Test 'ResultsScrapeWorkflow'
    res_scrape_workflow = AdbResultsScrapeWorkflow(data_request_client, None, None, None)
    url = 'https://www.adb.org/projects?page=558'
    project_page_urls = res_scrape_workflow.scrape_results_page(url)
    print(project_page_urls)

    # Test 'ProjectScrapeWorkflow'
    proj_scrape_workflow = AdbProjectScrapeWorkflow(data_request_client, None, None)
    url = 'https://www.adb.org/projects/53303-001/main'
    print(proj_scrape_workflow.scrape_project_page(url))
