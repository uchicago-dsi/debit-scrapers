'''
undp.py

A web scraper for the United Nations Development Programme (UNDP).
'''

import io
import pandas as pd
import requests
from datetime import datetime
from logging import Logger
from scrapers.abstract.project_scrape_workflow import ProjectScrapeWorkflow
from scrapers.abstract.seed_urls_workflow import SeedUrlsWorkflow
from scrapers.constants import PROJECT_PAGE_WORKFLOW, UNDP_ABBREVIATION
from scrapers.services.data_request import DataRequestClient
from scrapers.services.database import DbClient
from scrapers.services.pubsub import PubSubClient
from typing import Dict, List


class UndpSeedUrlsWorkflow(SeedUrlsWorkflow):
    '''
    Generates the first set of URLs/API resources to
    query for development bank project data.
    '''
    
    def __init__(
        self,
        pubsub_client: PubSubClient,
        db_client: DbClient,
        logger: Logger) -> None:
        '''
        The public constructor.

        Parameters:
            pubsub_client (PubSubClient): A wrapper client for the 
                Google Cloud Platform Pub/Sub API. Configured to
                publish messages to the appropriate 'tasks' topic.

            db_client (DbClient): A client used to insert and
                update tasks in the database.

            logger (Logger): An instance of the logging class.

        Returns:
            None
        '''
        super().__init__(pubsub_client, db_client, logger)


    @property
    def next_workflow(self) -> str:
        '''
        The name of the workflow to execute after this
        workflow has finished.
        '''
        return PROJECT_PAGE_WORKFLOW


    @property
    def project_list_base_url(self) -> str:
        '''
        The base URL for retrieving a list of project data.
        '''
        return 'https://api.open.undp.org/api/v1/undp/export_csv/'


    @property
    def project_base_url(self) -> str:
        '''
        The base URL for retrieving a single detailed project record.
        '''
        return 'https://api.open.undp.org/api/projects/{}.json'


    def generate_seed_urls(self) -> List[str]:
        '''
        Generates the first set of UNDP API URLs
        from which to retrieve data.

        Parameters:
            None

        Returns:
            (list of str): The URLs.
        ''' 
        try:
            # Retrieve list of unique projects from UNDP's public API
            response = requests.get(self.project_list_base_url)
            projects_df = pd.read_csv(
                filepath_or_buffer=io.StringIO(response.text),
                usecols=['project_id'],
                dtype='object'
            )

            # Create URLs for individual project details
            project_ids = projects_df['project_id'].tolist()
            return [self.project_base_url.format(id) for id in project_ids]

        except Exception as e:
            raise Exception(f"Failed to generate list of UNDP project API URLs. {e}")


class UndpProjectScrapeWorkflow(ProjectScrapeWorkflow):
    '''
    Retrieves project data from UNDP and saves it to a database.
    '''
    
    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: DbClient,
        logger: Logger) -> None:
        '''
        The public constructor.

        Parameters:
            data_request_client (DataRequestClient): A client
                for making HTTP GET requests while adding
                random delays and rotating user agent headers.

            db_client (DbClient): A client for inserting and
                updating tasks in the database.

            logger (Logger): An instance of the logging class.

        Returns:
            None
        '''
        super().__init__(data_request_client, db_client, logger)


    @property
    def project_page_base_url(self) -> str:
        '''
        The base URL for an UNDP project page.
        '''
        return 'https://open.undp.org/projects/{}'


    def scrape_project_page(self, url: str) -> List[Dict]:
        '''
        Scrapes an UNDP project page for data.

        Parameters:
            url (str): The URL for a project.

        Returns:
            (list of dict): The project records.
        '''
        # Retrieve project JSON
        response = self._data_request_client.get(
            url,
            use_random_delay=True,
            min_random_delay=1,
            max_random_delay=3
        )
        project = response.json()
       
        # Compute project status
        start_date = datetime.strptime(project['start'], "%Y-%m-%d").date()
        end_date = datetime.strptime(project['end'], "%Y-%m-%d").date()
        current_date = datetime.utcnow().date()
        if current_date < start_date:
            status = "Proposed"
        elif current_date > start_date and current_date < end_date:
            status = "Ongoing"
        else:
            status = 'Completed'

        # Extract project sector(s) and companies/donors
        sectors = ', '.join(set(o['focus_area_descr'] for o in project['outputs']))
        companies = ', '.join(set(d for o in project['outputs'] for d in o['donor_name']))

        # Correct formal country names to remove comma
        countries = project['operating_unit']
        if countries:
            name_parts = countries.split(',')
            uses_formal_name = len(name_parts) == 2
            if uses_formal_name:
                countries = f"{name_parts[1].strip()} {name_parts[0]}"

        # Compose final project record schema
        return [{
            "bank": UNDP_ABBREVIATION.upper(),
            "number": project['project_id'],
            "name": project['project_title'],
            "status": status,
            "year": start_date.year,
            "month": start_date.month,
            "day": start_date.day,
            "loan_amount": project['budget'],
            "loan_amount_currency": 'USD',
            "loan_amount_in_usd": project['budget'],
            "sectors": sectors if sectors else None,
            "countries": project['operating_unit'],
            "companies": companies if companies else None,
            "url": self.project_page_base_url.format(project['project_id'])
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
    w = UndpSeedUrlsWorkflow(None, None, None)
    print(w.generate_seed_urls())

    # Test 'ProjectPageScrapeWorkflow'
    w = UndpProjectScrapeWorkflow(data_request_client, None, None)
    url = 'https://api.open.undp.org/api/projects/00110684.json'
    print(w.scrape_project_page(url))