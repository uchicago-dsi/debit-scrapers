"""Integration tests for the bank scrapers.
"""

import time
import yaml
from pipeline.constants import (
    DOWNLOAD_WORKFLOW,
    PROJECT_PAGE_WORKFLOW,
    PROJECT_PARTIAL_PAGE_WORKFLOW,
    RESULTS_PAGE_MULTISCRAPE_WORKFLOW,
    RESULTS_PAGE_WORKFLOW,
    SEED_URLS_WORKFLOW,
    TESTS_DIR_PATH
)
from pipeline.scrapers.abstract import (
    ProjectDownloadWorkflow,
    ProjectPartialScrapeWorkflow,
    ProjectScrapeWorkflow,
    ResultsMultiScrapeWorkflow,
    ResultsScrapeWorkflow,
    SeedUrlsWorkflow
)
from pipeline.scrapers.registry import (
    StarterWorkflowRegistry,
    WorkflowClassRegistry
)
from pipeline.services.web import DataRequestClient
from pipeline.tests.integration.common import ScraperFixturesMixin
from typing import List


def load_url_params(file_name: str) -> List[List[str]]:
    """Loads bank URL parameters from a YAML file for testing.

    Args:
        file_name (`str`): The name of the yaml file. 
            Expected to exist in the `params` folder 
            within the configured test directory.

    Returns:
        (`list` of `list` of `str`): The parameters to use for
            testing. Consists of a list of lists where each
            item contains the name of the bank followed
            by the URL to request and scrape. For example:
            `[["afdb", "https://<...>"]]`.
    """
    with open(f"{TESTS_DIR_PATH}/params/{file_name}") as f:
        bank_params = yaml.safe_load(f)
        return [
            [bank, url] 
            for params in bank_params
            for bank, url_list in params.items()
            for url in url_list
        ]

def pytest_generate_tests(metafunc) -> None:
    """Collects, and supplies parameters for, test functions.

    References:
    - https://stackoverflow.com/a/69285727
    - https://pytest-with-eric.com/introduction/pytest-generate-tests/
    - https://docs.pytest.org/en/7.1.x/reference/reference.html#metafunc

    Args:
        metafunc: The current test class instance.

    Returns:
        `None`
    """
    # Seed URLs workflow
    if metafunc.function == TestWorkflows.test_generate_seed_urls:
        params = StarterWorkflowRegistry.list(
            workflow_type=SEED_URLS_WORKFLOW
        )
        metafunc.parametrize("bank", params)

    # Download workflow
    elif metafunc.function == TestWorkflows.test_download:
        params = StarterWorkflowRegistry.list(
            workflow_type=DOWNLOAD_WORKFLOW
        )
        metafunc.parametrize("bank", params)

    # Result page scrape workflow
    elif metafunc.function == TestWorkflows.test_result_page_scrape:
        params = load_url_params("result_pages.yaml")
        metafunc.parametrize("bank,results_page_url", params)

    # Result page "multi-scrape" workflow
    elif metafunc.function == TestWorkflows.test_result_page_multi_scrape:
        params = load_url_params("result_pages_multi.yaml")
        metafunc.parametrize("bank,results_page_url", params)

    # Project page scrape workflow
    elif metafunc.function == TestWorkflows.test_project_page_scrape:
        params = load_url_params("project_pages.yaml")
        metafunc.parametrize("bank,project_page_url", params)

    # Project page "partial scrape" workflow
    elif metafunc.function == TestWorkflows.test_project_partial_page_scrape:
        params = load_url_params("project_pages_partial.yaml")
        metafunc.parametrize("bank,project_partial_page_url", params)

    else:
        raise Exception(f"The current function, \"{metafunc.function}\" "
                        "has not been configured for dynamic parametrization.")
    
class TestWorkflows(ScraperFixturesMixin):
    """Tests workflows across all banks.
    """
    
    def test_download(
        self, 
        bank: str, 
        data_request_client: DataRequestClient) -> None:
        """Asserts that downloading a data file from
        a URL does not result in an exception.
        
        Args:
            bank (`str`): The abbreviation for the bank or
                financial institution (e.g., "AFDB").

            data_request_client (`DataRequestClient`): An instance
                of a client used to make HTTP requests while 
                rotating headers.

        Returns:
            (`None`)
        """
        workflow: ProjectDownloadWorkflow = WorkflowClassRegistry.get(
            source=bank,
            workflow_type=DOWNLOAD_WORKFLOW,
            data_request_client=data_request_client,
            pubsub_client=None,
            db_client=None
        )
        workflow.get_projects()

    def test_generate_seed_urls(
        self, 
        bank: str, 
        data_request_client: DataRequestClient) -> None:
        """Asserts that generating the first set of URLs to
        scrape does not result in an exception.
        
        Args:
            bank (`str`): The abbreviation for the bank or
                financial institution (e.g., "AFDB").

            data_request_client (`DataRequestClient`): An instance
                of a client used to make HTTP requests while 
                rotating headers.

        Returns:
            (`None`)
        """
        workflow: SeedUrlsWorkflow = WorkflowClassRegistry.get(
            source=bank,
            workflow_type=SEED_URLS_WORKFLOW,
            data_request_client=data_request_client,
            pubsub_client=None,
            db_client=None
        )
        workflow.generate_seed_urls()

    def test_project_page_scrape(
        self, 
        bank: str,
        project_page_url: str,
        data_request_client: DataRequestClient) -> None:
        """Asserts that scraping a project page for data
        does not result in an exception. Sleeps for three
        seconds in between HTTP calls to avoid potential 
        throttling by the bank website.
        
        Args:
            bank (`str`): The abbreviation for the bank or
                financial institution (e.g., "AFDB").

            project_page_url (`str`): A URL to a project 
                page on the bank's website.

            data_request_client (`DataRequestClient`): An instance
                of a client used to make HTTP requests while 
                rotating headers.

        Returns:
            (`None`)
        """
        workflow: ProjectScrapeWorkflow = WorkflowClassRegistry.get(
            source=bank,
            workflow_type=PROJECT_PAGE_WORKFLOW,
            data_request_client=data_request_client,
            pubsub_client=None,
            db_client=None
        )
        time.sleep(3)
        workflow.scrape_project_page(project_page_url)

    def test_project_partial_page_scrape(
        self, 
        bank: str,
        project_partial_page_url: str,
        data_request_client: DataRequestClient) -> None:
        """Asserts that scraping a project page for partial
        data does not result in an exception. Sleeps for three
        seconds in between HTTP calls to avoid potential 
        throttling by the bank website.
        
        Args:
            bank (`str`): The abbreviation for the bank or
                financial institution (e.g., "AFDB").

            project_partial_page_url (`str`): A URL to a
                project page on the bank's website.

            data_request_client (`DataRequestClient`): An instance
                of a client used to make HTTP requests while 
                rotating headers.

        Returns:
            (`None`)
        """
        workflow: ProjectPartialScrapeWorkflow = WorkflowClassRegistry.get(
            source=bank,
            workflow_type=PROJECT_PARTIAL_PAGE_WORKFLOW,
            data_request_client=data_request_client,
            pubsub_client=None,
            db_client=None
        )
        time.sleep(3)
        workflow.scrape_project_page(project_partial_page_url)

    def test_result_page_multi_scrape(
        self, 
        bank: str,
        results_page_url: str,
        data_request_client: DataRequestClient) -> None:
        """Asserts that scraping a project search results
        page to find project page URLs and partial project
        data does not result in an exception. Sleeps for 
        three seconds in between calls to avoid potential 
        throttling by the bank website.
        
        Args:
            bank (`str`): The abbreviation for the bank or
                financial institution (e.g., "AFDB").

            results_page_url (`str`): A URL to a project 
                results page on the bank's website.

            data_request_client (`DataRequestClient`): An instance
                of a client used to make HTTP requests while 
                rotating headers.

        Returns:
            (`None`)
        """
        workflow: ResultsMultiScrapeWorkflow = WorkflowClassRegistry.get(
            source=bank,
            workflow_type=RESULTS_PAGE_MULTISCRAPE_WORKFLOW,
            data_request_client=data_request_client,
            pubsub_client=None,
            db_client=None
        )
        time.sleep(3)
        workflow.scrape_results_page(results_page_url)

    def test_result_page_scrape(
        self, 
        bank: str,
        results_page_url: str,
        data_request_client: DataRequestClient) -> None:
        """Asserts that scraping a project search results
        page to generate project page URLs does not result
        in an exception. Sleeps for three seconds in between
        calls to avoid potential throttling by the bank website.
        
        Args:
            bank (`str`): The abbreviation for the bank or
                financial institution (e.g., "AFDB").

            results_page_url (`str`): A URL to a project 
                results page on the bank's website.

            data_request_client (`DataRequestClient`): An instance
                of a client used to make HTTP requests while 
                rotating headers.

        Returns:
            (`None`)
        """
        workflow: ResultsScrapeWorkflow = WorkflowClassRegistry.get(
            source=bank,
            workflow_type=RESULTS_PAGE_WORKFLOW,
            data_request_client=data_request_client,
            pubsub_client=None,
            db_client=None
        )
        time.sleep(3)
        workflow.scrape_results_page(results_page_url)
