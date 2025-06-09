"""Integration tests for the form scrapers.
"""

import yaml
from pipeline.scrapers.forms import (
    Form13FArchiveScrapeWorkflow,
    Form13FHistoryScrapeWorkflow,
    Form13FInvestmentScrapeWorkflow,
    Form13FSeedUrlsWorkflow
)
from pipeline.constants import TESTS_DIR_PATH
from pipeline.services.web import DataRequestClient
from pipeline.tests.integration.common import ScraperFixturesMixin
from typing import Dict, List

def load_url_params(file_name: str, key: str) -> Dict[str, List[str]]:
    """Loads bank URL parameters from a YAML file for testing.

    Args:
        file_name (`str`): The name of the yaml file. 
            Expected to exist in the `params` folder 
            within the configured test directory.

        key (`str): The name of the parameter to load
            from the file (e.g., "form_urls").

    Returns:
        (`list` of `list` of `str`): The parameters to use for
            testing. Consists of a list of lists where each
            item contains the name of the bank followed
            by the URL to request and scrape. For example:
            `[["afdb", "https://<...>"]]`.
    """
    with open(f"{TESTS_DIR_PATH}/params/{file_name}") as f:
        form_params = yaml.safe_load(f)
        return [
            url 
            for category, urls in form_params.items()
            for url in urls
            if category == key
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
    # Form 13F seed URLs workflow
    if metafunc.function == TestForm13F.test_generate_seed_urls:
        pass

    # Form 13F history scrape workflow
    elif metafunc.function == TestForm13F.test_scrape_filing_history:
        params = load_url_params("form13f.yaml", "history_urls")
        metafunc.parametrize("url", params)

    # Form 13F archived history scrape workflow
    elif metafunc.function == TestForm13F.test_parse_archived_submissions:
        params = load_url_params("form13f.yaml", "archived_urls")
        metafunc.parametrize("url", params)

    # Form 13F investment scrape workflow
    elif metafunc.function == TestForm13F.test_scrape_investments:
        params = load_url_params("form13f.yaml", "form_urls")
        metafunc.parametrize("url", params)

    else:
        raise Exception(f"The current function, \"{metafunc.function}\" "
                        "has not been configured for dynamic parametrization.")
   
class TestForm13F(ScraperFixturesMixin):
    """Tests workflows for U.S. S.E.C. Form 13F-HR.
    """

    def test_generate_seed_urls(
        self, 
        data_request_client: DataRequestClient) -> None:
        """Asserts that generating the first set of URLs to
        scrape does not result in an exception.
        
        Args:
            data_request_client (`DataRequestClient`): An instance
                of a client used to make HTTP requests while 
                rotating headers.

        Returns:
            `None`
        """
        workflow = Form13FSeedUrlsWorkflow(
            data_request_client=data_request_client,
            pubsub_client=None,
            db_client=None,
            logger=None
        )
        workflow.generate_seed_urls()

    def test_scrape_filing_history(
        self,
        url: str,
        data_request_client: DataRequestClient) -> None:
        """Asserts that parsing Form 13F submission 
        URLs from a company's S.E.C. submission history
        does not result in an exception.
        
        Args:
            url (`str`): The URL to a company's SEC
                submission history.

            data_request_client (`DataRequestClient`): An instance
                of a client used to make HTTP requests while 
                rotating headers.

        Returns:
            `None`
        """
        workflow = Form13FHistoryScrapeWorkflow(
            data_request_client=data_request_client,
            pubsub_client=None,
            db_client=None,
            logger=None
        )
        workflow.scrape_filing_history(url)

    def test_parse_archived_submissions(
        self,
        url: str,
        data_request_client: DataRequestClient) -> None:
        """Asserts that scraping and parsing URLs for
        a company's archived Form 13F filings does not
        result in an exception.
        
        Args:
            url (`str`): The URL to a company's archived
                S.E.C. submission history.

            data_request_client (`DataRequestClient`): An instance
                of a client used to make HTTP requests while 
                rotating headers.

        Returns:
            `None`
        """
        workflow = Form13FArchiveScrapeWorkflow(
            data_request_client=data_request_client,
            pubsub_client=None,
            db_client=None,
            logger=None
        )
        workflow.parse_archived_submissions(url)

    def test_scrape_investments(
        self,
        url: str,
        data_request_client: DataRequestClient) -> None:
        """Asserts that scraping data from a Form 13F-HR
        filing does not result in an exception.
        
        Args:
            url (`str`): The URL to the form home page.

            data_request_client (`DataRequestClient`): An instance
                of a client used to make HTTP requests while 
                rotating headers.

        Returns:
            `None`
        """
        workflow = Form13FInvestmentScrapeWorkflow(
            data_request_client=data_request_client,
            db_client=None,
            logger=None
        )
        workflow.scrape_investments(url)
