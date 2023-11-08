"""Unit tests for the web scrapers.
"""

import pytest
import json
import yaml
from scrapers.abstract.project_download_workflow import ProjectDownloadWorkflow
from scrapers.abstract.seed_urls_workflow import SeedUrlsWorkflow
from scrapers.constants import (
    CONFIG_DIR_PATH,
    DOWNLOAD_WORKFLOW,
    SEED_URLS_WORKFLOW
)
from scrapers.services.data_request import DataRequestClient
from scrapers.services.registry import (
    StarterWorkflowRegistry,
    WorkflowClassRegistry
)
from typing import List


def pytest_generate_tests(metafunc) -> None:
    """Dynamically supplies parameters for a test class.

    References:
    - https://stackoverflow.com/a/69285727
    - https://pytest-with-eric.com/introduction/pytest-generate-tests/

    Args:
        metafunc: The current test class instance.

    Returns:
        None
    """
    if metafunc.cls == TestSeedUrlsWorkflow:
        params = StarterWorkflowRegistry.list(
            workflow_type=SEED_URLS_WORKFLOW
        )
    elif metafunc.cls == TestDownloadWorkflow:
        params = StarterWorkflowRegistry.list(
            workflow_type=DOWNLOAD_WORKFLOW
        )
    else:
        params = None
    
    metafunc.parametrize("bank", params)


class ScraperFixturesMixin:
    """A mixin providing fixtures for scraper tests.
    """

    @pytest.fixture()
    def user_agent_headers(self) -> List[str]:
        """User agent HTTP headers to use to avoid throttling.
        """
        with open(f"{CONFIG_DIR_PATH}/user_agent_headers.json", "r") as stream:
            try:
                return json.load(stream)
            except yaml.YAMLError as e:
                raise RuntimeError("Failed to open configuration "
                                   "file."f" {e}") from None

    @pytest.fixture()
    def data_request_client(
        self, 
        user_agent_headers: List[str]) -> DataRequestClient:
        """Client to make HTTP requests while rotating headers.
        """
        return DataRequestClient(user_agent_headers)
    
class TestSeedUrlsWorkflow(ScraperFixturesMixin):
    """Tests the seed URLs workflow across all banks.
    """

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
            bank_abbr=bank,
            workflow_type=SEED_URLS_WORKFLOW,
            data_request_client=data_request_client,
            pubsub_client=None,
            db_client=None
        )
        workflow.generate_seed_urls()

class TestDownloadWorkflow(ScraperFixturesMixin):
    """Tests the download workflow across all banks.
    """

    def test_download_workflow(
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
            bank_abbr=bank,
            workflow_type=DOWNLOAD_WORKFLOW,
            data_request_client=data_request_client,
            pubsub_client=None,
            db_client=None
        )
        workflow.get_projects()
    