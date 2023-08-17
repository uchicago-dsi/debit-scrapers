import json

import yaml

from scrapers.banks.adb import (
    AdbProjectScrapeWorkflow,
    AdbResultsScrapeWorkflow,
    AdbSeedUrlsWorkflow,
)
from scrapers.banks.idb import (
    IdbProjectScrapeWorkflow,
    IdbResultsScrapeWorkflow,
    IdbSeedUrlsWorkflow,
)
from scrapers.banks.pro import ProProjectScrapeWorkflow, ProSeedUrlsWorkflow
from scrapers.constants import CONFIG_DIR_PATH
from scrapers.services.data_request import DataRequestClient


def test_adb():
    """Test scrape process for adb"""
    with open(
        f"{CONFIG_DIR_PATH}/user_agent_headers.json", "r", encoding="utf-8"
    ) as stream:
        try:
            user_agent_headers = json.load(stream)
            data_request_client = DataRequestClient(user_agent_headers)
        except yaml.YAMLError as exc:
            raise RuntimeError(f"Failed to open config file. {exc}") from exc

    # Test 'SeedUrlsWorkflow'
    w = AdbSeedUrlsWorkflow(None, None, None)
    print(w.generate_seed_urls())

    # Test 'ResultsScrapeWorkflow'
    w = AdbResultsScrapeWorkflow(data_request_client, None, None, None)
    url = "https://www.adb.org/projects?page=558"
    project_page_urls = w.scrape_results_page(url)
    print(project_page_urls)

    # Test 'ProjectScrapeWorkflow'
    w = AdbProjectScrapeWorkflow(data_request_client, None, None)
    url = "https://www.adb.org/print/projects/53303-001/main"
    print(w.scrape_project_page(url))


def test_idb():
    """Test workflow for idb scrape"""
    # Set up DataRequestClient to rotate HTTP headers and add random delays
    with open(
        f"{CONFIG_DIR_PATH}/user_agent_headers.json", "r", encoding="UTF-8"
    ) as stream:
        try:
            user_agent_headers = json.load(stream)
            data_request_client = DataRequestClient(user_agent_headers)
        except yaml.YAMLError as yml_err:
            raise RuntimeError(
                f"Failed to open configuration file. {yml_err}"
            ) from yml_err

    # Test 'SeedUrlsWorkflow'
    w = IdbSeedUrlsWorkflow(None, None, None)
    print(w.generate_seed_urls())

    # Test 'ResultsScrapeWorkflow'
    w = IdbResultsScrapeWorkflow(data_request_client, None, None, None)
    url = (
        "https://www.iadb.org/en/projects-search"
        "?country=&sector=&status=&query=&page=120"
    )
    print(w.scrape_results_page(url))

    # Test 'ProjectScrapeWorkflow'
    w = IdbProjectScrapeWorkflow(data_request_client, None, None)
    url = "https://www.iadb.org/en/project/TC9409295"
    print(w.scrape_project_page(url))


def test_pro():
    """test workflow for pro scrape"""
    # Test 'StartScrapeWorkflow'
    w = ProSeedUrlsWorkflow(None, None, None)
    print(w.generate_seed_urls())

    # Test 'ProjectScrapeWorkflow'
    w = ProProjectScrapeWorkflow(None, None, None)
    url = "https://www.proparco.fr/en/carte-des-projets/ecobank-trade-finance"
    print(w.scrape_project_page(url))
