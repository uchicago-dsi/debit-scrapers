import json

import yaml

from scrapers.banks.adb import (
    AdbProjectScrapeWorkflow,
    AdbResultsScrapeWorkflow,
    AdbSeedUrlsWorkflow,
)
from scrapers.banks.afdb import AfdbProjectScrapeWorkflow, AfdbSeedUrlsWorkflow
from scrapers.banks.idb import (
    IdbProjectScrapeWorkflow,
    IdbResultsScrapeWorkflow,
    IdbSeedUrlsWorkflow,
)
from scrapers.banks.pro import ProProjectScrapeWorkflow, ProSeedUrlsWorkflow
from scrapers.banks.wb import WbDownloadWorkflow
from scrapers.constants import CONFIG_DIR_PATH
from scrapers.services.data_request import DataRequestClient


def test_adb_seed() -> None:
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
    seed_workflow = AdbSeedUrlsWorkflow(None, None, None)
    print(seed_workflow.generate_seed_urls())
    assert seed_workflow.generate_seed_urls()


def test_adb_result_scrape() -> None:
    """Test scrape process for adb"""
    with open(
        f"{CONFIG_DIR_PATH}/user_agent_headers.json", "r", encoding="utf-8"
    ) as stream:
        try:
            user_agent_headers = json.load(stream)
            data_request_client = DataRequestClient(user_agent_headers)
        except yaml.YAMLError as exc:
            raise RuntimeError(f"Failed to open config file. {exc}") from exc

    # Test 'ResultsScrapeWorkflow'
    res_scrape_workflow = AdbResultsScrapeWorkflow(data_request_client, None, None, None)
    url = "https://www.adb.org/projects?page=558"
    project_page_urls: list[str] = res_scrape_workflow.scrape_results_page(url)

    print(project_page_urls)
    assert len(project_page_urls) > 0


def test_adb_project_scrape() -> None:
    """Test scrape process for adb"""
    with open(
        f"{CONFIG_DIR_PATH}/user_agent_headers.json", "r", encoding="utf-8"
    ) as stream:
        try:
            user_agent_headers = json.load(stream)
            data_request_client = DataRequestClient(user_agent_headers)
        except yaml.YAMLError as exc:
            raise RuntimeError(f"Failed to open config file. {exc}") from exc

    # Test 'ProjectScrapeWorkflow'
    proj_scrape_workflow = AdbProjectScrapeWorkflow(data_request_client, None, None)
    url = "https://www.adb.org/projects/53303-001/main"
    print(proj_scrape_workflow.scrape_project_page(url))

# BROKEN 403 -- detecting scraper?
def test_idb_seed() -> None:
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
    seed_workflow = IdbSeedUrlsWorkflow(None, None, None)
    print(seed_workflow.generate_seed_urls())


def test_idb_result_scrape() -> None:
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

    # Test 'ResultsScrapeWorkflow'
    res_scrape_workflow = IdbResultsScrapeWorkflow(data_request_client, None, None, None)
    url = (
        "https://www.iadb.org/en/projects-search"
        "?country=&sector=&status=&query=&page=120"
    )
    print(res_scrape_workflow.scrape_results_page(url))


def test_idb_project_scrape() -> None:
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

    # Test 'ProjectScrapeWorkflow'
    proj_scrape_workflow = IdbProjectScrapeWorkflow(data_request_client, None, None)
    url = "https://www.iadb.org/en/project/TC9409295"
    print(proj_scrape_workflow.scrape_project_page(url))


def test_pro_seed() -> None:
    """test workflow for pro scrape"""
    # Test 'StartScrapeWorkflow'
    seed_workflow = ProSeedUrlsWorkflow(None, None, None)
    print(seed_workflow.generate_seed_urls())


def test_pro_scrape() -> None:
    # Test 'ProjectScrapeWorkflow'
    scrape_workflow = ProProjectScrapeWorkflow(None, None, None)
    url = "https://www.proparco.fr/en/carte-des-projets/ecobank-trade-finance"
    print(scrape_workflow.scrape_project_page(url))


def test_wb() -> None:
    """test workflow for wb scrape"""
    # Test 'DownloadWorkflow'
    download_workflow = WbDownloadWorkflow(None, None, None)
    raw_df = download_workflow.get_projects()
    clean_df = download_workflow.clean_projects(raw_df)
    print(f"Found {len(clean_df)} record(s).")
    print(clean_df.head())


def test_afdb_seed() -> None:
    """Test afdb scrape"""
    # Test 'SeedUrlsWorkflow'
    # NOTE: Performs a download that takes
    # several seconds to complete.
    seed_workflow = AfdbSeedUrlsWorkflow(None, None, None)
    print(seed_workflow.generate_seed_urls())


def test_afdb_scrape_project() -> None:
    """Test afdb scrape"""
    # Test 'ProjectScrapeWorkflow'
    scrape_workflow = AfdbProjectScrapeWorkflow(None, None, None)
    url = ("https://projectsportal.afdb.org"
    "/dataportal/VProject/show/P-Z1-FAB-030")
    print(scrape_workflow.scrape_project_page(url))