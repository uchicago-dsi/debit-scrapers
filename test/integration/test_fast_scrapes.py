"""Tests the fastest bank scrapers in the project"""

import json

import yaml

from scrapers.banks.aiib import AiibProjectScrapeWorkflow, AiibSeedUrlsWorkflow
from scrapers.banks.bio import (
    BioProjectPartialScrapeWorkflow,
    BioResultsMultiScrapeWorkflow,
    BioSeedUrlsWorkflow,
)
from scrapers.banks.deg import DegDownloadWorkflow
from scrapers.banks.dfc import DfcDownloadWorkflow
from scrapers.banks.fmo import (
    FmoProjectScrapeWorkflow,
    FmoResultsScrapeWorkflow,
    FmoSeedUrlsWorkflow,
)
from scrapers.banks.kfw import KfwDownloadWorkflow
from scrapers.constants import CONFIG_DIR_PATH
from scrapers.services.data_request import DataRequestClient


def test_kfw() -> None:
    """Test KfW scrape"""
    # Test 'DownloadWorkflow'
    w = KfwDownloadWorkflow(None, None, None)
    raw_df = w.get_projects()
    clean_df = w.clean_projects(raw_df)
    print(f"Found {len(clean_df)} record(s).")
    print(clean_df.head())


def test_aiib() -> None:
    """Test aiib scrape"""
    # Set up DataRequestClient to rotate HTTP headers and add random delays
    with open(
        f"{CONFIG_DIR_PATH}/user_agent_headers.json", "r", encoding="utf-8"
    ) as stream:
        try:
            user_agent_headers = json.load(stream)
            data_request_client = DataRequestClient(user_agent_headers)
        except yaml.YAMLError as yml_err:
            raise RuntimeError(
                f"Failed to open configuration file. {yml_err}"
            ) from yml_err

    # Test 'SeedUrlsWorkflow'
    seed_workflow = AiibSeedUrlsWorkflow(None, None, None)
    print(seed_workflow.generate_seed_urls())

    # Test 'ProjectScrapeWorkflow'
    scrape_workflow = AiibProjectScrapeWorkflow(data_request_client, None, None)
    url = (
        "https://www.aiib.org/en/projects/details/2021/proposed"
        "/India-Extension-Renovation-and-Modernization-of-Grand-Anicut-"
        "Canal-System.html"
    )
    print(scrape_workflow.scrape_project_page(url))


def test_bio() -> None:
    """Test scrape for bio"""
    # Test 'StartScrape' workflow
    seed_workflow = BioSeedUrlsWorkflow(None, None, None)
    print(seed_workflow.generate_seed_urls())

    # Test 'ResultsPageMultiScrape' workflow
    res_multi_scrape_workflow = BioResultsMultiScrapeWorkflow(None, None, None, None)
    url = "https://www.bio-invest.be/en/investments/p5?search="
    urls, project_records = res_multi_scrape_workflow.scrape_results_page(url)
    print(urls)
    print(project_records)

    # Test 'ProjectPartialScrapeWorkflow' workflow
    proj_partial_scrape_workflow = BioProjectPartialScrapeWorkflow(None, None, None)
    url = "https://www.bio-invest.be/en/investments/zoscales-fund-i"
    print(proj_partial_scrape_workflow.scrape_project_page(url))


def test_deg() -> None:
    """Test deg workflow"""
    # Test 'DownloadWorkflow'
    w = DegDownloadWorkflow(None, None, None)
    raw_df = w.get_projects()
    clean_df = w.clean_projects(raw_df)
    print(f"Found {len(clean_df)} record(s).")
    print(clean_df.head())


def test_dfc() -> None:
    """Test dfc scrape workflow"""
    # Set up DataRequestClient to rotate HTTP headers and add random delays
    with open(
        f"{CONFIG_DIR_PATH}/user_agent_headers.json", "r", encoding="utf-8"
    ) as stream:
        try:
            user_agent_headers = json.load(stream)
            data_request_client = DataRequestClient(user_agent_headers)
        except yaml.YAMLError as yml_err:
            raise RuntimeError(
                f"Failed to open configuration file. {yml_err}"
            ) from yml_err

    # Test 'DownloadWorkflow'
    w = DfcDownloadWorkflow(data_request_client, None, None)
    raw_df = w.get_projects()
    clean_df = w.clean_projects(raw_df)
    print(f"Found {len(clean_df)} record(s).")
    print(clean_df.head())


def test_fmo() -> None:
    """Test workflow for fmo scrape"""
    # Test 'StartScrape' workflow
    seed_workflow = FmoSeedUrlsWorkflow(None, None, None)
    print(seed_workflow.generate_seed_urls())

    # Test 'ResultsPageScrape' workflow
    res_scrape_workflow = FmoResultsScrapeWorkflow(None, None, None, None)
    url = "https://www.fmo.nl/worldmap?page=21"
    print(res_scrape_workflow.scrape_results_page(url))

    # Test 'ProjectPageScrape' workflow
    proj_scrape_workflow = FmoProjectScrapeWorkflow(None, None, None)
    url = "https://www.fmo.nl/project-detail/60377"
    print(proj_scrape_workflow.scrape_project_page(url))
