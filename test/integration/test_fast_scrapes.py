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
from scrapers.banks.wb import WbDownloadWorkflow
from scrapers.constants import CONFIG_DIR_PATH
from scrapers.services.data_request import DataRequestClient


def test_kfw():
    """Test KfW scrape"""
    # Test 'DownloadWorkflow'
    w = KfwDownloadWorkflow(None, None, None)
    raw_df = w.get_projects()
    clean_df = w.clean_projects(raw_df)
    print(f"Found {len(clean_df)} record(s).")
    print(clean_df.head())


def test_aiib():
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
    w = AiibSeedUrlsWorkflow(None, None, None)
    print(w.generate_seed_urls())

    # Test 'ProjectScrapeWorkflow'
    w = AiibProjectScrapeWorkflow(data_request_client, None, None)
    url = (
        "https://www.aiib.org/en/projects/details/2021/proposed"
        "/India-Extension-Renovation-and-Modernization-of-Grand-Anicut-"
        "Canal-System.html"
    )
    print(w.scrape_project_page(url))


def test_bio():
    """Test scrape for bio"""
    # Test 'StartScrape' workflow
    w = BioSeedUrlsWorkflow(None, None, None)
    print(w.generate_seed_urls())

    # Test 'ResultsPageMultiScrape' workflow
    w = BioResultsMultiScrapeWorkflow(None, None, None, None)
    url = "https://www.bio-invest.be/en/investments/p5?search="
    urls, project_records = w.scrape_results_page(url)
    print(urls)
    print(project_records)

    # Test 'ProjectPartialScrapeWorkflow' workflow
    w = BioProjectPartialScrapeWorkflow(None, None, None)
    url = "https://www.bio-invest.be/en/investments/zoscales-fund-i"
    print(w.scrape_project_page(url))


def test_deg():
    """Test deg workflow"""
    # Test 'DownloadWorkflow'
    w = DegDownloadWorkflow(None, None, None)
    raw_df = w.get_projects()
    clean_df = w.clean_projects(raw_df)
    print(f"Found {len(clean_df)} record(s).")
    print(clean_df.head())


def test_dfc():
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


def test_fmo():
    """Test workflow for fmo scrape"""
    # Test 'StartScrape' workflow
    w = FmoSeedUrlsWorkflow(None, None, None)
    print(w.generate_seed_urls())

    # Test 'ResultsPageScrape' workflow
    w = FmoResultsScrapeWorkflow(None, None, None, None)
    url = "https://www.fmo.nl/worldmap?page=21"
    print(w.scrape_results_page(url))

    # Test 'ProjectPageScrape' workflow
    w = FmoProjectScrapeWorkflow(None, None, None)
    url = "https://www.fmo.nl/project-detail/60377"
    print(w.scrape_project_page(url))


def test_wb():
    """test workflow for wb scrape"""
    # Test 'DownloadWorkflow'
    w = WbDownloadWorkflow(None, None, None)
    raw_df = w.get_projects()
    clean_df = w.clean_projects(raw_df)
    print(f"Found {len(clean_df)} record(s).")
    print(clean_df.head())
