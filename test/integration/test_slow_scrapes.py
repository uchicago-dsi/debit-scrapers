import json

import yaml

from scrapers.banks.afdb import AfdbProjectScrapeWorkflow, AfdbSeedUrlsWorkflow
from scrapers.banks.ebrd import (
    EbrdProjectScrapeWorkflow,
    EbrdResultsScrapeWorkflow,
    EbrdSeedUrlsWorkflow,
)
from scrapers.banks.eib import EibProjectScrapeWorkflow, EibSeedUrlsWorkflow
from scrapers.banks.ifc import IfcProjectScrapeWorkflow, IfcSeedUrlsWorkflow
from scrapers.banks.miga import (
    MigaProjectScrapeWorkflow,
    MigaResultsScrapeWorkflow,
    MigaSeedUrlsWorkflow,
)
from scrapers.banks.nbim import NbimDownloadWorkflow
from scrapers.banks.undp import UndpProjectScrapeWorkflow, UndpSeedUrlsWorkflow
from scrapers.constants import CONFIG_DIR_PATH
from scrapers.services.data_request import DataRequestClient


def test_afdb() -> None:
    """Test afdb scrape"""
    # Test 'SeedUrlsWorkflow'
    # NOTE: Performs a download that takes
    # several seconds to complete.
    w = AfdbSeedUrlsWorkflow(None, None, None)
    print(w.generate_seed_urls())

    # Test 'ProjectScrapeWorkflow'
    w = AfdbProjectScrapeWorkflow(None, None, None)
    url = "https://projectsportal.afdb.org"
    "/dataportal/VProject/show/P-Z1-FAB-030"
    print(w.scrape_project_page(url))


def test_ebrd() -> None:
    """Test workflow for ebfd"""
    # Test 'StartScrape' workflow
    w = EbrdSeedUrlsWorkflow(None, None, None)
    print(w.generate_seed_urls())

    # # Test 'ResultsPageScrape' workflow
    w = EbrdResultsScrapeWorkflow(None, None, None, None)
    url = (
        "https://www.ebrd.com/cs/Satellite"
        "?c=Page&cid=1395238314964&d=&pagename=EBRD"
        "/Page/SolrSearchAndFilterPSD&page=65&safSortBy=PublicationDate_sort"
        "&safSortOrder=descending"
    )
    print(w.scrape_results_page(url))

    # Test 'ProjectPageScrape' workflow
    w = EbrdProjectScrapeWorkflow(None, None, None)
    url = "https://www.ebrd.com/work-with-us/projects/psd/52642.html"
    print(w.scrape_project_page(url))


def test_eib() -> None:
    """Test workflow for eib"""
    # Test 'SeedUrlsWorkflow'
    w = EibSeedUrlsWorkflow(None, None, None)
    print(w.generate_seed_urls())

    # Test 'ProjectScrapeWorkflow'
    w = EibProjectScrapeWorkflow(None, None, None)
    url = (
        "https://www.eib.org/page-provider/projects/list"
        "?pageNumber=17&itemPerPage=500&pageable=true&sortColumn=id"
    )
    print(w.scrape_project_page(url))


def test_ifc() -> None:
    """Test workflow for ifc scrape"""
    # Test 'SeedUrlsWorkflow'
    w = IfcSeedUrlsWorkflow(None, None, None)
    print(w.generate_seed_urls())

    # Test 'ProjectScrapeWorkflow'
    w = IfcProjectScrapeWorkflow(None, None, None)
    url = (
        "https://externalsearch.ifc.org/spi/api/searchxls"
        "?qterm=*&start=8000&srt=disclosed_date&order=desc&rows=1000"
    )
    records = w.scrape_project_page(url)
    print(records)
    print(f"Found {len(records)} record(s).")


def test_miga() -> None:
    """Test workflow for miga scrape"""
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
    w = MigaSeedUrlsWorkflow(None, None, None)
    print(w.generate_seed_urls())

    # Test 'ResultsScrapeWorkflow'
    w = MigaResultsScrapeWorkflow(data_request_client, None, None, None)
    url = "https://www.miga.org/projects?page=1"
    print(w.scrape_results_page(url))

    # Test 'ProjectScrapeWorkflow'
    w = MigaProjectScrapeWorkflow(data_request_client, None, None)
    url = "https://www.miga.org/project/bboxx-rwanda-kenya-and-drc-0"
    print(w.scrape_project_page(url))


def test_nbim() -> None:
    """test workflow for nbim scrape"""
    # Test 'DownloadWorkflow'
    w = NbimDownloadWorkflow(None, None, None)
    raw_df = w.get_projects()
    clean_df = w.clean_projects(raw_df)
    print(f"Found {len(clean_df)} record(s).")
    print(clean_df.head())


def test_undp() -> None:
    """test workflow for undp scrape"""
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
    w = UndpSeedUrlsWorkflow(None, None, None)
    print(w.generate_seed_urls())

    # Test 'ProjectPageScrapeWorkflow'
    w = UndpProjectScrapeWorkflow(data_request_client, None, None)
    url = "https://api.open.undp.org/api/projects/00110684.json"
    print(w.scrape_project_page(url))
