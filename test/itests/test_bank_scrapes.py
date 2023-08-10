"""Tests all of the bank scrapers in the project"""

import unittest
import json
import yaml
from scrapers.constants import CONFIG_DIR_PATH
from scrapers.services.data_request import DataRequestClient
from scrapers.banks.adb import (
    AdbSeedUrlsWorkflow, AdbResultsScrapeWorkflow, AdbProjectScrapeWorkflow,
)
from scrapers.banks.kfw import KfwDownloadWorkflow
from scrapers.banks.afdb import AfdbSeedUrlsWorkflow, AfdbProjectScrapeWorkflow
from scrapers.banks.aiib import AiibSeedUrlsWorkflow, AiibProjectScrapeWorkflow
from scrapers.banks.bio import BioSeedUrlsWorkflow, BioResultsMultiScrapeWorkflow, BioProjectPartialScrapeWorkflow
from scrapers.banks.deg import DegDownloadWorkflow
from scrapers.banks.dfc import DfcDownloadWorkflow
from scrapers.banks.ebrd import EbrdSeedUrlsWorkflow, EbrdResultsScrapeWorkflow, EbrdProjectScrapeWorkflow
from scrapers.banks.eib import EibSeedUrlsWorkflow, EibProjectScrapeWorkflow


#TODO problem is these tests literally run the entire scrape. Can we do quick checks faster?
# For now I'm just going to add skips on everything but the fast ones

class TestBankScrapes(unittest.TestCase):
    """Tests all of the bank scrapers in the project
    """

    @unittest.skip('BREAKING, needs some more work')
    def test_adb(self):
        """Test scrape process for adb
        """
        with open(f"{CONFIG_DIR_PATH}/user_agent_headers.json", "r", encoding="utf-8") as stream:
            try:
                user_agent_headers = json.load(stream)
                data_request_client = DataRequestClient(user_agent_headers)
            except yaml.YAMLError as exc:
                raise RuntimeError(f"Failed to open configuration file. {exc}") from exc

        # Test 'SeedUrlsWorkflow'
        w = AdbSeedUrlsWorkflow(None, None, None)
        print(w.generate_seed_urls())

        # Test 'ResultsScrapeWorkflow'
        w = AdbResultsScrapeWorkflow(data_request_client, None, None, None)
        url = 'https://www.adb.org/projects?page=558'
        project_page_urls = w.scrape_results_page(url)
        print(project_page_urls)

        # Test 'ProjectScrapeWorkflow'
        w = AdbProjectScrapeWorkflow(data_request_client, None, None)
        url = 'https://www.adb.org/print/projects/53303-001/main'
        print(w.scrape_project_page(url))

    def test_kfw(self):
        """Test KfW scrape
        """
        # Test 'DownloadWorkflow'
        w = KfwDownloadWorkflow(None, None, None)
        raw_df = w.get_projects()
        clean_df = w.clean_projects(raw_df)
        print(f"Found {len(clean_df)} record(s).")
        print(clean_df.head())

    @unittest.skip("WORKING, but takes forever so not going to "
                   "run it right now. Might be worth a refactor to speed this test up")
    def test_afdb(self):
        """Test afdb scrape
        """
        # Test 'SeedUrlsWorkflow'
        # NOTE: Performs a download that takes
        # several seconds to complete.
        w = AfdbSeedUrlsWorkflow(None, None, None)
        print(w.generate_seed_urls())

        # Test 'ProjectScrapeWorkflow'
        w = AfdbProjectScrapeWorkflow(None, None, None)
        url = 'https://projectsportal.afdb.org/dataportal/VProject/show/P-Z1-FAB-030'
        print(w.scrape_project_page(url))

    def test_aiib(self):
        """Test aiib scrape
        """
        # Set up DataRequestClient to rotate HTTP headers and add random delays
        with open(f"{CONFIG_DIR_PATH}/user_agent_headers.json", "r", encoding="utf-8") as stream:
            try:
                user_agent_headers = json.load(stream)
                data_request_client = DataRequestClient(user_agent_headers)
            except yaml.YAMLError as yml_err:
                raise RuntimeError(f"Failed to open configuration file. {yml_err}") from yml_err

        # Test 'SeedUrlsWorkflow'
        w = AiibSeedUrlsWorkflow(None, None, None)
        print(w.generate_seed_urls())

        # Test 'ProjectScrapeWorkflow'
        w = AiibProjectScrapeWorkflow(data_request_client, None, None)
        url = (
                "https://www.aiib.org/en/projects/details/2021/proposed"
                "/India-Extension-Renovation-and-Modernization-of-Grand-Anicut-Canal-System.html"
        )
        print(w.scrape_project_page(url))

    def test_bio(self):
        """Test scrape for bio
        """
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

    def test_deg(self):
        """Test deg workflow
        """
        # Test 'DownloadWorkflow'
        w = DegDownloadWorkflow(None, None, None)
        raw_df = w.get_projects()
        clean_df = w.clean_projects(raw_df)
        print(f"Found {len(clean_df)} record(s).")
        print(clean_df.head())

    def test_dfc(self):
        """Test dfc scrape workflow
        """
        # Set up DataRequestClient to rotate HTTP headers and add random delays
        with open(f"{CONFIG_DIR_PATH}/user_agent_headers.json", "r", encoding="utf-8") as stream:
            try:
                user_agent_headers = json.load(stream)
                data_request_client = DataRequestClient(user_agent_headers)
            except yaml.YAMLError as yml_err:
                raise RuntimeError(f"Failed to open configuration file. {yml_err}") from yml_err

        # Test 'DownloadWorkflow'
        w = DfcDownloadWorkflow(data_request_client, None, None)
        raw_df = w.get_projects()
        clean_df = w.clean_projects(raw_df)
        print(f"Found {len(clean_df)} record(s).")
        print(clean_df.head())

    @unittest.skip("WORKING but taking a while")
    def test_ebrd(self):
        """Test workflow for ebfd
        """
        # Test 'StartScrape' workflow
        w = EbrdSeedUrlsWorkflow(None, None, None)
        print(w.generate_seed_urls())

        # # Test 'ResultsPageScrape' workflow
        w = EbrdResultsScrapeWorkflow(None, None, None, None)
        url = (
            'https://www.ebrd.com/cs/Satellite?c=Page&cid=1395238314964&d=&pagename=EBRD'
            '/Page/SolrSearchAndFilterPSD&page=65&safSortBy=PublicationDate_sort'
            '&safSortOrder=descending'
        )
        print(w.scrape_results_page(url))

        # Test 'ProjectPageScrape' workflow
        w = EbrdProjectScrapeWorkflow(None, None, None)
        url = 'https://www.ebrd.com/work-with-us/projects/psd/52642.html'
        print(w.scrape_project_page(url))

    @unittest.skip("WORKING but taking a long time")
    def test_eib(self):
        """Test workflow for eib
        """
        # Test 'SeedUrlsWorkflow'
        w = EibSeedUrlsWorkflow(None, None, None)
        print(w.generate_seed_urls())

        # Test 'ProjectScrapeWorkflow'
        w = EibProjectScrapeWorkflow(None, None, None)
        url = (
            'https://www.eib.org/page-provider/projects/list'
            '?pageNumber=17&itemPerPage=500&pageable=true&sortColumn=id'
        )
        print(w.scrape_project_page(url))

if __name__ == '__main__':
    unittest.main()
