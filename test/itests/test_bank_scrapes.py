"""Tests all of the bank scrapers in the project"""

import unittest
import json
import yaml
from scrapers.constants import CONFIG_DIR_PATH
from scrapers.services.data_request import DataRequestClient
from scrapers.banks.adb import (
    AdbSeedUrlsWorkflow, AdbResultsScrapeWorkflow, AdbProjectScrapeWorkflow
)
from scrapers.banks.kfw import (
    KfwDownloadWorkflow
)



class TestBankScrapes(unittest.TestCase):
    """Tests all of the bank scrapers in the project
    """

    @unittest.skip('Test is breaking, needs some more work')
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

if __name__ == '__main__':
    unittest.main()
