"""
registry.py
"""

from scrapers.banks.adb import *
from scrapers.banks.afdb import *
from scrapers.banks.aiib import *
from scrapers.banks.bio import *
from scrapers.banks.deg import *
from scrapers.banks.dfc import *
from scrapers.banks.ebrd import *
from scrapers.banks.eib import *
from scrapers.banks.fmo import *
from scrapers.banks.idb import *
from scrapers.banks.ifc import *
from scrapers.banks.kfw import *
from scrapers.banks.miga import *
from scrapers.banks.nbim import *
from scrapers.banks.pro import *
from scrapers.banks.undp import *
from scrapers.banks.wb import *
from scrapers.constants import (
    ADB_ABBREVIATION, 
    AFDB_ABBREVIATION, 
    AIIB_ABBREVIATION,
    BIO_ABBREVIATION,
    DEG_ABBREVIATION,
    DFC_ABBREVIATION,
    DOWNLOAD_WORKFLOW,
    EBRD_ABBREVIATION,
    EIB_ABBREVIATION,
    FMO_ABBREVIATION,
    IDB_ABBREVIATION,
    IFC_ABBREVIATION,
    KFW_ABBREVIATION,
    MIGA_ABBREVIATION,
    NBIM_ABBREVIATION,
    PRO_ABBREVIATION,
    PROJECT_PAGE_WORKFLOW,
    PROJECT_PARTIAL_PAGE_WORKFLOW,
    RESULTS_PAGE_MULTISCRAPE_WORKFLOW,
    RESULTS_PAGE_WORKFLOW,
    SEED_URLS_WORKFLOW,
    UNDP_ABBREVIATION,
    WB_ABBREVIATION
)

scraper_registry = {
    f"{ADB_ABBREVIATION}-{SEED_URLS_WORKFLOW}": AdbSeedUrlsWorkflow,
    f"{ADB_ABBREVIATION}-{RESULTS_PAGE_WORKFLOW}": AdbResultsScrapeWorkflow,
    f"{ADB_ABBREVIATION}-{PROJECT_PAGE_WORKFLOW}": AdbProjectScrapeWorkflow,
    f"{AFDB_ABBREVIATION}-{SEED_URLS_WORKFLOW}": AfdbSeedUrlsWorkflow,
    f"{AFDB_ABBREVIATION}-{PROJECT_PAGE_WORKFLOW}": AfdbProjectScrapeWorkflow,
    f"{AIIB_ABBREVIATION}-{SEED_URLS_WORKFLOW}": AiibSeedUrlsWorkflow,
    f"{AIIB_ABBREVIATION}-{PROJECT_PAGE_WORKFLOW}": AiibProjectScrapeWorkflow,
    f"{BIO_ABBREVIATION}-{SEED_URLS_WORKFLOW}": BioSeedUrlsWorkflow,
    f"{BIO_ABBREVIATION}-{RESULTS_PAGE_MULTISCRAPE_WORKFLOW}": BioResultsMultiScrapeWorkflow,
    f"{BIO_ABBREVIATION}-{PROJECT_PARTIAL_PAGE_WORKFLOW}": BioProjectPartialScrapeWorkflow,
    f"{DEG_ABBREVIATION}-{DOWNLOAD_WORKFLOW}": DegDownloadWorkflow,
    f"{DFC_ABBREVIATION}-{DOWNLOAD_WORKFLOW}": DfcDownloadWorkflow,
    f"{EBRD_ABBREVIATION}-{SEED_URLS_WORKFLOW}": EbrdSeedUrlsWorkflow,
    f"{EBRD_ABBREVIATION}-{RESULTS_PAGE_WORKFLOW}": EbrdResultsScrapeWorkflow,
    f"{EBRD_ABBREVIATION}-{PROJECT_PAGE_WORKFLOW}": EbrdProjectScrapeWorkflow,
    f"{EIB_ABBREVIATION}-{SEED_URLS_WORKFLOW}": EibSeedUrlsWorkflow,
    f"{EIB_ABBREVIATION}-{PROJECT_PAGE_WORKFLOW}": EibProjectScrapeWorkflow,
    f"{FMO_ABBREVIATION}-{SEED_URLS_WORKFLOW}": FmoSeedUrlsWorkflow,
    f"{FMO_ABBREVIATION}-{RESULTS_PAGE_WORKFLOW}": FmoResultsScrapeWorkflow,
    f"{FMO_ABBREVIATION}-{PROJECT_PAGE_WORKFLOW}": FmoProjectScrapeWorkflow,
    f"{IDB_ABBREVIATION}-{SEED_URLS_WORKFLOW}": IdbSeedUrlsWorkflow,
    f"{IDB_ABBREVIATION}-{RESULTS_PAGE_WORKFLOW}": IdbResultsScrapeWorkflow,
    f"{IDB_ABBREVIATION}-{PROJECT_PAGE_WORKFLOW}": IdbProjectScrapeWorkflow,
    f"{IFC_ABBREVIATION}-{SEED_URLS_WORKFLOW}": IfcSeedUrlsWorkflow,
    f"{IFC_ABBREVIATION}-{PROJECT_PAGE_WORKFLOW}": IfcProjectScrapeWorkflow,
    f"{KFW_ABBREVIATION}-{DOWNLOAD_WORKFLOW}": KfwDownloadWorkflow,
    f"{MIGA_ABBREVIATION}-{SEED_URLS_WORKFLOW}": MigaSeedUrlsWorkflow,
    f"{MIGA_ABBREVIATION}-{RESULTS_PAGE_WORKFLOW}": MigaResultsScrapeWorkflow,
    f"{MIGA_ABBREVIATION}-{PROJECT_PAGE_WORKFLOW}": MigaProjectScrapeWorkflow,
    f"{NBIM_ABBREVIATION}-{DOWNLOAD_WORKFLOW}": NbimDownloadWorkflow,
    f"{PRO_ABBREVIATION}-{SEED_URLS_WORKFLOW}": ProSeedUrlsWorkflow,
    f"{PRO_ABBREVIATION}-{PROJECT_PAGE_WORKFLOW}": ProProjectScrapeWorkflow,
    f"{UNDP_ABBREVIATION}-{SEED_URLS_WORKFLOW}": UndpSeedUrlsWorkflow,
    f"{UNDP_ABBREVIATION}-{PROJECT_PAGE_WORKFLOW}": UndpProjectScrapeWorkflow,
    f"{WB_ABBREVIATION}-{DOWNLOAD_WORKFLOW}": WbDownloadWorkflow
}
