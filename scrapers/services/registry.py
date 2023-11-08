"""Registers names for web scraping workflows.
"""

from scrapers.abstract.base_workflow import BaseWorkflow
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
from scrapers.services.data_request import DataRequestClient
from scrapers.services.database import DbClient
from scrapers.services.logger import LoggerFactory
from scrapers.services.pubsub import PubSubClient
from typing import Optional


class StarterWorkflowRegistry:
    """Provides methods for fetching the names of starter
    workflows for banks and other financial institutions.
    """

    _REGISTRY = {
        ADB_ABBREVIATION : SEED_URLS_WORKFLOW,
        AFDB_ABBREVIATION : SEED_URLS_WORKFLOW,
        AIIB_ABBREVIATION : SEED_URLS_WORKFLOW,
        BIO_ABBREVIATION : SEED_URLS_WORKFLOW,
        DEG_ABBREVIATION : DOWNLOAD_WORKFLOW,
        DFC_ABBREVIATION : DOWNLOAD_WORKFLOW,
        EBRD_ABBREVIATION : SEED_URLS_WORKFLOW,
        EIB_ABBREVIATION : SEED_URLS_WORKFLOW,
        FMO_ABBREVIATION : SEED_URLS_WORKFLOW,
        IDB_ABBREVIATION : SEED_URLS_WORKFLOW,
        IFC_ABBREVIATION : SEED_URLS_WORKFLOW,
        KFW_ABBREVIATION : DOWNLOAD_WORKFLOW,
        MIGA_ABBREVIATION : SEED_URLS_WORKFLOW,
        NBIM_ABBREVIATION : DOWNLOAD_WORKFLOW,
        PRO_ABBREVIATION : SEED_URLS_WORKFLOW,
        UNDP_ABBREVIATION : SEED_URLS_WORKFLOW,
        WB_ABBREVIATION : DOWNLOAD_WORKFLOW
    }

    def get(bank_abbr: str) -> str:
        """Fetches the name of the starter workflow for the given bank.

        Args:
            bank_abbr (`str`): The abbreviation used for the bank
                or financial institution.
        Raises:
            (`ValueError`): If the bank does not exist in the registry.

        Returns:
            (`str`): The workflow name.
        """
        try:
            return StarterWorkflowRegistry._REGISTRY[bank_abbr]
        except KeyError as e:
            raise ValueError(f"Invalid starter workflow requested: {e}. "
                             "All workflows must be properly "
                             "registered.") from None

    def exists(bank_abbr: str) -> bool:
        """Returns a boolean indicating whether the given
        bank has a registered starter workflow.

        Args:
            bank_abbr (`str`): The abbreviation used for the bank
                or financial institution.
        Returns:
            (`bool`): The boolean.
        """
        return bank_abbr in StarterWorkflowRegistry._REGISTRY
    
    def list(workflow_type: Optional[str]=None) -> List[str]:
        """Returns a list of registered banks,
        optionally filtered by workflow type.

        Args:
            workflow_type (`str`): The workflow to filter
                by. Defaults to `None`, in which case
                all workflows are returned.

        Returns:
            (`list` of `str`): The list of bank names.
        """
        return [
            k 
            for k, v in 
            StarterWorkflowRegistry._REGISTRY.items()
            if not workflow_type or v == workflow_type
        ]
    
class WorkflowClassRegistry:
    """Provides methods for fetching and instantiating
    workflow classes by bank and workflow name lookup.
    """

    _REGISTRY = {
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

    @staticmethod
    def get(
        bank_abbr: str,
        workflow_type: str,
        data_request_client: DataRequestClient,
        pubsub_client: PubSubClient,
        db_client: DbClient) -> BaseWorkflow:
        """Fetches a workflow type from the registry and
        then instantiates with the correct parameters.

        Args:
            bank_abbr (`str`): The abbreviation used for the bank
                or financial institution.

            workflow_type (`str`): The name of the workflow.

            data_request_client (`DataRequestClient`): A client
                for making HTTP GET requests while adding
                random delays and rotating user agent headers.

            pubsub_client (`PubSubClient`): The Google Pub/Sub client.

            db_client (`DbClient`): The database client.

        Returns:
            (`BaseWorkflow`): A concrete instance of a workflow.
        """
        # Fetch workflow from registry
        try:
            key = f"{bank_abbr}-{workflow_type}"
            workflow_cls = WorkflowClassRegistry._REGISTRY[key]
        except KeyError as e:
            raise ValueError(f"Invalid workflow requested: {e}. "
                             "All scraping workflows must be properly "
                             "registered.") from None
        
        # Create logger for selected workflow
        logger = LoggerFactory.get(f"run-workflows - {bank_abbr}")

        # Select correct keyword args for workflow initializer
        if workflow_type in (
            DOWNLOAD_WORKFLOW,
            PROJECT_PAGE_WORKFLOW,
            PROJECT_PARTIAL_PAGE_WORKFLOW
        ):
            params = {
                "data_request_client": data_request_client,
                "db_client": db_client,
                "logger": logger
            }
        elif workflow_type in (
            RESULTS_PAGE_MULTISCRAPE_WORKFLOW,
            RESULTS_PAGE_WORKFLOW
        ):
            params = {
                "data_request_client": data_request_client,
                "pubsub_client": pubsub_client,
                "db_client": db_client,
                "logger": logger
            }
        elif workflow_type in (
            SEED_URLS_WORKFLOW
        ):
            params = {
                "pubsub_client": pubsub_client,
                "db_client": db_client,
                "logger": logger
            }
        else:
            raise ValueError("A workflow was improperly "
                             f"configured: \"{workflow_type}\".")

        return workflow_cls(**params)
