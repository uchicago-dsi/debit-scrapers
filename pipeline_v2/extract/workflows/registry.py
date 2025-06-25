"""Provides mappings of web scraping workflow names to class types."""

# Standard library imports
from typing import List, Optional

# Third-party imports
from django.conf import settings

# Application imports
from common.logger import LoggerFactory
from common.pubsub import PublisherClient
from common.web import DataRequestClient
from extract.dal import ExtractionDbClient
from extract.workflows.abstract import BaseWorkflow
from extract.workflows.banks import *


class StarterWorkflowRegistry:
    """Provides methods for fetching the names of starter
    workflows for banks and other financial institutions.
    """

    _REGISTRY = {
        settings.ADB_ABBREVIATION: settings.SEED_URLS_WORKFLOW,
        settings.AFDB_ABBREVIATION: settings.PROJECT_PARTIAL_DOWNLOAD_WORKFLOW,
        settings.AIIB_ABBREVIATION: settings.SEED_URLS_WORKFLOW,
        settings.BIO_ABBREVIATION: settings.SEED_URLS_WORKFLOW,
        settings.DEG_ABBREVIATION: settings.PROJECT_DOWNLOAD_WORKFLOW,
        settings.DFC_ABBREVIATION: settings.PROJECT_DOWNLOAD_WORKFLOW,
        settings.EBRD_ABBREVIATION: settings.PROJECT_PARTIAL_DOWNLOAD_WORKFLOW,
        settings.EIB_ABBREVIATION: settings.SEED_URLS_WORKFLOW,
        settings.FMO_ABBREVIATION: settings.SEED_URLS_WORKFLOW,
        settings.IDB_ABBREVIATION: settings.PROJECT_DOWNLOAD_WORKFLOW,
        settings.IFC_ABBREVIATION: settings.SEED_URLS_WORKFLOW,
        settings.KFW_ABBREVIATION: settings.PROJECT_DOWNLOAD_WORKFLOW,
        settings.MIGA_ABBREVIATION: settings.SEED_URLS_WORKFLOW,
        settings.NBIM_ABBREVIATION: settings.PROJECT_DOWNLOAD_WORKFLOW,
        settings.PRO_ABBREVIATION: settings.SEED_URLS_WORKFLOW,
        settings.UNDP_ABBREVIATION: settings.SEED_URLS_WORKFLOW,
        settings.WB_ABBREVIATION: settings.PROJECT_DOWNLOAD_WORKFLOW,
    }

    def get(bank_abbr: str) -> str:
        """Fetches the name of the starter workflow for the given bank.

        Args:
            bank_abbr: The abbreviation used for the bank
                or financial institution.

        Raises:
            `ValueError` If the bank does not exist in the registry.

        Returns:
            The workflow name.
        """
        try:
            return StarterWorkflowRegistry._REGISTRY[bank_abbr]
        except KeyError as e:
            raise ValueError(
                f"Invalid starter workflow requested: {e}. "
                "All workflows must be properly "
                "registered."
            ) from None

    def exists(bank_abbr: str) -> bool:
        """Returns a boolean indicating whether the given
        bank has a registered starter workflow.

        Args:
            bank_abbr: The abbreviation used for the bank
                or financial institution.

        Returns:
            The boolean.
        """
        return bank_abbr in StarterWorkflowRegistry._REGISTRY

    def list(workflow_type: Optional[str] = None) -> List[str]:
        """Returns a list of registered banks,
        optionally filtered by workflow type.

        Args:
            workflow_type: The workflow to filter
                by. Defaults to `None`, in which case
                all workflows are returned.

        Returns:
            The list of bank names.
        """
        return [
            k
            for k, v in StarterWorkflowRegistry._REGISTRY.items()
            if not workflow_type or v == workflow_type
        ]


class WorkflowClassRegistry:
    """Provides methods for fetching and instantiating
    workflow classes by bank and workflow name.
    """

    _REGISTRY = {
        f"{settings.ADB_ABBREVIATION}-{settings.SEED_URLS_WORKFLOW}": AdbSeedUrlsWorkflow,
        f"{settings.ADB_ABBREVIATION}-{settings.RESULTS_PAGE_WORKFLOW}": AdbResultsScrapeWorkflow,
        f"{settings.ADB_ABBREVIATION}-{settings.PROJECT_PAGE_WORKFLOW}": AdbProjectScrapeWorkflow,
        f"{settings.AFDB_ABBREVIATION}-{settings.PROJECT_PARTIAL_DOWNLOAD_WORKFLOW}": AfdbProjectPartialDownloadWorkflow,
        f"{settings.AFDB_ABBREVIATION}-{settings.PROJECT_PARTIAL_PAGE_WORKFLOW}": AfdbProjectPartialScrapeWorkflow,
        f"{settings.AIIB_ABBREVIATION}-{settings.SEED_URLS_WORKFLOW}": AiibSeedUrlsWorkflow,
        f"{settings.AIIB_ABBREVIATION}-{settings.PROJECT_PAGE_WORKFLOW}": AiibProjectScrapeWorkflow,
        f"{settings.BIO_ABBREVIATION}-{settings.SEED_URLS_WORKFLOW}": BioSeedUrlsWorkflow,
        f"{settings.BIO_ABBREVIATION}-{settings.RESULTS_PAGE_MULTISCRAPE_WORKFLOW}": BioResultsMultiScrapeWorkflow,
        f"{settings.BIO_ABBREVIATION}-{settings.PROJECT_PARTIAL_PAGE_WORKFLOW}": BioProjectPartialScrapeWorkflow,
        f"{settings.DEG_ABBREVIATION}-{settings.PROJECT_DOWNLOAD_WORKFLOW}": DegDownloadWorkflow,
        f"{settings.DFC_ABBREVIATION}-{settings.PROJECT_DOWNLOAD_WORKFLOW}": DfcDownloadWorkflow,
        f"{settings.EBRD_ABBREVIATION}-{settings.PROJECT_PARTIAL_DOWNLOAD_WORKFLOW}": EbrdProjectPartialDownloadWorkflow,
        f"{settings.EBRD_ABBREVIATION}-{settings.PROJECT_PARTIAL_PAGE_WORKFLOW}": EbrdProjectPartialScrapeWorkflow,
        f"{settings.EIB_ABBREVIATION}-{settings.SEED_URLS_WORKFLOW}": EibSeedUrlsWorkflow,
        f"{settings.EIB_ABBREVIATION}-{settings.RESULTS_PAGE_MULTISCRAPE_WORKFLOW}": EibResultsMultiScrapeWorkflow,
        f"{settings.EIB_ABBREVIATION}-{settings.PROJECT_PARTIAL_PAGE_WORKFLOW}": EibProjectPartialScrapeWorkflow,
        f"{settings.FMO_ABBREVIATION}-{settings.SEED_URLS_WORKFLOW}": FmoSeedUrlsWorkflow,
        f"{settings.FMO_ABBREVIATION}-{settings.RESULTS_PAGE_WORKFLOW}": FmoResultsScrapeWorkflow,
        f"{settings.FMO_ABBREVIATION}-{settings.PROJECT_PAGE_WORKFLOW}": FmoProjectScrapeWorkflow,
        f"{settings.IDB_ABBREVIATION}-{settings.PROJECT_DOWNLOAD_WORKFLOW}": IdbProjectDownloadWorkflow,
        f"{settings.IFC_ABBREVIATION}-{settings.SEED_URLS_WORKFLOW}": IfcSeedUrlsWorkflow,
        f"{settings.IFC_ABBREVIATION}-{settings.PROJECT_PAGE_WORKFLOW}": IfcProjectScrapeWorkflow,
        f"{settings.KFW_ABBREVIATION}-{settings.PROJECT_DOWNLOAD_WORKFLOW}": KfwDownloadWorkflow,
        f"{settings.MIGA_ABBREVIATION}-{settings.SEED_URLS_WORKFLOW}": MigaSeedUrlsWorkflow,
        f"{settings.MIGA_ABBREVIATION}-{settings.RESULTS_PAGE_WORKFLOW}": MigaResultsScrapeWorkflow,
        f"{settings.MIGA_ABBREVIATION}-{settings.PROJECT_PAGE_WORKFLOW}": MigaProjectScrapeWorkflow,
        f"{settings.NBIM_ABBREVIATION}-{settings.PROJECT_DOWNLOAD_WORKFLOW}": NbimDownloadWorkflow,
        f"{settings.PRO_ABBREVIATION}-{settings.SEED_URLS_WORKFLOW}": ProSeedUrlsWorkflow,
        f"{settings.PRO_ABBREVIATION}-{settings.RESULTS_PAGE_WORKFLOW}": ProResultsScrapeWorkflow,
        f"{settings.PRO_ABBREVIATION}-{settings.PROJECT_PAGE_WORKFLOW}": ProProjectScrapeWorkflow,
        f"{settings.UNDP_ABBREVIATION}-{settings.SEED_URLS_WORKFLOW}": UndpSeedUrlsWorkflow,
        f"{settings.UNDP_ABBREVIATION}-{settings.RESULTS_PAGE_MULTISCRAPE_WORKFLOW}": UndpResultsMultiScrapeWorkflow,
        f"{settings.UNDP_ABBREVIATION}-{settings.PROJECT_PARTIAL_PAGE_WORKFLOW}": UndpProjectPartialScrapeWorkflow,
        f"{settings.WB_ABBREVIATION}-{settings.PROJECT_DOWNLOAD_WORKFLOW}": WbDownloadWorkflow,
    }

    @staticmethod
    def get(
        source: str,
        workflow_type: str,
        data_request_client: DataRequestClient,
        pubsub_client: PublisherClient,
        db_client: ExtractionDbClient,
    ) -> BaseWorkflow:
        """Fetches a workflow type from the registry and
        then instantiates with the correct parameters.

        Args:
            source: The abbreviation used for the data source.

            workflow_type: The name of the workflow.

            data_request_client: A client for making HTTP GET requests while
                adding random delays and rotating user agent headers.

            pubsub_client: The Google Pub/Sub client.

            db_client: The database client.

        Returns:
            A concrete instance of a workflow.
        """
        # Fetch workflow from registry
        try:
            key = f"{source}-{workflow_type}"
            workflow_cls = WorkflowClassRegistry._REGISTRY[key]
        except KeyError as e:
            raise ValueError(
                f"Invalid workflow requested: {e}. "
                "All scraping workflows must be properly "
                "registered."
            ) from None

        # Create logger for selected workflow
        logger = LoggerFactory.get(f"run-workflows - {source}")

        # Select correct keyword args for workflow initializer
        if workflow_type in (
            settings.PROJECT_DOWNLOAD_WORKFLOW,
            settings.PROJECT_PAGE_WORKFLOW,
            settings.PROJECT_PARTIAL_DOWNLOAD_WORKFLOW,
            settings.PROJECT_PARTIAL_PAGE_WORKFLOW,
        ):
            params = {
                "data_request_client": data_request_client,
                "db_client": db_client,
                "logger": logger,
            }
        elif workflow_type in (
            settings.RESULTS_PAGE_MULTISCRAPE_WORKFLOW,
            settings.RESULTS_PAGE_WORKFLOW,
            settings.SEED_URLS_WORKFLOW,
        ):
            params = {
                "data_request_client": data_request_client,
                "pubsub_client": pubsub_client,
                "db_client": db_client,
                "logger": logger,
            }
        else:
            raise ValueError(
                f'A workflow was improperly configured: "{workflow_type}".'
            )

        return workflow_cls(**params)
