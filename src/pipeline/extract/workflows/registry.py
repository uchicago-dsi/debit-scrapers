"""Provides mappings of web scraping workflow names to class types."""

# Third-party imports
from django.conf import settings

# Application imports
from common.http import DataRequestClient
from common.logger import LoggerFactory
from common.tasks import MessageQueueClient
from extract.dal import DatabaseClient
from extract.workflows.abstract import BaseWorkflow
from extract.workflows import banks


class StarterWorkflowRegistry:
    """A registry of the first data extraction workflow for each source."""

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
        settings.IDB_ABBREVIATION: settings.PROJECT_PARTIAL_DOWNLOAD_WORKFLOW,
        settings.IFC_ABBREVIATION: settings.SEED_URLS_WORKFLOW,
        settings.KFW_ABBREVIATION: settings.PROJECT_DOWNLOAD_WORKFLOW,
        settings.MIGA_ABBREVIATION: settings.SEED_URLS_WORKFLOW,
        settings.NBIM_ABBREVIATION: settings.SEED_URLS_WORKFLOW,
        settings.PRO_ABBREVIATION: settings.SEED_URLS_WORKFLOW,
        settings.UNDP_ABBREVIATION: settings.PROJECT_PARTIAL_DOWNLOAD_WORKFLOW,
        settings.WB_ABBREVIATION: settings.PROJECT_DOWNLOAD_WORKFLOW,
    }

    @staticmethod
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

    @staticmethod
    def exists(bank_abbr: str) -> bool:
        """Indicates whether the given bank has a registered starter workflow.

        Args:
            bank_abbr: The abbreviation used for the bank
                or financial institution.

        Returns:
            The boolean.
        """
        return bank_abbr in StarterWorkflowRegistry._REGISTRY

    @staticmethod
    def list(workflow_type: str | None = None) -> list[str]:
        """Returns the registered banks, optionally filtered by workflow type.

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
    """The registry of all data extraction workflows."""

    _REGISTRY = {
        f"{settings.ADB_ABBREVIATION}-{settings.SEED_URLS_WORKFLOW}": banks.AdbSeedUrlsWorkflow,
        f"{settings.ADB_ABBREVIATION}-{settings.PROJECT_PAGE_WORKFLOW}": banks.AdbProjectScrapeWorkflow,
        f"{settings.AFDB_ABBREVIATION}-{settings.PROJECT_PARTIAL_DOWNLOAD_WORKFLOW}": banks.AfdbProjectPartialDownloadWorkflow,
        f"{settings.AFDB_ABBREVIATION}-{settings.PROJECT_PARTIAL_PAGE_WORKFLOW}": banks.AfdbProjectPartialScrapeWorkflow,
        f"{settings.AIIB_ABBREVIATION}-{settings.SEED_URLS_WORKFLOW}": banks.AiibSeedUrlsWorkflow,
        f"{settings.AIIB_ABBREVIATION}-{settings.PROJECT_PAGE_WORKFLOW}": banks.AiibProjectScrapeWorkflow,
        f"{settings.BIO_ABBREVIATION}-{settings.SEED_URLS_WORKFLOW}": banks.BioSeedUrlsWorkflow,
        f"{settings.BIO_ABBREVIATION}-{settings.RESULTS_PAGE_MULTISCRAPE_WORKFLOW}": banks.BioResultsMultiScrapeWorkflow,
        f"{settings.BIO_ABBREVIATION}-{settings.PROJECT_PARTIAL_PAGE_WORKFLOW}": banks.BioProjectPartialScrapeWorkflow,
        f"{settings.DEG_ABBREVIATION}-{settings.PROJECT_DOWNLOAD_WORKFLOW}": banks.DegDownloadWorkflow,
        f"{settings.DFC_ABBREVIATION}-{settings.PROJECT_DOWNLOAD_WORKFLOW}": banks.DfcDownloadWorkflow,
        f"{settings.EBRD_ABBREVIATION}-{settings.PROJECT_PARTIAL_DOWNLOAD_WORKFLOW}": banks.EbrdProjectPartialDownloadWorkflow,
        f"{settings.EBRD_ABBREVIATION}-{settings.PROJECT_PARTIAL_PAGE_WORKFLOW}": banks.EbrdProjectPartialScrapeWorkflow,
        f"{settings.EIB_ABBREVIATION}-{settings.SEED_URLS_WORKFLOW}": banks.EibSeedUrlsWorkflow,
        f"{settings.EIB_ABBREVIATION}-{settings.RESULTS_PAGE_MULTISCRAPE_WORKFLOW}": banks.EibResultsMultiScrapeWorkflow,
        f"{settings.EIB_ABBREVIATION}-{settings.PROJECT_PARTIAL_PAGE_WORKFLOW}": banks.EibProjectPartialScrapeWorkflow,
        f"{settings.FMO_ABBREVIATION}-{settings.SEED_URLS_WORKFLOW}": banks.FmoSeedUrlsWorkflow,
        f"{settings.FMO_ABBREVIATION}-{settings.RESULTS_PAGE_WORKFLOW}": banks.FmoResultsScrapeWorkflow,
        f"{settings.FMO_ABBREVIATION}-{settings.PROJECT_PAGE_WORKFLOW}": banks.FmoProjectScrapeWorkflow,
        f"{settings.IDB_ABBREVIATION}-{settings.PROJECT_PARTIAL_DOWNLOAD_WORKFLOW}": banks.IdbPartialProjectDownloadWorkflow,
        f"{settings.IDB_ABBREVIATION}-{settings.SEED_URLS_WORKFLOW}": banks.IdbSeedUrlsWorkflow,
        f"{settings.IDB_ABBREVIATION}-{settings.RESULTS_PAGE_WORKFLOW}": banks.IdbResultsScrapeWorkflow,
        f"{settings.IDB_ABBREVIATION}-{settings.PROJECT_PARTIAL_PAGE_WORKFLOW}": banks.IdbProjectPartialScrapeWorkflow,
        f"{settings.IFC_ABBREVIATION}-{settings.SEED_URLS_WORKFLOW}": banks.IfcSeedUrlsWorkflow,
        f"{settings.IFC_ABBREVIATION}-{settings.PROJECT_PAGE_WORKFLOW}": banks.IfcProjectScrapeWorkflow,
        f"{settings.KFW_ABBREVIATION}-{settings.PROJECT_DOWNLOAD_WORKFLOW}": banks.KfwDownloadWorkflow,
        f"{settings.MIGA_ABBREVIATION}-{settings.SEED_URLS_WORKFLOW}": banks.MigaSeedUrlsWorkflow,
        f"{settings.MIGA_ABBREVIATION}-{settings.RESULTS_PAGE_WORKFLOW}": banks.MigaResultsScrapeWorkflow,
        f"{settings.MIGA_ABBREVIATION}-{settings.PROJECT_PAGE_WORKFLOW}": banks.MigaProjectScrapeWorkflow,
        f"{settings.NBIM_ABBREVIATION}-{settings.PROJECT_PAGE_WORKFLOW}": banks.NbimProjectScrapeWorkflow,
        f"{settings.NBIM_ABBREVIATION}-{settings.SEED_URLS_WORKFLOW}": banks.NbimSeedUrlsWorkflow,
        f"{settings.PRO_ABBREVIATION}-{settings.SEED_URLS_WORKFLOW}": banks.ProSeedUrlsWorkflow,
        f"{settings.PRO_ABBREVIATION}-{settings.RESULTS_PAGE_WORKFLOW}": banks.ProResultsScrapeWorkflow,
        f"{settings.PRO_ABBREVIATION}-{settings.PROJECT_PAGE_WORKFLOW}": banks.ProProjectScrapeWorkflow,
        f"{settings.UNDP_ABBREVIATION}-{settings.PROJECT_PARTIAL_DOWNLOAD_WORKFLOW}": banks.UndpProjectPartialDownloadWorkflow,
        f"{settings.UNDP_ABBREVIATION}-{settings.PROJECT_PARTIAL_PAGE_WORKFLOW}": banks.UndpProjectPartialScrapeWorkflow,
        f"{settings.WB_ABBREVIATION}-{settings.PROJECT_DOWNLOAD_WORKFLOW}": banks.WbDownloadWorkflow,
    }

    @staticmethod
    def get(
        source: str,
        workflow_type: str,
        data_request_client: DataRequestClient,
        msg_queue_client: MessageQueueClient,
        db_client: DatabaseClient,
    ) -> BaseWorkflow:
        """Looks up a workflow by source and type and then instantiates it.

        Args:
            source: The abbreviation used for the data source.

            workflow_type: The name of the workflow.

            data_request_client: A client for making HTTP GET requests while
                adding random delays and rotating user agent headers.

            msg_queue_client: A client for a message queue.

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
        logger = LoggerFactory.get(
            f"WORKER - {source.upper()} - {workflow_type.upper()}"
        )

        # Select correct keyword args for workflow initializer
        if workflow_type in (
            settings.PROJECT_DOWNLOAD_WORKFLOW,
            settings.PROJECT_PAGE_WORKFLOW,
            settings.PROJECT_PARTIAL_PAGE_WORKFLOW,
        ):
            params = {
                "data_request_client": data_request_client,
                "db_client": db_client,
                "logger": logger,
            }
        elif workflow_type in (
            settings.PROJECT_PARTIAL_DOWNLOAD_WORKFLOW,
            settings.RESULTS_PAGE_MULTISCRAPE_WORKFLOW,
            settings.RESULTS_PAGE_WORKFLOW,
            settings.SEED_URLS_WORKFLOW,
        ):
            params = {
                "data_request_client": data_request_client,
                "msg_queue_client": msg_queue_client,
                "db_client": db_client,
                "logger": logger,
            }
        else:
            raise RuntimeError(
                f'A workflow was improperly configured: "{workflow_type}".'
            )

        return workflow_cls(**params)
