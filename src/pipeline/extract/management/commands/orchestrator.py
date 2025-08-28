"""Classes used to orchestrate the data extraction process."""

# Standard library imports
import logging
import os
import time
from abc import ABC, abstractmethod
from datetime import datetime, UTC

# Third-party imports
from django.core.management.base import BaseCommand, CommandParser

# Application imports
from common.logger import LoggerFactory
from common.tasks import MessageQueueClient, TaskQueueFactory
from extract.dal import DatabaseClient
from extract.models import ExtractionJob, ExtractionTask
from extract.workflows.registry import StarterWorkflowRegistry


class JobSubmission(ABC):
    """A template for triggering and monitoring a data extraction job."""

    def __init__(
        self,
        db_client: DatabaseClient,
        queue: MessageQueueClient,
        polling_interval: int,
        timeout: int,
        logger: logging.Logger,
    ) -> None:
        """Initializes a new instance of a `JobSubmission`.

        Args:
            db_client: The database client.

            queue: The task queue.

            polling_interval: The minutes to wait in between polling
                attempts when monitoring the completion of the job.

            timeout: The maximum number of minutes to wait for the job
                to complete before raising an exception.

            logger: The logger.

        Returns:
            `None`
        """
        self._db_client = db_client
        self._task_queue = queue
        self._polling_interval = polling_interval
        self._timeout = timeout
        self._logger = logger

    @abstractmethod
    def handle_tasks(
        self, job: ExtractionJob, sources: list[str], force_restart: bool
    ) -> None:
        """Saves and submits tasks to process the given data sources.

        Args:
            job: The parent job.

            sources: The data sources.

            force_restart: Whether all tasks should be re-queued
                regardless of whether they have previously been attempted.

        Returns:
            `None`
        """
        raise NotImplementedError

    def validate_sources(self, sources: list[str]) -> list[str]:
        """Validates and cleans a list of requested data sources.

        Raises:
            `ValueError` if any of the requested data sources
                have not been registered.

        Args:
            sources: The data sources.

        Returns:
            The validated.
        """
        valid_sources = StarterWorkflowRegistry.list()

        if not sources:
            return valid_sources

        for source in sources:
            if source not in valid_sources:
                valid_sources = ", ".join(valid_sources)
                raise ValueError(
                    "Failed to queue workflows. Received invalid data "
                    f'source name "{source}" as argument. Only the '
                    f"following names are permitted: {valid_sources}."
                )

        return list(set(sources))

    def monitor_job(self, job: ExtractionJob) -> None:
        """Monitors the progress of tasks for a job.

        Args:
            job: The job to monitor.

        Returns:
            `None`
        """
        start = datetime.now(tz=UTC)

        while True:
            # Query database for tasks that have not reached a terminal state
            self._logger.info("Polling database for outstanding tasks.")
            outstanding_tasks = ExtractionTask.objects.exists(
                job=job,
                status__in=[
                    ExtractionTask.StatusChoices.NOT_STARTED,
                    ExtractionTask.StatusChoices.IN_PROGRESS,
                ],
            )

            # Break if all tasks have completed
            if not outstanding_tasks:
                self._logger.info("All tasks have completed.")
                break

            # Raise exception if maximum time allotted has been exceeded
            elapsed = (datetime.now(tz=UTC) - start).total_seconds()
            if elapsed > self._timeout * 60:
                raise RuntimeError(
                    f"Maximum time limit of {self._timeout} minutes "
                    f"exceeded. Elapsed time: {elapsed} second(s)."
                )

            # Otherwise, sleep for configured interval
            self._logger.info(
                f"{len(outstanding_tasks):,} task(s) still to be processed. "
                f"Sleeping for {self._polling_interval} minute(s)."
            )
            time.sleep(self._polling_interval * 60)

    def execute(
        self, job: ExtractionTask, sources: list[str], force_restart: bool
    ) -> None:
        """Executes the data extraction job.

        Args:
            job: The job.

            sources: The data sources.

            force_restart: Whether all tasks should be re-queued
                regardless of whether they have previously been attempted.

        Returns:
            `None`
        """
        # Drain existing queues
        self._logger.info("Draining existing task queues.")
        all_sources = StarterWorkflowRegistry.list()
        self._task_queue.purge(all_sources)
        self._logger.info(f"{len(all_sources):,} task queues drained successfully.")

        # Validate requested data sources
        self._logger.info("Validating requested data sources.")
        validated_sources = self.validate_sources(sources)
        self._logger(f"Identified {len(validated_sources):,} source(s) to process.")

        # Compose and submit data extraction tasks
        self._logger.info("Preparing data extraction tasks for data sources.")
        self.handle_tasks(job, validated_sources, force_restart)
        self._logger.info("Tasks successfully queued and persisted.")

        # Monitor tasks
        self._logger.info("Beginning task monitoring phase.")
        self.monitor_job(job)


class NewSubmission(JobSubmission):
    """Represents a new or restarted data extraction job."""

    def handle_tasks(
        self, job: ExtractionJob, sources: list[str], force_restart: bool
    ) -> None:
        """Saves and submits tasks to process the given data sources.

        Args:
            job: The parent job.

            sources: The data sources.

            force_restart: Whether all tasks should be re-queued
                regardless of whether they have previously been attempted.

        Returns:
            `None`
        """
        # Compose tasks
        tasks = [
            {
                "job_id": job.id,
                "source": source,
                "workflow_type": StarterWorkflowRegistry.get(source),
            }
            for source in sources
        ]
        self._logger.info(
            f"Created {len(tasks):,} new tasks to trigger the "
            "first stage of the data extraction workflow(s)."
        )

        # Insert task metadata into the database
        try:
            self._logger.info("Inserting task(s) into the database.")
            self._db_client.bulk_create_tasks(tasks)
        except Exception as e:
            raise RuntimeError(
                f"Failed to insert new task(s) into the database. {e}"
            ) from None

        # Queue tasks for processing by workers
        try:
            self._logger.info("Queueing tasks for processing.")
            self._task_queue.enqueue(tasks)
        except Exception as e:
            raise RuntimeError(f"Failed to queue task(s). {e}") from None


class Resubmission(JobSubmission):
    """Represents a resubmitted data extraction job."""

    def handle_tasks(
        self, job: ExtractionJob, sources: list[str], force_restart: bool
    ) -> None:
        """Saves and submits tasks to process the given data sources.

        Args:
            job: The parent job.

            sources: The data sources.

            force_restart: Whether all tasks should be re-queued
                regardless of whether they have previously been attempted.

        Returns:
            `None`
        """
        # Mark tasks with a status of "In Progress" or "Not Started"
        # as cancelled in database if they belong to a source that
        # is NOT configured for this resubmission attempt
        self._logger.info(
            "Cancelling any pending or unstarted tasks for "
            "data sources not requested in this resubmission."
        )
        ExtractionTask.objects.filter(
            job=job,
            status__in=[
                ExtractionTask.StatusChoices.IN_PROGRESS,
                ExtractionTask.StatusChoices.NOT_STARTED,
            ],
        ).exclude(source__in=sources).update(
            status=ExtractionTask.StatusChoices.CANCELLED
        )

        # Define set of tasks to resubmit
        self._logger.info("Identifying tasks to resubmit.")
        queryset = (
            ExtractionTask.objects.filter(job=job, source__in=sources)
            if force_restart
            else ExtractionTask.objects.filter(
                job=job,
                source__in=sources,
                status__in=[
                    ExtractionTask.StatusChoices.NOT_STARTED,
                    ExtractionTask.StatusChoices.IN_PROGRESS,
                    ExtractionTask.StatusChoices.ERROR,
                ],
            )
        )
        self._logger.info(
            f"Found {len(queryset):,} non-completed task(s) "
            f'for pre-existing job "{job.id}" in the database.'
        )

        # Update the tasks' status to "Not Started"
        self._logger.info("Updating task statuses to 'Not Started'.")
        queryset.update(status=ExtractionTask.StatusChoices.NOT_STARTED)

        # Queue tasks for processing by workers
        try:
            self._logger.info("Resubmitting task(s) to the queue for processing.")
            self._task_queue.enqueue(
                [
                    {
                        "job_id": job.id,
                        "source": task.source,
                        "workflow_type": task.workflow_type,
                    }
                    for task in queryset
                ]
            )
        except Exception as e:
            raise RuntimeError(f"Failed to queue task(s). {e}") from None


class Command(BaseCommand):
    """The Django management command."""

    help_text = (
        "Orchestrates the data extraction process for configured "
        "financial institutions by queuing tasks for workers to consume. "
        "Data is written to file storage and task statuses are "
        "maintained in a SQL dasebase."
    )

    def add_arguments(self, parser: CommandParser) -> None:
        """Configures command line arguments.

        Args:
            parser: The command line argument parser.

        Returns:
            `None`
        """
        parser.add_argument(
            "--restrict-to",
            nargs="*",
            help="The initial list of data sources to scrape (default: all sources).",
        )
        parser.add_argument(
            "--force-restart",
            action="store_true",
            help="Re-queue all tasks for the given data sources regardless "
            "of whether they have previously been attempted.",
        )

    def handle(self, *args, **options) -> None:
        """Orchestrates tasks for a data extraction job.

        A task is a single unit of work to perform on a URL. Tasks are placed
        on queues for processing and their status is tracked in a database.
        A job is a collection of tasks processed for a unique, given date
        ("YYYY-MM-DD").

        This command creates a job in the database for the current date
        ("YYYY-MM-DD") if it does not already exist and then queues the
        tasks necessary to finish that job. If the job is newly-created OR
        the command line argument `force_restart` is set to `True`, the
        command queues the initial set of tasks for the specified data sources;
        otherwise, it queues all tasks that have not yet succeeded to be
        attempted (and/or re-attempted).

        The command monitors the progress of the tasks until they have all
        reached a terminal state or the configured timeout has been reached.

        Args:
            *args: Positional arguments.

            **options: Keyword arguments that include:

                `restrict_to`: The list of data sources to process. If not
                    specified, the command queues tasks for all sources.

                `force_restart`: If set to `True`, the command queues the
                    initial set of tasks for the given data sources; otherwise,
                    it queues all tasks that have not yet succeeded to be
                    attempted (and/or re-attempted).

        Returns:
            `None`
        """
        # Configure logger
        logger = LoggerFactory.get("ORCHESTRATOR")

        # Parse command line options
        try:
            logger.info("Parsing command line options.")
            restrictions = options["restrict_to"]
            force_restart = options["force_restart"]
        except KeyError as e:
            logger.error(f'Missing expected command line option "{e}".')
            exit(1)

        # Parse required environment variables
        try:
            logger.info("Parsing required environment variables.")
            polling_interval = int(os.environ["POLLING_INTERVAL_IN_MINUTES"])
            timeout = int(os.environ["MAX_WAIT_IN_MINUTES"])
        except KeyError as e:
            logger.error(f'Missing required environment variable "{e}".')
            exit(1)

        # Initialize database client
        try:
            logger.info("Initializing database client.")
            db_client = DatabaseClient(logger)
        except Exception as e:
            logger.error(f"Failed to initialize database client. {e}")
            exit(1)

        # Initialize task queue client
        try:
            logger.info("Initializing task queue client.")
            task_queue = TaskQueueFactory.get()
        except Exception as e:
            logger.error(f"Failed to initialize task queue client. {e}")
            exit(1)

        # Initialize job in database for current date ("YYYY-MM-DD")
        now = datetime.now(tz=UTC).strftime("%Y-%m-%d")
        job, created = ExtractionJob.objects.get_or_create(date=now)
        logger.info(
            f'Created new job for date "{now}".'
            if created
            else f'Found existing job for date "{now}".'
        )

        # Submit tasks for job and monitor progress
        try:
            args = [db_client, task_queue, polling_interval, timeout, logger]
            submission = NewSubmission(*args) if created else Resubmission(*args)
            submission.execute(job, restrictions, force_restart)
        except Exception as e:
            logger.error(f"Failed to orchestrate tasks. {e}")
            exit(1)

        # Log success
        logger.info("Data extraction completed successfully.")
