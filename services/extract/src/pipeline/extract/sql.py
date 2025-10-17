"""Permits CRUD database operations for entities related to data extraction."""

# Standard library imports
from dataclasses import asdict
from datetime import datetime, UTC

# Third-party imports
from django.conf import settings
from django.db.models import Q
from django.forms.models import model_to_dict

# Application imports
from extract.domain import ProjectUpsertRequest, TaskUpsertRequest
from extract.models import ExtractedProject, ExtractionJob, ExtractionTask


class DatabaseClient:
    """A database client for use throughout the application."""

    def bulk_upsert_projects(self, projects: list[dict]) -> list[dict]:
        """Bulk upserts project records.

        Raises:
            `RuntimeError` if the operation fails.

        Args:
            projects: The project records.

        Returns:
            A representation of the newly upserted projects.
        """
        # Abort upsert if no projects provided
        if not projects:
            return []

        # Manually add timestamp fields
        # NOTE: Bulk operations don't call Model.save(), so
        # the auto_now and auto_now_add fields aren't updated.
        try:
            mapped = []
            for project in projects:
                now = datetime.now(tz=UTC)
                project["created_at_utc"] = project.get("created_at_utc", now)
                project["last_updated_at_utc"] = now
                safe_project = asdict(ProjectUpsertRequest(**project))
                mapped.append(ExtractedProject(**safe_project))
        except Exception as e:
            raise RuntimeError(
                f"Failed to prepare projects for bulk upsert. {e}"
            ) from None

        # Bulk upsert projects (i.e., create new records and update conflicts)
        try:
            unique_fields = ["source", "url"]
            update_fields = [
                "affiliates",
                "countries",
                "date_actual_close",
                "date_approved",
                "date_disclosed",
                "date_effective",
                "date_last_updated",
                "date_planned_close",
                "date_planned_effective",
                "date_revised_close",
                "date_signed",
                "date_under_appraisal",
                "finance_types",
                "fiscal_year_effective",
                "name",
                "number",
                "sectors",
                "status",
                "total_amount",
                "total_amount_currency",
                "total_amount_usd",
            ]
            upserted = ExtractedProject.objects.bulk_create(
                mapped,
                update_conflicts=True,
                update_fields=update_fields,
                unique_fields=unique_fields,
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to bulk upsert projects in the database. {e}"
            ) from None

        # Return dictionary representations of upserted projects
        return [model_to_dict(obj) for obj in upserted]

    def bulk_upsert_tasks(self, tasks: list[TaskUpsertRequest]) -> list[dict]:
        """Bulk upserts task records while managing conflicts.

        Raises:
            `RuntimeError` if the operation fails.

        Args:
            tasks: The tasks to upsert.

        Returns:
            The database representation of the newly-created or updated tasks.
        """
        # Abort upsert if no tasks provided
        if not tasks:
            return []

        # Manually add timestamp fields
        # NOTE: Bulk operations don't call Model.save(),
        # so the auto_now_add field isn't updated.
        try:
            mapped = []
            for task in tasks:
                now = datetime.now(tz=UTC)
                task["created_at_utc"] = task.get("created_at_utc", now)
                mapped.append(ExtractionTask(**task))
        except Exception as e:
            raise RuntimeError(
                f"Failed to prepare tasks for bulk upsert. {e}"
            ) from None

        # Attempt to upsert tasks
        try:
            upserted = ExtractionTask.objects.bulk_create(
                objs=mapped,
                update_conflicts=True,
                update_fields=["status"],
                unique_fields=["job_id", "source", "workflow_type", "url"],
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to bulk upsert task(s) into the database. {e}"
            ) from None

        # Return dictionary representations of upserted tasks
        return [model_to_dict(task) for task in upserted]

    def cancel_outstanding_tasks(
        self, job_pk: int, excluded_sources: list[str]
    ) -> None:
        """Cancels outstanding tasks for the given job.

        Args:
            job_pk: The unique identifier of the job.

            excluded_sources: A list of data sources to exclude.

        Returns:
            `None`
        """
        ExtractionTask.objects.filter(
            job_id=job_pk,
            status__in=[
                ExtractionTask.StatusChoices.IN_PROGRESS,
                ExtractionTask.StatusChoices.NOT_STARTED,
            ],
        ).exclude(source__in=excluded_sources).update(
            status=ExtractionTask.StatusChoices.CANCELLED
        )

    def get_or_create_job(self, date: str | None = None) -> tuple[int, str, bool]:
        """Gets or creates a new job in the database for a given date.

        NOTE: The date should be expressed as YYYY-MM-DD and represent UTC.
        If no date is provided, the current date (today) is used by default.

        Args:
            date: The date to create a job for.

        Returns:
            A three-item tuple consisting of the job primary key
                and date as well as a boolean indicating whether
                the job was created.
        """
        # Validate date format if date provided
        if date:
            try:
                datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                raise ValueError(f"Invalid date format: {date}.") from None

        # Validate that date is not set in the future
        now = datetime.now(tz=UTC).strftime("%Y-%m-%d")
        if date and date > now:
            raise ValueError(f'Cannot create job for future date "{date}".') from None

        # Get or create job
        job, created = ExtractionJob.objects.get_or_create(date=date or now)
        return job.id, job.date, created

    def count_outstanding_tasks(self, job_pk: int) -> int:
        """Counts the number of outstanding tasks for the given job.

        Args:
            job_pk: The unique identifier of the job.

        Returns:
            The number of outstanding tasks.
        """
        # Compose query filters
        belongs_to_job = Q(job_id=job_pk)
        eligible_for_retry = Q(
            status=ExtractionTask.StatusChoices.ERROR,
        ) & Q(retry_count__lt=settings.MAX_TASK_RETRIES)
        pending = Q(
            status__in=[
                ExtractionTask.StatusChoices.NOT_STARTED,
                ExtractionTask.StatusChoices.IN_PROGRESS,
            ]
        )

        # Query database for outstanding tasks and take count
        return ExtractionTask.objects.filter(
            belongs_to_job & (eligible_for_retry | pending)
        ).count()

    def mark_job_completed(self, pk: int) -> None:
        """Marks a job as completed in the database.

        Args:
            pk: The unique identifier of the job.

        Returns:
            `None`
        """
        # Fetch job corresponding to given primary key
        try:
            job = ExtractionJob.objects.get(pk=pk)
        except ExtractionJob.DoesNotExist:
            raise RuntimeError(f'Job with given id "{pk}" does not exist.') from None

        # Attempt to mark job as completed
        try:
            job.completed_at_utc = datetime.now(UTC)
            job.save()
        except Exception as e:
            raise RuntimeError(
                f'Failed to update completed job "{pk}" within database. {e}'
            ) from None

    def mark_task_failure(self, pk: int, error: str, retry_count: int) -> None:
        """Marks a task as failed in the database.

        Args:
            pk: The unique identifier of the task.

            error: The exception message.

            retry_count: The number of times the task has been attempted.

        Returns:
            `None`
        """
        # Fetch task corresponding to given primary key
        try:
            task = ExtractionTask.objects.get(pk=pk)
        except ExtractionTask.DoesNotExist:
            raise RuntimeError(f'Task with given id "{pk}" does not exist.') from None

        # Attempt to mark task as failed
        try:
            task.status = ExtractionTask.StatusChoices.ERROR
            task.retry_count = retry_count
            task.last_error = error
            task.failed_at_utc = datetime.now(UTC)
            task.save()
        except Exception as e:
            raise RuntimeError(
                f'Failed to update failed task "{pk}" within database. {e}'
            ) from None

    def mark_task_pending(self, pk: int) -> None:
        """Marks a task as pending in the database.

        Args:
            pk: The unique identifier of the task.

        Returns:
            `None`
        """
        # Fetch task corresponding to given primary key
        try:
            task = ExtractionTask.objects.get(pk=pk)
        except ExtractionTask.DoesNotExist:
            raise RuntimeError(f'Task with given id "{pk}" does not exist.') from None

        # Attempt to mark task as pending
        try:
            task.status = ExtractionTask.StatusChoices.IN_PROGRESS
            task.started_at_utc = datetime.now(UTC)
            task.save()
        except Exception as e:
            raise RuntimeError(
                f'Failed to update pending task "{pk}" within database. {e}'
            ) from None

    def mark_task_success(self, pk: int, retry_count: int) -> None:
        """Marks a task as successful in the database.

        Args:
            pk: The unique identifier of the task.

            retry_count: The number of times the task has been attempted.

        Returns:
            `None`
        """
        # Fetch task corresponding to given primary key
        try:
            task = ExtractionTask.objects.get(pk=pk)
        except ExtractionTask.DoesNotExist:
            raise RuntimeError(f'Task with given id "{pk}" does not exist.') from None

        # Attempt to mark task as successful
        try:
            task.status = ExtractionTask.StatusChoices.COMPLETED
            task.retry_count = retry_count
            task.completed_at_utc = datetime.now(UTC)
            task.save()
        except Exception as e:
            raise RuntimeError(
                f'Failed to update successful task "{pk}" within database. {e}'
            ) from None

    def patch_project(self, project: dict) -> dict:
        """Updates select fields of a project in the database.

        Args:
            project: A representation of the project update.
                Expected to have `source` and `url` fields
                as well as one or more data fields.

        Returns:
            The database representation of the updated project.
        """
        # Isolate project data source and URL
        source = project.pop("source")
        url = project.pop("url")

        # Update project in database
        try:
            obj, _ = ExtractedProject.objects.update_or_create(
                source=source, url=url, defaults=project
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to update project within database. {e}"
            ) from None

        return model_to_dict(obj)

    def reset_tasks(
        self, job_id: int, sources: list[str], force_restart: bool = False
    ) -> list[dict]:
        """Resets tasks for the given job and data sources.

        If `force_restart` is `True`, all tasks will be reset,
        regardless of their current status. Otherwise, only tasks
        in a potentially non-terminal state (e.g., "Not Started",
        "In Progress", or "Error") will be reset. Resetting a task
        entails setting its status to "Not Started".

        Args:
            job_id: The unique identifier of the job.

            sources: The data sources.

            force_restart: Whether all tasks should be reset.
                Defaults to `False`.

        Returns:
            The database representation of the reset tasks.
        """
        # Determine which tasks should be reset
        queryset = (
            ExtractionTask.objects.filter(job_id=job_id, source__in=sources)
            if force_restart
            else ExtractionTask.objects.filter(
                job_id=job_id,
                source__in=sources,
                status__in=[
                    ExtractionTask.StatusChoices.NOT_STARTED,
                    ExtractionTask.StatusChoices.IN_PROGRESS,
                    ExtractionTask.StatusChoices.ERROR,
                ],
            )
        )

        # Reset tasks
        queryset.update(status=ExtractionTask.StatusChoices.NOT_STARTED)

        # Return dictionary representations of reset tasks
        return [model_to_dict(task) for task in queryset]
