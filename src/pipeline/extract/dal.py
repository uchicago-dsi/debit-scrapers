"""Permits CRUD database operations for entities related to data extraction."""

# Standard library imports

# Application imports
from common.database import bulk_insert_records
from extract.domain import (
    JobUpdateRequest,
    StagedProjectUpsertRequest,
    TaskInsertRequest,
    TaskUpdateRequest,
)
from extract.models import ExtractedProject, ExtractionJob, ExtractionTask


class DatabaseClient:
    """A database client for use throughout the application."""

    def bulk_create_staged_projects(
        self, projects: list[StagedProjectUpsertRequest]
    ) -> list[dict]:
        """Bulk inserts staged project records while ignoring conflicts.

        Raises:
            `RuntimeError` if the operation fails.

        Args:
            projects: The project records associated with the task.

        Returns:
            A representation of the created projects.
        """
        try:
            return bulk_insert_records(
                projects,
                "extracted_project",
                ["job_id", "status", "source", "workflow_type", "url"],
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to create a new task in the database. {e}"
            ) from None

    def bulk_create_tasks(self, tasks: list[TaskInsertRequest]) -> list[dict]:
        """Bulk inserts task records while ignoring conflicts.

        Raises:
            `RuntimeError` if the operation fails.

        Args:
            tasks: The tasks to insert.

        Returns:
            The newly-created database rows, to be used as messages.
        """
        try:
            return bulk_insert_records(
                tasks,
                "extraction_task",
                ["job_id", "status", "source", "workflow_type", "url"],
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to create a new task in the database. {e}"
            ) from None

    def update_job(self, job: JobUpdateRequest) -> dict:
        """Updates a job in the database and returns the new representation.

        Args:
            job: The job to update.

        Returns:
            The newly-updated job.
        """
        # Isolate invocation id
        invocation_id = job.pop("invocation_id")

        # Confirm that job is unique
        try:
            ExtractionJob.objects.get(invocation_id=invocation_id)
        except ExtractionJob.DoesNotExist:
            raise RuntimeError(
                "An unexpected error occurred. Job wih given"
                f'invocation id "{invocation_id}" does not exist.'
            ) from None
        except ExtractionJob.MultipleObjectsReturned:
            raise RuntimeError(
                "An unexpected error occurred. Job with given"
                f'invocation id "{invocation_id}" is not unique.'
            ) from None

        # Update job in database
        try:
            obj, _ = ExtractionJob.objects.update_or_create(
                invocation_id=invocation_id, defaults=job
            )
        except Exception as e:
            raise RuntimeError(f"Failed to update job within database. {e}") from None

        return obj

    def update_staged_project(self, project: StagedProjectUpsertRequest) -> dict:
        """Updates a staged project in the database.

        Args:
            project: The project to update.

        Returns:
            The updated project.
        """
        # Isolate project data source and URL
        source = project.pop("source")
        url = project.pop("url")

        # Confirm that project is unique
        try:
            ExtractedProject.objects.get(source=source, url=url)
        except ExtractedProject.DoesNotExist:
            raise RuntimeError(
                f"Project from bank {source} with URL {url} does not exist."
            ) from None
        except ExtractedProject.MultipleObjectsReturned:
            raise RuntimeError(
                "An unexpected error occurred. Project from bank "
                f"{source} with URL {url} is not unique."
            ) from None

        # Update project in database
        try:
            obj, _ = ExtractedProject.objects.update_or_create(
                source=source, url=url, defaults=project
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to update project within database. {e}"
            ) from None

        return obj

    def update_task(self, task: TaskUpdateRequest) -> dict:
        """Updates a task in the database.

        Args:
            task: The task to update.

        Returns:
            The updated task.
        """
        # Isolate task id
        task_id = task.pop("id")

        # Confirm that task is unique
        try:
            ExtractionTask.objects.get(id=task_id)
        except ExtractionTask.DoesNotExist:
            raise RuntimeError(
                f"Task with given id {task_id} does not exist."
            ) from None
        except ExtractionTask.MultipleObjectsReturned:
            raise RuntimeError(
                "An unexpected error occurred. Task with "
                f"given id {task_id} is not unique."
            ) from None

        # Update task in database
        try:
            obj, _ = ExtractionTask.objects.update_or_create(id=task_id, defaults=task)
        except Exception as e:
            raise RuntimeError(f"Failed to update task within database. {e}") from None

        return obj
