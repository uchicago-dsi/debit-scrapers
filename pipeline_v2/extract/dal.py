"""Permits CRUD database operations for entities related to data extraction."""

# Standard library imports
from typing import Dict, List

# Application imports
from common.database import bulk_insert_records
from extract.domain import (
    JobUpdateRequest,
    StagedProjectUpsertRequest,
    TaskInsertRequest,
    TaskUpdateRequest,
)
from extract.models import ExtractionJob, ExtractionTask


class ExtractionDbClient:
    """A database client for use throughout the application."""

    def bulk_create_staged_projects(
        self, projects: List[StagedProjectUpsertRequest]
    ) -> List[Dict]:
        """Inserts staged project records into the database using a bulk
        operation while ignoring any conflicts. Raises an exception if
        the operation fails.

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

    def bulk_create_tasks(self, tasks: List[TaskInsertRequest]) -> List[Dict]:
        """Inserts tasks into the database using a bulk operation while
        ignoring any conflicts. Raises an exception if the operation fails.

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

    def update_job(self, job: JobUpdateRequest) -> Dict:
        """Updates a job in the database and returns the resulting JSON.

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

    def update_staged_project(self, project: StagedProjectUpsertRequest) -> Dict:
        """Updates a staged project in the database.

        Args:
            project: The project to update.

        Returns:
            The updated project.
        """
        # Isolate project bank and URL
        bank = project.pop("bank")
        url = project.pop("url")

        # Confirm that project is unique
        try:
            StagedProjectUpsertRequest.objects.get(bank=bank, url=url)
        except StagedProjectUpsertRequest.DoesNotExist:
            raise Exception(f"Project from bank {bank} with URL {url} does not exist.")
        except StagedProjectUpsertRequest.MultipleObjectsReturned:
            raise Exception(
                "An unexpected error occurred. Project from bank "
                f"{bank} with URL {url} is not unique."
            )

        # Update project in database
        try:
            obj, _ = StagedProjectUpsertRequest.objects.update_or_create(
                bank=bank, url=url, defaults=project
            )
        except Exception as e:
            raise Exception(f"Failed to update project within database. {e}")

        return obj

    def update_task(self, task: TaskUpdateRequest) -> Dict:
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
            raise Exception(f"Task with given id {task_id} does not exist.")
        except ExtractionTask.MultipleObjectsReturned:
            raise Exception(
                "An unexpected error occurred. Task with "
                f"given id {task_id} is not unique."
            )

        # Update task in database
        try:
            obj, _ = ExtractionTask.objects.update_or_create(id=task_id, defaults=task)
        except Exception as e:
            raise Exception(f"Failed to update task within database. {e}")

        return obj
