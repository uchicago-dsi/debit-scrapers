"""Provides a client permitting CRUD database operations
on pipeline objects through a REST API.
"""

import json
import os
import requests
from abc import ABC
from flask.wrappers import Response
from logging import Logger
from pipeline.models.task import TaskRequest, TaskUpdate
from typing import Dict, List, Tuple


class BaseDbClient(ABC):
    """Provides basic operations for all database clients.
    """
    
    def __init__(self, logger: Logger) -> None:
        """Initializes a new instance of a `DbClient`.

        Args:
            logger (`Logger`): An instance of the logging class.

        Returns:
            `None`
        """
        try:
            base_url = os.environ["API_BASE_URL"]
        except KeyError:
            raise Exception("Failed to instantiate database client. "
                "No base API URL found in environmental variables.")

        self._logger = logger
        self._base_url = base_url

    def _get_records(
        self, 
        url: str, 
        record_type: str,
        timeout: int=60) -> List[Dict]:
        """A generic method for retrieving records from the database.

        Args:
            url (`str`): The API URL.

            record_type (`str`): The entity type of the records
                (e.g., 'projects', 'tasks', etc.) Used to
                compose an exception message.

            timeout (`int`): The number of seconds to wait for
                the HTTP GET request to complete. Defaults
                to 60.

        Returns:
            (`list` of `dict`): The list of records.
        """
        r = requests.get(url, timeout=timeout)
        try:
            response_body = r.json()
        except:
            response_body = None

        if not r.ok:
            raise Exception(f"Failed to retrieve {record_type} from "
                f"database.  Received \"{r.status_code}\" status code "
                f"and message \"{response_body}\".")

        return response_body

    def _perform_bulk_operation(
        self,
        url: str,
        records: List[Dict],
        record_type: str,
        perform_upsert: bool=False,
        batch_size: int=1000) -> Tuple[List[Dict], int]:
        """Calls the API to bulk insert or upsert a list of generic
        records into a database table using batches.

        Args:
            url (`str`): The API URL.

            records (`list` of `dict`): The records to upsert.

            record_type (`str`): The entity type of the records
                (e.g., 'projects', 'tasks', etc.) Used to
                compose an exception message.

            peform_upsert (`bool`): A boolean indicating whether
                records that already exist in the database
                should be updated (as opposed to ignored).
                Defaults to False.

            batch_size (`int`): The default batch size for
                a bulk operation. Defaults to 1000 records.

        Returns:
            ((`list` of `dict`, `int`,)): A two-item tuple consisting
                of the newly-created or upserted objects and
                the overall HTTP status code (i.e., 201 if
                any records were created and 200 otherwise
                for a successful operation).
        """
        # Compute number of batches necessary for bulk operation
        num_records = len(records)
        num_batches = (num_records // batch_size) + \
            (1 if num_records % batch_size > 0 else 0)

        # Generate batches and insert into database
        self._logger.info("Beginning batch processing "
                          f"for record type '{record_type}'.")
        batch_num = 1
        last_status_code = None
        returned_records = []
        for i in range(0, num_records, batch_size):
            if i + batch_size > num_records:
                batch = records[i:]
            else:
                batch = records[i: i + batch_size]

            # Make POST request
            self._logger.info("Performing bulk insert or upsert for "
                f"batch {batch_num} of {num_batches}.")
            payload = {
                "upsert": perform_upsert, 
                "records": batch, 
                "batch_size": batch_size
            }
            response: Response = requests.post(url, json=payload)

            # Parse response body
            try:
                response_body = response.json()
            except:
                response_body = None

            # Handle any exceptions
            if not response.ok:
                raise Exception(f"Bulk upsert of '{record_type}' failed with status code "
                    f"{response.status_code} - {response.reason} and message '{response_body}'.")

            # Prepare for next batch
            returned_records.extend(response_body)
            last_status_code = response.status_code
            batch_num += 1

        return returned_records, last_status_code

class DbClient(BaseDbClient):
    """Permits CRUD operations against pipeline entities in the database. 
    """

    def bulk_create_staged_projects(
        self, 
        project_records: List[Dict]) -> List[Dict]:
        """Inserts new staged project records into the database table.
        Raises an exception if the operation fails.

        Args:
            project_records (`list` of `dict`): The project records
                associated with the task.

        Returns:
            (`list` of `dict`): A representation of the created projects.
        """
        url = f"{self._base_url}/api/pipeline/staged-projects"
        record_type = 'staged projects'
        records, _ = self._perform_bulk_operation(url, project_records, record_type)
        return records

    def bulk_create_tasks(self, tasks: List[Dict]) -> List[Dict]:
        """Creates new tasks for processing web pages and inserts
        them into the database using a bulk operation. Raises an
        exception if the operation fails.

        Args:
            tasks (`list` of `dict`): The tasks to insert.

        Returns:
            (`list` of `dict`): A representation of the created database rows,
                to be used as messages. Fields include "id", "job_id",
                "bank", "workflow_type", and "url".
        """
        url = f"{self._base_url}/api/pipeline/tasks"
        record_type = 'tasks'
        records, _ = self._perform_bulk_operation(url, tasks, record_type)
        return records

    def bulk_insert_project_countries(self, records: List[Dict]) -> List[Dict]:
        """Inserts finalized project-country records into the database using
        a bulk operation. Raises an exception if the operation fails.

        Args:
            records (`list` of `dict`): The project-country records.

        Returns:
            (`list` of `dict`): A representation of the 
                created project-country rows.
        """
        url = f"{self._base_url}/api/countries/project-countries"
        record_type = 'project-countries'
        return self._perform_bulk_operation(url, records, record_type)
    
    def bulk_insert_project_sectors(self, records: List[Dict]) -> List[Dict]:
        """Inserts finalized project-sector records into the database using
        a bulk operation. Raises an exception if the operation fails.

        Args:
            records (`list` of `dict`): The project-sector records.

        Returns:
            (`list` of `dict`): A representation of the 
                created project-sector rows.
        """
        url = f"{self._base_url}/api/sectors/project-sectors"
        record_type = 'project sectors'
        records, status = self._perform_bulk_operation(url, records, record_type)
        return records, status
    
    def bulk_upsert_companies(self, records: List[Dict]) -> List[Dict]:
        """Upserts finalized S.E.C. company records into the database
        using a bulk operation. Raises an exception if the operation fails.

        Args:
            records (`list` of `dict`): The company records.

        Returns:
            (`list` of `dict`): A representation of the created companies.
        """
        url = f"{self._base_url}/api/form13f/companies"
        record_type = "companies"
        return self._perform_bulk_operation(url, records, record_type)

    def bulk_upsert_finalized_projects(self, records: List[Dict]) -> List[Dict]:
        """Upserts finalized project records into the database using
        a bulk operation. Raises an exception if the operation fails.

        Args:
            records (`list` of `dict`): The project records.

        Returns:
            (`list` of `dict`): A representation of the created projects.
        """
        url = f"{self._base_url}/api/projects"
        record_type = 'projects'
        return self._perform_bulk_operation(
            url, 
            records, 
            record_type, 
            perform_upsert=True)

    def bulk_upsert_forms(self, records: List[Dict]) -> List[Dict]:
        """Upserts finalized Form 13F submissions into the database
        using a bulk operation. Raises an exception if the operation fails.

        Args:
            records (`list` of `dict`): The Form 13F submissions.

        Returns:
            (`list` of `dict`): A representation of the created forms.
        """
        url = f"{self._base_url}/api/form13f/forms"
        record_type = 'forms'
        return self._perform_bulk_operation(url, records, record_type)

    def bulk_upsert_investments(self, records: List[Dict]) -> List[Dict]:
        """Upserts finalized Form 13F investments into the database
        using a bulk operation. Raises an exception if the operation fails.

        Args:
            records (`list` of `dict`): The Form 13F investments.

        Returns:
            (`list` of `dict`): A representation of the created investments.
        """
        url = f"{self._base_url}/api/form13f/investments"
        record_type = "investments"
        return self._perform_bulk_operation(url, records, record_type)

    def create_job(
        self,
        invocation_id: str,
        job_type: str) -> Tuple[int, bool]:
        """Creates a new pipeline job with the given invocation
        id if no record with that id already exists.

        Args:
            invocation_id (`str`): The id of the job.

            job_type (`str`): The job type key.

        Returns:
            (int, bool): A two-item tuple consisting of the pipeline
                job's id (primary key) and a boolean indicating whether
                the job was newly-created.
        """
        url = f"{self._base_url}/api/pipeline/jobs"
        data = {"invocation_id": invocation_id, "job_type": job_type}
        response = requests.post(url, json=data)
        
        if not response.ok:
            response_body = json.dumps(response.json())
            raise Exception(f"Failed to create new job in database. "
                f"Received '{response.status_code} - {response.reason}' status code "
                f"and message '{response_body}'.")

        was_created = response.status_code == 201
        return response.json()['id'], was_created

    def create_task(self, task: TaskRequest) -> None:
        """Inserts a single task in the database. Logs any failures
        that occur rather than raising an exception.

        Args:
            task (`TaskRequest`): The task to insert.

        Returns:
            `None`
        """
        url = f"{self._base_url}/api/pipeline/tasks"
        response = requests.post(url, data=vars(task))
        try:
            response_body = response.json()
        except Exception:
            response_body = None

        if not response.ok:
            response_body = json.dumps(response.json())
            raise Exception(f"Failed to create new task in database. "
                f"Received '{response.status_code} - {response.reason}'"
                f"status code and the message"
                f"{json.dumps(response_body) + '.' if response_body else '.'}")

    def delete_staged_investments(self, ids: List[int]) -> int:
        """Deletes staged investments based on id.

        Args:
            ids (`list` of `int`): The unique identifiers of
                the staged investments to delete.

        Returns:
            (`int`): The number of records deleted.
        """
        url = f"{self._base_url}/api/pipeline/staged-investments"
        payload = {"ids": ids}
        response = requests.delete(url, timeout=60, json=payload)

        try:
            response_body = response.json()
        except:
            response_body = None

        if not response.ok:
            raise Exception(f"Failed to delete staged investments. "
                f"Received '{response.status_code}' status code "
                f"and message '{response_body}'.")

        return response_body

    def delete_staged_projects_by_id(self, ids: List[int]) -> int:
        """Deletes staged projects based on id.

        Args:
            ids (`list` of `int`): The unique identifiers of the
                staged projects to delete.

        Returns:
            (`int`): The number of records deleted.
        """
        url = f"{self._base_url}/api/pipeline/staged-projects"
        payload = {"ids": ids}
        response = requests.delete(url, timeout=60, json=payload)

        try:
            response_body = response.json()
        except:
            response_body = None

        if not response.ok:
            raise Exception(f"Failed to delete staged projects. "
                f"Received '{response.status_code}' status code "
                f"and message '{response_body}'.")

        return response_body

    def get_banks(self) -> List[Dict]:
        """Retrieves all development banks from the database.

        Args:
            None

        Returns:
            (`list` of `dict`): The list of bank records.
        """
        url = f"{self._base_url}/api/banks"
        record_type = "banks"
        return self._get_records(url, record_type)

    def get_countries(self) -> List[Dict]:
        """Retrieves all country metadata from the database.
        NOTE: Geometries are not included.

        Args:
            `None`

        Returns:
            (`list` of `dict`): The list of country records.
        """
        url = f"{self._base_url}/api/countries"
        record_type = "countries"
        return self._get_records(url, record_type)

    def get_sectors(self) -> List[Dict]:
        """Retrieves all sectors from the database.

        Args:
            `None`

        Returns:
            (`list` of `dict`): The list of sector records.
        """
        url = f"{self._base_url}/api/sectors"
        record_type = 'sectors'
        return self._get_records(url, record_type)

    def get_staged_investments(self, limit: int) -> List[Dict]:
        """Retrieves a batch of staged investments from the database.

        Args:
            limit (`int`): The number of records to retrieve at once.

        Returns:
            (`list` of `dict`): The list of staged investment records.
        """
        url = f"{self._base_url}/api/pipeline/staged-investments?limit={limit}"
        record_type = "staged investments"
        return self._get_records(url, record_type)

    def get_staged_projects(self, limit: int) -> List[Dict]:
        """Retrieves a batch of staged projects from the database.

        Args:
            limit (`int`): The number of records to retrieve at once.

        Returns:
            (`list` of `dict`): The list of project records.
        """
        url = f"{self._base_url}/api/pipeline/staged-projects?limit={limit}"
        record_type = "staged projects"
        return self._get_records(url, record_type)

    def update_job(self, job: Dict) -> Dict:
        """Updates a job in the database and returns the resulting JSON.

        Args:
            job (`dict`): The job to update.

        Returns:
            (`dict`): The job representation.
        """
        url = f"{self._base_url}/api/pipeline/jobs/{job['id']}"
        response = requests.patch(url, data=job)
        print(response)

        if not response.ok:
            raise Exception(f"Failed to update job within database. "
                f"Received \"{response.status_code}\" status code "
                f"and message '\"{response.json()}\".")

        return response.json()

    def update_staged_project(self, project: Dict) -> None:
        """Updates a staged project in the database.

        Args:
            project (`dict`): The project to update.

        Returns:
            `None`
        """
        url = f"{self._base_url}/api/pipeline/staged-projects"
        response = requests.patch(url, data=project)

        if not response.ok:
            raise Exception(f"Failed to update project within database. "
                f"Received '{response.status_code}' status code.")

    def update_task(self, task: TaskUpdate) -> None:
        """Updates a task in the database.

        Args:
            task (`TaskUpdate`): The task to update.

        Returns:
            `None`
        """
        url = f"{self._base_url}/api/pipeline/tasks/{task.id}"
        response = requests.patch(url, data=vars(task))

        if not response.ok:
            response_body = json.dumps(response.json())
            raise Exception(f"Failed to update task within database. "
                f"Received '{response.status_code}' status code "
                f"and message '{response_body}'.")
