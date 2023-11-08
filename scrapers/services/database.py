"""Provides a client permitting CRUD database operations
on pipeline objects through a REST API.
"""

import json
import os
import requests
from flask.wrappers import Response
from logging import Logger
from scrapers.models.task import TaskRequest, TaskUpdate
from typing import Dict, List, Tuple


class DbClient():
    """Permits CRUD operations against pipeline entities in the database. 
    """

    def __init__(self, logger: Logger) -> None:
        """Initializes a new instance of a `DbClient`.

        Args:
            logger (`Logger`): An instance of the logging class.

        Returns:
            None
        """
        try:
            base_url = os.environ['API_BASE_URL']
        except KeyError:
            raise Exception("Failed to instantiate database client. "
                "No base API URL found in environmental variables.")

        self._logger = logger
        self._base_url = base_url


    def _get_batch_records(
        self,
        url: str,
        record_type: str,
        page_number: int=1,
        timeout: int=60) -> Tuple[List[Dict], int]:
        """Receives a single page of records from the database.

        Args:
            url (url): The API URL.

            record_type (`str`): The entity type of the records
                (e.g., 'projects', 'tasks', etc.) Used to
                compose an exception message.

            page_number (int): The page to retrieve. Defaults to 1.

            timeout (int): The number of seconds to wait for
                the HTTP GET request to complete. Defaults
                to 60.

        Returns:
            ((list of dict, int)): A two-item tuple consisting of
                the list of records and the total number of pages.
        """
        self._logger.info(f"Requesting page {page_number} of "
            f"data for record type {record_type}.")

        response = requests.get(f"{url}?page={page_number}", timeout=timeout)
        try:
            response_body = response.json()
        except:
            response_body = None

        if not response.ok:
            raise Exception(f"Failed to retrieve {record_type} from database. "
                f"Received '{response.status_code}' status code "
                f"and message '{response_body}'.")

        records = response_body['records']
        total_num_pages = response_body['total_num_pages']
        self._logger.info("Request completed successfully.")
        
        return records, total_num_pages


    def _get_paged_records(
        self,
        url: str,
        record_type: str,
        timeout: int=60) -> List[Dict]:
        """Paginates through results to retrieve records from the database.

        Args:
            url (url): The API URL.

            record_type (`str`): The entity type of the records
                (e.g., 'projects', 'tasks', etc.) Used to
                compose an exception message.

            timeout (int): The number of seconds to wait for
                the HTTP GET request to complete. Defaults
                to 60.

        Returns:
            (`list` of `dict`): The list of records.
        """
        self._logger.info(f"Requesting first page of data for "
            f"record type {record_type}.")
        has_pages = True
        page_number = 1
        records = []

        while has_pages:
            response = requests.get(f"{url}?page={page_number}", timeout=timeout)
            try:
                response_body = response.json()
            except:
                response_body = None

            if not response.ok:
                raise Exception(f"Failed to retrieve {record_type} from database. "
                    f"Received '{response.status_code}' status code "
                    f"and message '{response_body}'.")

            records.extend(response_body['records'])
            total_num_pages = response_body['total_num_pages']
            current_page = int(response_body['page'])
            self._logger.info(f"Processed page {current_page} of {total_num_pages}.")
            
            if total_num_pages == current_page:
                self._logger.info("Finished retrieving data for "
                    f"record type '{record_type}'.")
                break

            page_number = current_page + 1
        
        return records


    def _get_records(
        self, 
        url: str, 
        record_type: str,
        timeout: int=60) -> List[Dict]:
        """A generic method for retrieving records from the database.

        Args:
            url (url): The API URL.

            record_type (`str`): The entity type of the records
                (e.g., 'projects', 'tasks', etc.) Used to
                compose an exception message.

            timeout (int): The number of seconds to wait for
                the HTTP GET request to complete. Defaults
                to 60.

        Returns:
            (`list` of `dict`): The list of records.
        """
        response = requests.get(url, timeout=timeout)
        try:
            response_body = response.json()
        except:
            response_body = None

        if not response.ok:
            raise Exception(f"Failed to retrieve {record_type} from database. "
                f"Received '{response.status_code}' status code "
                f"and message '{response_body}'.")

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
            url (url): The API URL.

            records (`list` of `dict`): The records to upsert.

            record_type (`str`): The entity type of the records
                (e.g., 'projects', 'tasks', etc.) Used to
                compose an exception message.

            peform_upsert (bool): A boolean indicating whether
                records that already exist in the database
                should be updated (as opposed to ignored).
                Defaults to False.

            batch_size (int): The default batch size for
                a bulk operation. Defaults to 1000 records.

        Returns:
            ((list of dict, int)): A two-item tuple consisting
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
        self._logger.info(f"Beginning batch processing for record type '{record_type}'.")
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
            payload = {'upsert': perform_upsert, 'records': batch, 'batch_size': batch_size}
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


    def bulk_insert_staged_projects(
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


    def bulk_insert_tasks(self, tasks: List[Dict]) -> List[Dict]:
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


    def insert_task(self, task: TaskRequest) -> None:
        """Inserts a single task in the database. Logs any failures
        that occur rather than raising an exception.

        Args:
            task (`TaskRequest`): The task to insert.

        Returns:
            None
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


    def update_task(self, task: TaskUpdate) -> None:
        """Updates a task in the database.

        Args:
            task (`TaskUpdate`): The task to update.

        Returns:
            None
        """
        url = f"{self._base_url}/api/pipeline/tasks/{task.id}"
        response = requests.patch(url, data=vars(task))

        if not response.ok:
            response_body = json.dumps(response.json())
            raise Exception(f"Failed to update task within database. "
                f"Received '{response.status_code}' status code "
                f"and message '{response_body}'.")


    def update_job(self, job: Dict) -> Dict:
        """
        Updates a job in the database and returns the resulting JSON.

        Args:
            job (dict): The job to update.

        Returns:
            (dict): The job representation.
        """
        url = f"{self._base_url}/api/pipeline/jobs/{job['id']}"
        response = requests.patch(url, data=job)

        if not response.ok:
            raise Exception(f"Failed to update job within database. "
                f"Received '{response.status_code}' status code "
                f"and message '{response.json()}'.")

        return response.json()


    def update_staged_project(self, project: Dict) -> None:
        """Updates a staged project in the database.

        Args:
            project (dict): The project to update.

        Returns:
            None
        """
        url = f"{self._base_url}/api/pipeline/staged-projects"
        response = requests.patch(url, data=project)

        if not response.ok:
            raise Exception(f"Failed to update project within database. "
                f"Received '{response.status_code}' status code.")

