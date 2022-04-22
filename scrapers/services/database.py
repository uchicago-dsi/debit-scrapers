'''
database.py

Provides a client permitting CRUD database operations
on pipeline objects through a REST API.
'''

import json
import os
import requests
from flask.wrappers import Response
from logging import Logger
from scrapers.models.task import TaskRequest, TaskUpdate
from typing import Dict, List, Tuple


class DbClient():
    '''
    Permits CRUD operations against pipeline entities in the database. 
    '''

    def __init__(self, logger: Logger) -> None:
        '''
        Instantiates a new `DbClient`.

        Parameters:
            logger (Logger): An instance of the logging class.

        Returns:
            None
        '''
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
        timeout: int=60) -> List[Dict]:
        '''
        Receives a single page of records from the database.

        Parameters:
            url (url): The API URL.

            record_type (str): The entity type of the records
                (e.g., 'projects', 'tasks', etc.) Used to
                compose an exception message.

            page_number (int): The page to retrieve. Defaults to 1.

            timeout (int): The number of seconds to wait for
                the HTTP GET request to complete. Defaults
                to 60.

        Returns:
            (list of dict): The list of records.
        '''
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
        '''
        Paginates through results to retrieve records from the database.

        Parameters:
            url (url): The API URL.

            record_type (str): The entity type of the records
                (e.g., 'projects', 'tasks', etc.) Used to
                compose an exception message.

            timeout (int): The number of seconds to wait for
                the HTTP GET request to complete. Defaults
                to 60.

        Returns:
            (list of dict): The list of records.
        '''
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
        '''
        A generic method for retrieving records from the database.

        Parameters:
            url (url): The API URL.

            record_type (str): The entity type of the records
                (e.g., 'projects', 'tasks', etc.) Used to
                compose an exception message.

            timeout (int): The number of seconds to wait for
                the HTTP GET request to complete. Defaults
                to 60.

        Returns:
            (list of dict): The list of records.
        '''
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
        '''
        Calls the API to bulk insert or upsert a list of generic
        records into a database table using batches.

        Parameters:
            url (url): The API URL.

            records (list of dict): The records to upsert.

            record_type (str): The entity type of the records
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
        '''
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


    def create_job(self, invocation_id: str, job_type: str) -> Tuple[int, bool]:
        '''
        Creates a new pipeline job with the given invocation
        id if no record with that id already exists.

        Parameters:
            None

        Returns:
            (int, bool): A two-item tuple consisting of the pipeline
                job's id (primary key) and a boolean indicating whether
                the job was newly-created.
        '''
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


    def bulk_insert_staged_investments(self, records: List[Dict]) -> List[Dict]:
        '''
        Inserts new staged investment records into the database table.
        Raises an exception if the operation fails.

        Parameters:
            records (list of dict): The investment records
                associated with the task.

        Returns:
            (list of dict): A representation of the created projects.
        '''
        url = f"{self._base_url}/api/pipeline/staged-investments"
        record_type = 'staged investments'
        records, _ = self._perform_bulk_operation(url, records, record_type)
        return records


    def bulk_insert_staged_projects(self, project_records: List[Dict]) -> List[Dict]:
        '''
        Inserts new staged project records into the database table.
        Raises an exception if the operation fails.

        Parameters:
            project_records (list of dict): The project records
                associated with the task.

        Returns:
            (list of dict): A representation of the created projects.
        '''
        url = f"{self._base_url}/api/pipeline/staged-projects"
        record_type = 'staged projects'
        records, _ = self._perform_bulk_operation(url, project_records, record_type)
        return records


    def bulk_upsert_complaints(self, records: List[Dict]) -> List[Dict]:
        '''
        Inserts finalized complaint records into the database using
        a bulk operation. Raises an exception if the operation fails.

        Parameters:
             records (list of dict): The complaint records.

        Returns:
            (list of dict): A representation of the created complaints.
        '''
        url = f"{self._base_url}/api/complaints"
        record_type = 'complaints'
        return self._perform_bulk_operation(url, records, record_type)


    def bulk_insert_complaint_issues(self, records: List[Dict]) -> List[Dict]:
        '''
        Inserts finalized complaint-issue id pairs into the database using
        a bulk operation. Raises an exception if the operation fails.

        Parameters:
             records (list of dict): The complaint-issue records.

        Returns:
            (list of dict): A representation of the created records.
        '''
        url = f"{self._base_url}/api/complaints/complaint-issues"
        record_type = 'complaint issues'
        return self._perform_bulk_operation(url, records, record_type)


    def bulk_upsert_finalized_projects(self, records: List[Dict]) -> List[Dict]:
        '''
        Upserts finalized project records into the database using
        a bulk operation. Raises an exception if the operation fails.

        Parameters:
             records (list of dict): The project records.

        Returns:
            (list of dict): A representation of the created projects.
        '''
        url = f"{self._base_url}/api/projects"
        record_type = 'projects'
        return self._perform_bulk_operation(url, records, record_type, perform_upsert=True)


    def bulk_upsert_issues(self, records: List[Dict]) -> List[Dict]:
        '''
        Upserts finalized issue records into the database using
        a bulk operation. Raises an exception if the operation fails.

        Parameters:
             records (list of dict): The issue records.

        Returns:
            (list of dict): A representation of the created issues.
        '''
        url = f"{self._base_url}/api/complaints/issues"
        record_type = 'issues'
        return self._perform_bulk_operation(url, records, record_type)


    def bulk_insert_project_countries(self, records: List[Dict]) -> List[Dict]:
        '''
        Inserts finalized project-country records into the database using
        a bulk operation. Raises an exception if the operation fails.

        Parameters:
             records (list of dict): The project-country records.

        Returns:
            (list of dict): A representation of the created project-country rows.
        '''
        url = f"{self._base_url}/api/countries/project-countries"
        record_type = 'project-countries'
        return self._perform_bulk_operation(url, records, record_type)


    def bulk_upsert_companies(self, records: List[Dict]) -> List[Dict]:
        '''
        '''
        url = f"{self._base_url}/api/form13f/companies"
        record_type = 'companies'
        return self._perform_bulk_operation(url, records, record_type)


    def bulk_upsert_forms(self, records: List[Dict]) -> List[Dict]:
        '''
        '''
        url = f"{self._base_url}/api/form13f/forms"
        record_type = 'forms'
        return self._perform_bulk_operation(url, records, record_type)


    def bulk_upsert_investments(self, records: List[Dict]) -> List[Dict]:
        '''
        '''
        url = f"{self._base_url}/api/form13f/investments"
        record_type = 'investments'
        return self._perform_bulk_operation(url, records, record_type)


    def bulk_insert_tasks(self, tasks: List[Dict]) -> List[Dict]:
        '''
        Creates new tasks for processing web pages and inserts
        them into the database using a bulk operation. Raises an
        exception if the operation fails.

        Parameters:
            tasks (list of dict): The tasks to insert.

        Returns:
            (list of dict): A representation of the created database rows,
                to be used as messages. Fields include "id", "job_id",
                "bank", "workflow_type", and "url".
        '''
        url = f"{self._base_url}/api/pipeline/tasks"
        record_type = 'tasks'
        records, _ = self._perform_bulk_operation(url, tasks, record_type)
        return records

    
    def bulk_insert_project_sectors(self, tasks: List[Dict]) -> List[Dict]:
        '''
        Inserts finalized project-sector records into the database using
        a bulk operation. Raises an exception if the operation fails.

        Parameters:
             records (list of dict): The project-sector records.

        Returns:
            (list of dict): A representation of the created project-sector rows.
        '''
        url = f"{self._base_url}/api/sectors/project-sectors"
        record_type = 'project sectors'
        records, status = self._perform_bulk_operation(url, tasks, record_type)
        return records, status


    def insert_task(self, task: TaskRequest) -> None:
        '''
        Inserts a single task in the database. Logs any failures
        that occur rather than raising an exception.

        Parameters:
            task (TaskRequest): The task to insert.

        Returns:
            None
        '''
        url = f"{self._base_url}/api/pipeline/tasks"
        response = requests.post(url, data=vars(task))
        try:
            response_body = response.json()
        except Exception:
            response_body = None

        if not response.ok:
            response_body = json.dumps(response.json())
            raise Exception(f"Failed to create new task in database. "
                f"Received '{response.status_code} - {response.reason}' status code"
                f"{' and message ' + json.dumps(response_body) + '.' if response_body else '.'}")


    def update_task(self, task: TaskUpdate) -> None:
        '''
        Updates a task in the database.

        Parameters:
            task (TaskUpdate): The task to update.

        Returns:
            None
        '''
        url = f"{self._base_url}/api/pipeline/tasks/{task.id}"
        response = requests.patch(url, data=vars(task))

        if not response.ok:
            response_body = json.dumps(response.json())
            raise Exception(f"Failed to update task within database. "
                f"Received '{response.status_code}' status code "
                f"and message '{response_body}'.")


    def update_job(self, job: Dict) -> None:
        '''
        Updates a job in the database.

        Parameters:
            job (dict): The job to update.

        Returns:
            None
        '''
        url = f"{self._base_url}/api/pipeline/jobs/{job['id']}"
        response = requests.patch(url, data=job)

        if not response.ok:
            raise Exception(f"Failed to update job within database. "
                f"Received '{response.status_code}' status code "
                f"and message '{response.json()}'.")

        return response.json()


    def update_staged_project(self, project: Dict):
        '''
        Updates a staged project in the database.

        Parameters:
            project (dict): The project to update.

        Returns:
            None
        '''
        url = f"{self._base_url}/api/pipeline/staged-projects"
        response = requests.patch(url, data=project)

        if not response.ok:
            raise Exception(f"Failed to update project within database. "
                f"Received '{response.status_code}' status code.")


    def get_banks(self) -> List[Dict]:
        '''
        Retrieves all development banks from the database.

        Parameters:
            None

        Returns:
            (list of dict): The list of bank records.
        '''
        url = f"{self._base_url}/api/banks"
        record_type = "banks"
        return self._get_records(url, record_type)


    def get_countries(self) -> List[Dict]:
        '''
        Retrieves all countries from the database.

        Parameters:
            None

        Returns:
            (list of dict): The list of country records.
        '''
        url = f"{self._base_url}/api/countries?fields=id,name,iso_code"
        record_type = "countries"
        return self._get_records(url, record_type)


    def get_staged_projects(self, limit: int) -> List[Dict]:
        '''
        Retrieves a batch of staged projects from the database.

        Parameters:
            limit (int): The number of records to retrieve at once.

        Returns:
            (list of dict): The list of project records.
        '''
        url = f"{self._base_url}/api/pipeline/staged-projects?limit={limit}"
        record_type = "staged projects"
        return self._get_records(url, record_type)


    def get_staged_investments(self, limit: int) -> List[Dict]:
        '''
        Retrieves a batch of staged investments from the database.

        Parameters:
            limit (int): The number of records to retrieve at once.

        Returns:
            (list of dict): The list of staged investment records.
        '''
        url = f"{self._base_url}/api/pipeline/staged-investments?limit={limit}"
        record_type = "staged investments"
        return self._get_records(url, record_type)


    def get_sectors(self) -> List[Dict]:
        '''
        Retrieves all sectors from the database.

        Parameters:
            None

        Returns:
            (list of dict): The list of sector records.
        '''
        url = f"{self._base_url}/api/sectors"
        record_type = 'sectors'
        return self._get_records(url, record_type)
    

    def delete_staged_investments(self, ids: List[int]) -> int:
        '''
        Deletes staged investments based on id.

        Parameters:
            ids (list of int): The unique identifiers of the
                staged investments to delete.

        Returns:
            (int): The number of records deleted.
        '''
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


    def delete_staged_projects(self, ids: List[int]) -> int:
        '''
        Deletes staged projects based on id.

        Parameters:
            ids (list of int): The unique identifiers of the
                staged projects to delete.

        Returns:
            (int): The number of records deleted.
        '''
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



if __name__ == "__main__":
    from services.logger import DebitLogger
    logger = DebitLogger()
    DbClient(logger)