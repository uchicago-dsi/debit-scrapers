"""Web scrapers for Form 13F of the U.S.
Securities and Exchange Commission (SEC).

References:
- https://www.investor.gov/introduction-investing/investing-basics/glossary/form-13f-reports-filed-institutional-investment
- https://www.sec.gov/edgar/sec-api-documentation
- https://www.sec.gov/pdf/form13f.pdf
"""

import json
import re
from bs4 import BeautifulSoup
from datetime import datetime
from json.decoder import JSONDecodeError
from logging import Logger
from pipeline.models.task import TaskUpdate
from pipeline.constants import (
    COMPLETED_STATUS,
    DYNAMIC_WORKFLOW,
    ERROR_STATUS,
    FORM_13F_ABBREVIATION,
    FORM_13F_ARCHIVED_HISTORY_WORKFLOW,
    FORM_13F_COMPANY_FPATH,
    FORM_13F_HISTORY_WORKFLOW,
    FORM_13F_WORKFLOW,
    NOT_STARTED_STATUS
)
from pipeline.scrapers.abstract import BaseWorkflow, SeedUrlsWorkflow
from pipeline.services.database import DbClient
from pipeline.services.web import DataRequestClient
from pipeline.services.pubsub import PublisherClient
from typing import Dict, List, Tuple


class Form13FSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Retrieves the first set of Form 13F URLs to scrape.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        pubsub_client: PublisherClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of a `Form13FSeedUrlsWorkflow`.

        Args:
            data_request_client (`DataRequestClient`): A client
                for making HTTP GET requests while adding
                random delays and rotating user agent headers.
                
            pubsub_client (`PubSubClient`): A wrapper client for the 
                Google Cloud Platform Pub/Sub API. Configured to
                publish messages to the appropriate 'tasks' topic.

            db_client (`DbClient`): A client used to insert and
                update tasks in the database.

            logger (`Logger`): An instance of the logging class.

        Returns:
            `None`
        """
        super().__init__(data_request_client, pubsub_client, db_client, logger)

    @property
    def next_workflow(self) -> str:
        """The name of the workflow to execute after this
        workflow has finished.
        """
        return FORM_13F_HISTORY_WORKFLOW

    @property
    def entity_submissions_history_base_url(self) -> str:
        """The base URL for an entity's submissions history,
        which includes 13F among other forms. The Central
        Index Key (CIK) in the URL is a 10-digit identifier
        with leading zeroes.
        """
        return 'https://data.sec.gov/submissions/CIK{central_index_key}.json'

    def generate_seed_urls(self) -> List[str]:
        """Generates the set of seed URLs to scrape (i.e.,
        submission histories for the configured companies).

        Args:
            None

        Returns:
            (`list` of `str`): The unique list of URLs.
        """
        # Load companies to search from file
        try:
            with open(FORM_13F_COMPANY_FPATH) as f:
                companies = json.load(f)
        except Exception as e:
            raise Exception("Failed to seed Form 13F URLs. Error "
                f"loading Form 13F companies from file. {e}")

        # Generate URLs to company submission histories
        try:
            urls = [
                self.entity_submissions_history_base_url.format(
                    central_index_key=c['cik']
                )
                for c in companies
            ]
        except KeyError as e:
            raise Exception("Failed to seed Form 13F URLs. Company "
                "JSON improperly formed. Expected property 'cik'.")

        return urls

class Form13FHistoryScrapeWorkflow(BaseWorkflow):
    """Parses Form 13F URLs from a company's SEC submission history.
    """
    
    def __init__(
        self,
        data_request_client: DataRequestClient,
        pubsub_client: PublisherClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of a `Form13FHistoryScrapeWorkflow`.

        Args:
            data_request_client (`DataRequestClient`): A client
                for making HTTP GET requests while adding
                random delays and rotating user agent headers.

            pubsub_client (`PubSubClient`): A wrapper client for the 
                Google Cloud Platform Pub/Sub API. Configured to
                publish messages to the appropriate 'tasks' topic.

            db_client (`DbClient`): A client used to insert and
                update tasks in the database.

            logger (`logging.Logger`): An instance of the logging class.

        Returns:
            `None`
        """
        super().__init__(logger)
        self._data_request_client = data_request_client
        self._pubsub_client = pubsub_client
        self._db_client = db_client

    @property
    def archived_history_base_url(self) -> str:
        """The base URL for an entity's archived submissions
        history. The `archive_file_name` has a format like:
        `CIK0001067983-submissions-001.json`.
        """
        return 'https://data.sec.gov/submissions/{archive_file_name}'

    @property
    def filed_form_base_url(self) -> str:
        """The base URL for a company's filed Form 13F document.
        """
        return 'https://www.sec.gov/Archives/edgar/data/{cik}/{formatted_sec_acc_no}/{sec_acc_no}-index.htm'

    @property
    def next_workflow(self) -> str:
        """The name of the workflow to execute after this workflow 
        has finished. In this case, either one of two choices are 
        possible based on runtime values.
        """
        return DYNAMIC_WORKFLOW

    def scrape_filing_history(self, url: str) -> Tuple[List[str], List[str]]:
        """Requests a company's SEC submissions history as JSON
        and then parses that JSON to retrieve URLs for summaries
        of filed Form 13F documents.

        Args:
            url (`str`): The URL to a company's SEC
                submission history.

        Returns:
            (`list` of `str`, `list` of `str`): A two-item tuple consisting
                of a list of recent Form 13F filing URLs and a list of
                archived Form 13F filing URLs.
        """
        # Retrieve company filing history as JSON
        response = self._data_request_client.get(
            url,
            use_random_delay=True,
            min_random_delay=1,
            max_random_delay=10,
            custom_headers={
                "User-Agent": "University of Chicago launagreer@uchicago.edu",
                "Accept-Encoding": "gzip, deflate",
                "Host": "data.sec.gov"
            }
        )
        if not response.ok:
            raise RuntimeError("An error occurred retrieving Form 13F "
                f"company filing history. The call to \"{url}'\" returned "
                f"a \"{response.status_code}-{response.reason}\" status code "
                "and text \"" + response.text + "\"." if response.text else ".")            
        
        # Parse JSON
        try:
            data = response.json()
            cik = data['cik']
            archived_filings = data['filings']['files']
            recent_filings = data['filings']['recent']
            form_type = recent_filings['form']
            accession_number = recent_filings['accessionNumber']
            filing_date = recent_filings['filingDate']
        except JSONDecodeError as e:
            raise RuntimeError(f"Could not decode JSON. {e}") from e
        except KeyError as e:
            raise Exception("Response JSON is in an unexpected format. "
                f"Missing key {e}.")

        # Generate URLs from recent filings
        filing_page_urls = []
        num_filings = len(accession_number)
        encountered_old_format_info_table = False
        def parse_date(date_str: str) -> datetime:
            if not date_str:
                return None
            return datetime.strptime(date_str, '%Y-%m-%d')

        for i in range(num_filings):

            # Only examine Form 13F-HR records
            if form_type[i] == FORM_13F_ABBREVIATION:

                # Filter to those filed on or after 2013,
                # when the Form 13F information table existed
                # in a parseable format (i.e., either HTML or XML)
                parsed_date = parse_date(filing_date[i])

                if parsed_date and parsed_date.year < 2013:
                    encountered_old_format_info_table = True
                    continue

                # If both criteria are met, add URL to list
                acc_no = accession_number[i]
                url = self.filed_form_base_url.format(
                    cik=cik,
                    sec_acc_no=acc_no,
                    formatted_sec_acc_no=acc_no.replace('-', '')
                )
                filing_page_urls.append(url)

        # Generate URLs from archived filings. If an information
        # table in an old format (e.g., ".txt") was already encountered
        # in the list of recent filings, don't queue processing of
        # archived files.
        if not encountered_old_format_info_table:
            archived_filing_urls = [
                self.archived_history_base_url.format(
                    archive_file_name=f['name']
                )
                for f in archived_filings
            ]
        else:
            archived_filing_urls = []

        return filing_page_urls, archived_filing_urls

    def execute(
        self,
        message_id: str,
        num_delivery_attempts: int,
        job_id: str,
        task_id: str,
        form: str,
        url: str) -> None:
        """Executes the workflow.

        Args:
            message_id (`str`): The assigned id for the Pub/Sub message.

            num_delivery_attempts (`int`): The number of times the
                Pub/Sub message has been delivered without being
                acknowledged.

            job_id (`int`): The unique identifier for the processing
                job that encapsulates all loading, scraping, and
                cleaning tasks related to form data.

            task_id (`str`): The unique identifier for the current 
                scraping task.

            form (`str`): The name of the financial form to scrape.

            url (`list` of `str`): The URL to scrape.

        Returns:
            `None`
        """
        # Begin tracking updates for current task
        task_update = TaskUpdate()
        task_update.id = task_id
        task_update.processing_start_utc = datetime.utcnow()
        task_update.retry_count = num_delivery_attempts - 1
        self._logger.info(f"Processing job '{job_id}', form '{form}', "
            f"task '{task_id}', message '{message_id}'.")

        try:
            # Parse form history JSON to retrieve URLs to
            # "recently"-submitted Form 13F webpages, as 
            # well as URLs to archived Form 13F submissions JSON.
            try:
                task_update.scraping_start_utc = datetime.utcnow()
                form_urls, archived_form_submission_urls = self.scrape_filing_history(url)
                task_update.scraping_end_utc = datetime.utcnow()
            except Exception as e:
                raise Exception(f"Failed to scrape company submissions history. {e}")

            try:
                # Prepare for creation of new tasks
                tasks = []
                all_urls = form_urls + archived_form_submission_urls
                last_filing_url_idx = len(form_urls) - 1

                for idx in range(len(all_urls)):
                    # Determine next workflow type
                    if idx <= last_filing_url_idx:
                        workflow_type = FORM_13F_WORKFLOW
                    else:
                        workflow_type = FORM_13F_ARCHIVED_HISTORY_WORKFLOW

                    # Create task and add to list
                    tasks.append({
                        "job_id": job_id,
                        "status": NOT_STARTED_STATUS,
                        "source": form,
                        "url": all_urls[idx],
                        "workflow_type": workflow_type
                    })

                # Insert new tasks in database
                messages = self._db_client.bulk_create_tasks(tasks)

            except Exception as e:
                raise Exception("Failed to insert new tasks for scraping "
                    f"Form 13F filing pages and archived history pages. {e}")

            # Publish task messages to Pub/Sub for other nodes to pick up
            try:
                for msg in messages:
                    self._pubsub_client.publish_message(msg)
            except Exception as e:
                raise Exception(f"Failed to publish all {len(messages)} "
                    f"messages to Pub/Sub. {e}")

        except Exception as e:
            # Log error
            error_message = "Form 13F history scraping workflow " \
                f"failed for message {message_id}. {e}"
            self._logger.error(error_message)

            # Record task failure in database
            task_update.status = ERROR_STATUS
            task_update.last_failed_at_utc = datetime.utcnow()
            task_update.last_error_message = error_message
            self._db_client.update_task(task_update)

            # Bubble up error
            raise Exception(error_message)

        # Record task success in database
        task_update.status = COMPLETED_STATUS
        task_update.processing_end_utc = datetime.utcnow()
        self._db_client.update_task(task_update)
                
class Form13FArchiveScrapeWorkflow(BaseWorkflow):
    """Scrapes a JSON payload containing URLs for
    archived Form 13F filings.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        pubsub_client: PublisherClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of a `Form13FArchiveScrapeWorkflow`.

        Args:
            data_request_client (`DataRequestClient`): A client
                for making HTTP GET requests while adding
                random delays and rotating user agent headers.

            pubsub_client (`PubSubClient`): A wrapper client for the 
                Google Cloud Platform Pub/Sub API. Configured to
                publish messages to the appropriate "tasks" topic.

            db_client (`DbClient`): A client used to insert and
                update tasks in the database.

            logger (`logging.Logger`): An instance of the logging class.

        Returns:
            `None`
        """
        super().__init__(logger)
        self._data_request_client = data_request_client
        self._pubsub_client = pubsub_client
        self._db_client = db_client

    @property
    def filed_form_base_url(self) -> str:
        """The base URL for a company's filed Form 13F document.
        """
        return "https://www.sec.gov/Archives/edgar/data/{cik}/{formatted_sec_acc_no}/{sec_acc_no}-index.htm"

    @property
    def next_workflow(self) -> str:
        """The name of the workflow to execute after this
        workflow has finished.
        """
        return FORM_13F_WORKFLOW

    def parse_archived_submissions(self, url: str) -> List[str]:
        """Requests a company's SEC archived submission history as JSON
        and then parses that JSON to generate URLs for archived
        filing pages.

        Args:
            url (`str`): The URL to a company's archived SEC
                submission history.

        Returns:
            (`list` of `str`): A list of archived Form 13F filing pages.
        """
        # Retrieve company filing history as JSON
        response = self._data_request_client.get(
            url,
            use_random_delay=True,
            min_random_delay=1,
            max_random_delay=10,
            custom_headers={
                "User-Agent": "University of Chicago launagreer@uchicago.edu",
                "Accept-Encoding": "gzip, deflate",
                "Host": "data.sec.gov"
            }
        )
        if not response.ok:
            raise Exception("An error occurred retrieving Form 13F "
                f"company filing history. The call to '{url}' returned "
                f"a '{response.status_code}-{response.reason}' status code"
                " and text '" + response.text + "'." if response.text else ".")
        
        # Parse JSON
        try:
            data = response.json()
            form_type = data['form']
            accession_number = data['accessionNumber']
        except JSONDecodeError as e:
            raise Exception(f"Could not decode JSON. {e}")
        except KeyError as e:
            raise Exception("Response JSON is in an unexpected format. "
                f"Missing key {e}.")

        # Parse company CIK from URL
        cik_regex = r"(?<=CIK)([0-9]){10}(?=-)"
        cik = re.search(cik_regex, url).group(0)

        # Generate URLs to archived filing pages
        filing_page_urls = []
        for i in range(len(accession_number)):
            if form_type[i] == FORM_13F_ABBREVIATION:
                acc_no = accession_number[i]
                url = self.filed_form_base_url.format(
                    cik=cik,
                    sec_acc_no=acc_no,
                    formatted_sec_acc_no=acc_no.replace('-', '')
                )
                filing_page_urls.append(url)

        return filing_page_urls

    def execute(
        self,
        message_id: str,
        num_delivery_attempts: int,
        job_id: str,
        task_id: str,
        form: str,
        url: str) -> None:
        """Executes the workflow.

        Args:
            message_id (`str`): The assigned id for the Pub/Sub message.

            num_delivery_attempts (`int`): The number of times the
                Pub/Sub message has been delivered without being
                acknowledged.

            job_id (`int`): The unique identifier for the processing
                job that encapsulates all loading, scraping, and
                cleaning tasks related to form data.

            task_id (`str`): The unique identifier for the current 
                scraping task.

            form (`str`): The name of the financial form to scrape.

            url (`list` of `str`): The URL to scrape.

        Returns:
            `None`
        """
        # Begin tracking updates for current task
        task_update = TaskUpdate()
        task_update.id = task_id
        task_update.processing_start_utc = datetime.utcnow()
        task_update.retry_count = num_delivery_attempts - 1
        self._logger.info(f"Processing job '{job_id}', form '{form}', "
            f"task '{task_id}', message '{message_id}'.")

        try:
            # Parse archived submission payload for individual Form 13F URLs
            try:
                task_update.scraping_start_utc = datetime.utcnow()
                project_page_urls = self.parse_archived_submissions(url)
                task_update.scraping_end_utc = datetime.utcnow()
            except Exception as e:
                raise Exception(f"Failed to parsed archived Form 13F URLs. {e}")

            # Insert new tasks for scraping Form 13F pages into database
            try:
                payload = []
                for url in project_page_urls:
                    payload.append({
                        "job_id": job_id,
                        "status": NOT_STARTED_STATUS,
                        "source": form,
                        "url": url,
                        "workflow_type": self.next_workflow
                    })
                project_page_messages = self._db_client.bulk_insert_tasks(payload)
            except Exception as e:
                raise Exception("Failed to insert new tasks for scraping "
                    f"Form 13F pages into database. {e}")
            
            # Publish task messages to Pub/Sub for other nodes to pick up
            try:
                for msg in project_page_messages:
                    self._pubsub_client.publish_message(msg)
            except Exception as e:
                raise Exception(f"Failed to publish all {len(project_page_messages)} "
                    f"messages to Pub/Sub. {e}")

        except Exception as e:
            # Log error
            error_message = "Form 13F archived submissions scraping " \
                f"workflow failed for message {message_id}. {e}"
            self._logger.error(error_message)

            # Record task failure in database
            task_update.status = ERROR_STATUS
            task_update.last_failed_at_utc = datetime.utcnow()
            task_update.last_error_message = error_message
            self._db_client.update_task(task_update)

            # Bubble up error
            raise Exception(error_message)

        # Record task success in database
        task_update.status = COMPLETED_STATUS
        task_update.processing_end_utc = datetime.utcnow()
        self._db_client.update_task(task_update)

class Form13FInvestmentScrapeWorkflow(BaseWorkflow):
    """Retrieves a Form 13F info table and scrapes it for
    data to load into the database if it exists.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of a `Form13FInvestmentScrapeWorkflow`.

        Args:
            data_request_client (`DataRequestClient`): A client
                for making HTTP GET requests while adding
                random delays and rotating user agent headers.

            db_client (`DbClient`): A client for inserting and
                updating tasks in the database.

            logger (`logging.Logger`): An instance of the logging class.

        Returns:
            `None`
        """
        super().__init__(logger)
        self._data_request_client = data_request_client
        self._db_client = db_client

    @property
    def next_workflow(self) -> str:
        """The name of the workflow to execute, if any.
        """
        return None

    @property
    def us_sec_base_url(self) -> str:
        """The base URL for the U.S. Securities and Exchange Commission website.
        """
        return 'https://www.sec.gov'


    def _parse_form_home_page(self, url: str) -> Tuple[Dict, str]:
        """Requests a company's Form 13F home page and
        parses the HTML to retrieve the URL of the
        form's information table.

        Args:
            url (`str`): The URL to the filing page.

        Returns:
            (dict, str): A two-item tuple consisting of (1)
                a dictionary of parsed filing data and
                (2) the URL to a filing's information table. 
        """
        # Retrieve company 13F form webpage as HTML
        response = self._data_request_client.get(
            url,
            use_random_delay=True,
            min_random_delay=1,
            max_random_delay=10,
            custom_headers={
                "User-Agent": "University of Chicago launagreer@uchicago.edu",
                "Accept-Encoding": "gzip, deflate",
                "Host": "www.sec.gov"
            }
        )
        if not response.ok:
            raise Exception("An error occurred retrieving Form 13F "
                f"company filing page. The call to '{url}' returned "
                f"a '{response.status_code}-{response.reason}' status code"
                " and text '" + response.text + "'." if response.text else ".")

        # Parse webpage into BeautifulSoup object
        soup = BeautifulSoup(response.text, 'html.parser')

        # Retrieve form name
        filing_info_div = soup.find(id="formDiv")
        form_name = filing_info_div.find("strong").text
        filing_data = {"form_name": form_name}

        # Retrieve form dates
        info_headers = filing_info_div.find_all("div", {"class": "infoHead"})
        for header in info_headers:
            header_name = header.text.strip()
            value = header.find_next_sibling("div").text.strip()
            filing_data[header_name] = value

        # Retrieve company name associated with filing.
        # NOTE: The name of a company may change over time.
        company_name = soup.find("span", {"class", "companyName"}).text.strip()
        filing_data["company_name"] = company_name

        # Retrieve URL to information table
        table = soup.find("table", {"class": "tableFile"})
        info_type_cells = table.find_all("td", string="INFORMATION TABLE")

        info_tbl_url = None
        for cell in info_type_cells:
            info_tbl_cell = cell.parent.find("td", string=re.compile(".*html"))
            if info_tbl_cell:
                info_tbl_partial_url = info_tbl_cell.find("a")['href']
                info_tbl_url = f"{self.us_sec_base_url}{info_tbl_partial_url}"
                break

        return filing_data, info_tbl_url

    def _parse_url(self, url: str) -> Tuple[str, str]:
        """Parses a URL to a form home page to retrieve a
        company's CIK and the form's SEC Accession Number.

        Args:
            url (`str`): The url.

        Returns:
            (`str`, `str`): A two-item tuple consisting of the
                CIK and accession number. 
        """
        cik_regex = r"(?<=/data/)([0-9])*(?=/)"
        cik = re.search(cik_regex, url).group(0).zfill(10)
        sec_acc_no = url.split('/')[-1].replace('-index.htm', '')

        return cik, sec_acc_no

    def scrape_investments(self, url: str) -> List[Dict]:
        """Orchestrates the creation of Form 13F staged equity
        investment entities by scraping the form home page
        and information table page for data.

        Args:
            url (`str`): The URL to the form home page.

        Returns:
            (`list` of `dict`): The investments.
        """
        # Parse URL string for company CIK and SEC accession number
        company_cik, filing_acc_no = self._parse_url(url)

        # Request form home page and parse HTML for metadata and info table URL
        filing_metadata, info_table_url = self._parse_form_home_page(url)

        # Return empty list of investment records if no table available
        if not info_table_url:
            return []

        # Otherwise, request table
        response = self._data_request_client.get(
            info_table_url,
            use_random_delay=True,
            min_random_delay=1,
            max_random_delay=10,
            custom_headers={
                "User-Agent": "University of Chicago launagreer@uchicago.edu",
                "Accept-Encoding": "gzip, deflate",
                "Host": "www.sec.gov"
            }
        )
        if not response.ok:
            raise Exception("An error occurred retrieving Form 13F "
                f"company information table. The call to '{url}' returned "
                f"a '{response.status_code}-{response.reason}' status code"
                " and text '" + response.text + "'." if response.text else ".")

        # Parse webpage into BeautifulSoup object
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract investment rows from HTML
        table = soup.find("table", summary="Form 13F-NT Header Information")
        num_header_rows = 3
        investment_rows = table.find_all('tr')[num_header_rows:]

        # Define local functions for cleaning
        def format_date(date_str: str) -> str:
            """Local function to format a datetime string
            as a simple date string ('yyyy-mm-dd') if
            the date exists.

            Args:
                date_str (`str`): The raw date string.

            Returns:
                (`str`): The cleaned date string.
            """
            if not date_str:
                return None

            date_str_regex = r'[0-9]{4}-[0-9]{2}-[0-9]{2}'
            matches = re.search(date_str_regex, date_str)
            return matches.group(0) if matches else None

        def replace_null(value: str, replacement_value: str=None) -> str:
            """Local function to replace null or blank 
            unicode values with a replacement value 
            that defaults to `None`.

            Args:
                value (`str`): The raw string.

            Returns:
                (`str`): The cleaned string.
            """
            is_null = value == '\u00a0' or value == ''
            return replacement_value if is_null else value

        def correct_company_name(name: str) -> str:
            """Local function to strip additional metadata 
            from the company name field.

            Args:
                name (`str`): The raw name.

            Returns:
                (`str`): The cleaned name.
            """
            company_regex = r'.*(?=[\s][(]{1}Filer[)]{1})'
            matches = re.search(company_regex, name)
            return matches.group(0) if matches else None

        def get_digits(value: str) -> int:
            """Local function to convert a string representation of a
            number into an integer by removing commas and decimals.

            Args:
                value (`str`): The number string.

            Returns:
                (`int`): The parsed number
            """
            try:
                stripped = value.replace(',', '').replace('.', '')
                return int(stripped)
            except Exception:
                raise Exception(f"Value '{value}' could not be "
                    "coerced into an integer.")

        # Generate investments
        investments = []
        for row in investment_rows:
            cells = row.find_all('td')
            investments.append({
                'company_cik': company_cik,
                'company_name': correct_company_name(filing_metadata['company_name']),
                'form_name': filing_metadata['form_name'],
                'form_accession_number': filing_acc_no,
                'form_report_period': format_date(filing_metadata.get('Period of Report')),
                'form_filing_date': format_date(filing_metadata.get('Filing Date')),
                'form_acceptance_date': format_date(filing_metadata.get('Accepted')),
                'form_effective_date': format_date(filing_metadata.get('Effectiveness Date')),
                'form_url': url,
                'stock_issuer_name': cells[0].text,
                'stock_title_class': cells[1].text,
                'stock_cusip': cells[2].text,
                'stock_value_x1000': get_digits(cells[3].text),
                'stock_shares_prn_amt': get_digits(cells[4].text),
                'stock_sh_prn': cells[5].text,
                'stock_put_call': replace_null(cells[6].text),
                'stock_investment_discretion': replace_null(cells[7].text),
                'stock_manager': replace_null(cells[8].text, replacement_value=''),
                'stock_voting_auth_sole': get_digits(cells[9].text),
                'stock_voting_auth_shared': get_digits(cells[10].text),
                'stock_voting_auth_none': get_digits(cells[11].text)
            })

        return investments

    def execute(
        self,
        message_id: str,
        num_delivery_attempts: int,
        job_id: str,
        task_id: str,
        form: str,
        url: str) -> None:
        """Executes the workflow.

        Args:
            message_id (`str`): The assigned id for the Pub/Sub message.

            num_delivery_attempts (`int`): The number of times the
                Pub/Sub message has been delivered without being
                acknowledged.

            job_id (`int`): The unique identifier for the processing
                job that encapsulates all loading, scraping, and
                cleaning tasks related to form data.

            task_id (`str`): The unique identifier for the current 
                scraping task.

            form (`str`): The name of the financial form to scrape.

            url (`list` of `str`): The URL to scrape.

        Returns:
            `None`
        """
        # Begin tracking updates for current task
        task_update = TaskUpdate()
        task_update.id = task_id
        task_update.processing_start_utc = datetime.utcnow()
        task_update.retry_count = num_delivery_attempts - 1
        self._logger.info(f"Processing job '{job_id}', form '{form}', "
            f"task '{task_id}', message '{message_id}'.")

        try:
            # Extract investment records
            try:
                task_update.scraping_start_utc = datetime.utcnow()
                investment_records = self.scrape_investments(url)
                task_update.scraping_end_utc = datetime.utcnow()
            except Exception as e:
                raise Exception(f"Failed to scrape investment. {e}")

            # Insert record(s) into database
            try:
                for r in investment_records:
                    r['task_id'] = task_update.id
                self._db_client.bulk_insert_staged_investments(investment_records)
            except Exception as e:
                raise Exception("Failed to insert investment record(s) "
                    f" into database. {e}")
               
        except Exception as e:
            # Log error
            error_message = f"{form} investment scraping workflow " \
                f"failed for message {message_id}. {e}"
            self._logger.error(error_message)

            # Record task failure in database
            task_update.status = ERROR_STATUS
            task_update.last_failed_at_utc = datetime.utcnow()
            task_update.last_error_message = error_message
            self._db_client.update_task(task_update)

            # Bubble up error
            raise Exception(error_message)

        # Record task success in database
        task_update.status = COMPLETED_STATUS
        task_update.processing_end_utc = datetime.utcnow()
        self._db_client.update_task(task_update)
