"""A script to pull web scraping tasks from a 
database and publish-subscribe ("Pub/Sub") service,
process the tasks in parallel, and then persist
the results to a database.
"""

import concurrent.futures
import json
import os
import yaml
from datetime import datetime
from google.api_core import retry
from google.cloud import pubsub_v1
from google.pubsub_v1.types.pubsub import PullResponse, ReceivedMessage
from google.pubsub_v1.services.subscriber.client import SubscriberClient
from scrapers.constants import (
    COMPLETED_STATUS,
    DEV_ENV,
    DOWNLOAD_WORKFLOW,
    ENV,
    PROJECT_PAGE_WORKFLOW,
    PROJECT_PARTIAL_PAGE_WORKFLOW,
    RESULTS_PAGE_MULTISCRAPE_WORKFLOW,
    RESULTS_PAGE_WORKFLOW,
    SEED_URLS_WORKFLOW,
    USER_AGENT_HEADERS_FPATH,
)
from scrapers.services.data_request import DataRequestClient
from scrapers.services.database import DbClient
from scrapers.services.logger import LoggerFactory
from scrapers.services.pubsub import PubSubClient
from scrapers.services.registry import scraper_registry
from typing import List
from yaml.loader import FullLoader


#region ----------------Setup---------------------

# Create logger
logger = LoggerFactory.get("run-workflows")

# Retrieve development environment
env = os.getenv(ENV, DEV_ENV)

# Load corresponding configuration file
logger.info(f"Loading configuration file for '{env}' environment.")
with open(f"config.{env}.yaml", "r") as stream:
    try:
        config: dict = yaml.load(stream, Loader=FullLoader)
    except yaml.YAMLError as e:
        raise Exception(f"Failed to open configuration file. {e}")

# Parse config values
logger.info("Parsing config values.")
try:
    project_id = config["google_cloud"]["project_id"]
    max_num_workers = config["data_retrieval"]["max_num_workers"]
    pubsub_config = config["google_cloud"]["pubsub"]
    data_retrieval_topic_id = pubsub_config["data_retrieval_topic_id"]
    data_retrieval_subscription_id = pubsub_config["data_retrieval_subscription_id"]
    data_cleaning_topic_id = pubsub_config["data_cleaning_topic_id"]
    data_retrieval_topic_id = pubsub_config["data_retrieval_topic_id"]
    max_num_received_messages = pubsub_config["max_num_received_messages"]
    publish_timeout_in_seconds = pubsub_config["publish_timeout_in_seconds"]
    retry_deadline_in_seconds = pubsub_config["retry_deadline_in_seconds"]
except KeyError as e:
    raise Exception(f"Missing expected Pub/Sub configuration value. {e}")

# Set up database client for create and update
# operations against tasks and retrieved records
logger.info("Creating database client.")
db_client = DbClient(logger)

# Set up DataRequestClient to rotate HTTP headers and add random delays
with open(USER_AGENT_HEADERS_FPATH, "r") as stream:
    try:
        user_agent_headers = json.load(stream)
        data_request_client = DataRequestClient(user_agent_headers)
    except yaml.YAMLError as e:
        raise Exception(f"Failed to open configuration file. {e}")
        
# Set up Pub/Sub topic client for publishing data processing tasks
logger.info("Setting up client for managing data retrieval Pub/Sub topic.")
workflows_pubsub_client = PubSubClient(
    logger=logger,
    project_id=project_id,
    topic_id=data_retrieval_topic_id,
    publish_timeout_in_seconds=publish_timeout_in_seconds
)

# Set up Pub/Sub topic client for publishing
# alert message for data cleaning process
logger.info("Setting up client for managing data cleaning Pub/Sub topic.")
data_cleaning_pubsub_client = PubSubClient(
    logger=logger,
    project_id=project_id,
    topic_id=data_cleaning_topic_id,
    publish_timeout_in_seconds=publish_timeout_in_seconds
)

# Connect to topic subscriber
logger.info("Connecting to data retrieval topic subscriber.")
subscriber: SubscriberClient = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(
    project=project_id,
    subscription=data_retrieval_subscription_id
)

#endregion ----------------Setup---------------------


def main() -> None:
    """Executes workflows for retrieving development bank projects
    and government agency form submissions through download links,
    web scraping, and API queries in response to messages sent
    from Pub/Sub. Saves resulting records to database tables.

    Args:
        logger (`Logger`): A logger instance.

    Returns:
        None
    """
    # Initialize variables and helper function
    encountered_jobs = set()
    messages_in_previous_batch = False

    def complete_message(msg):
        """Local function to process and acknowledge single message.
        """
        try:
            request = { "subscription": subscription_path, "ack_ids": [msg.ack_id] }
            job_id = process_message(msg, workflows_pubsub_client, db_client, data_request_client)
            encountered_jobs.add(job_id)
            subscriber.acknowledge(request)
        except Exception as e:
            logger.error(f"Failed to process message: {e}")

    with subscriber:
        while True:     
            # Pull message batch from topic
            logger.info("Pulling batch of new messages.")
            response: PullResponse = subscriber.pull(
                request={
                    "subscription": subscription_path,
                    "max_messages": max_num_received_messages
                },
                retry=retry.Retry(deadline=retry_deadline_in_seconds)
            )
            messages_received = len(response.received_messages)

            # If messages exist, process using multithreading
            if messages_received:
                logger.info(f"Processing {messages_received} message(s).")
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_num_workers) as executor:
                    try:
                        executor.map(complete_message, response.received_messages)
                    except Exception as e:
                        logger.error(e)
                messages_in_previous_batch = True

            # Trigger cleaning of retrieved records if no more messages are present
            elif messages_in_previous_batch:
                logger.info("End of new messages after previous "
                    "message batch. Requesting data cleaning for "
                    "retrieved staged project records.")
                audit(list(encountered_jobs), db_client, data_cleaning_pubsub_client)
                encountered_jobs = set()
                messages_in_previous_batch = False
                continue

            # Otherwise, log absence of messages to process
            else:
                logger.info("No new messages to process.")


def process_message(
    received_message: ReceivedMessage,
    pubsub_client: PubSubClient,
    db_client: DbClient,
    data_request_client: DataRequestClient) -> None:
    """Processes a single message to scrape or download
    data from a URL.

    Args:
        received_message (`ReceivedMessage`): The Google Pub/Sub message.
        
        pubsub_client (`PubSubClient`): The Google Pub/Sub client.

        db_client (`DbClient`): The database client.

        data_request_client (`DataRequestClient`): A client
            for making HTTP GET requests while adding
            random delays and rotating user agent headers.

    Returns:
        None
    """
    # Retrieve message metadata
    message_id = received_message.message.message_id
    num_delivery_attempts = received_message.delivery_attempt

    # Decode and extract message data
    try:
        data = json.loads(received_message.message.data.decode('utf'))
        task_id = data['id']
        job_id = data['job_id']
        source = data['source']
        workflow_type = data['workflow_type']
        url = data['url']
    except KeyError as e:
        raise Exception(f"Failed to extract data from Pub/Sub message. {e}")

    # Fetch workflow class type from registry
    source_workflow = f"{source}-{workflow_type}"
    logger = LoggerFactory.get(f"run-workflows - {source}")
    try:
        registered_workflow = scraper_registry[source_workflow]
    except KeyError:
        raise Exception(f"Invalid input workflow encountered: "
            f"{source_workflow}. All scraping workflows must "
            "be properly registered.")

    # Instantiate workflow
    if workflow_type == DOWNLOAD_WORKFLOW:
        w = registered_workflow(data_request_client, db_client, logger)
    elif workflow_type == PROJECT_PAGE_WORKFLOW:
        w = registered_workflow(data_request_client, db_client, logger)
    elif workflow_type == PROJECT_PARTIAL_PAGE_WORKFLOW:
        w = registered_workflow(data_request_client, db_client, logger)
    elif workflow_type == RESULTS_PAGE_MULTISCRAPE_WORKFLOW:
        w = registered_workflow(data_request_client, pubsub_client, db_client, logger)
    elif workflow_type == RESULTS_PAGE_WORKFLOW:
        w = registered_workflow(data_request_client, pubsub_client, db_client, logger)
    elif workflow_type == SEED_URLS_WORKFLOW:
        w = registered_workflow(pubsub_client, db_client, logger)
    else:
        raise Exception(f"Invalid workflow type encountered: {workflow_type}.")

    # Excecute workflow
    w.execute(
        message_id,
        num_delivery_attempts,
        job_id,
        task_id,
        source,
        url
    )

    return job_id


def audit(
    encountered_jobs: List[int],
    db_client: DbClient,
    data_cleaning_pubsub_client: PubSubClient) -> None:
    """Marks the end of workflow processing by updating the status
    of the affected job(s) in the database and publishing
    Pub/Sub messages to trigger the next job stage: cleaning and
    merging records.

    Args:
        encountered_jobs (list of int): The job ids encountered
            during processing.

        db_client (`DbClient`): A client providing access to the
            database holding pipeline job records.

        data_cleaning_pubsub_client (`PubSubClient`): A client
            providing access to a topic for data cleaning.

    Returns:
        None
    """
    # Mark end of workflow processing
    stage_completed_utc = datetime.utcnow()
    stage_completed_utc_str = datetime.strftime(stage_completed_utc, '%Y_%m_%d_%H_%M_%S')

    # If no more messages are received within the timeout window,
    # publish a Pub/Sub message to trigger data cleaning
    # of staged records.
    try:
        for job_id in encountered_jobs:
            logger.info("Publishing Pub/Sub message for completion "
                f"of data collection stage for job '{job_id}'.")
            success_msg = {
                "job_id": job_id,
                "time_completed_utc": stage_completed_utc_str
            }
            data_cleaning_pubsub_client.publish_message(success_msg)
    except Exception as e:
        msg = "Failed to publish notification signaling " \
            f"end of data collection stage. {e}"
        logger.error(msg)
        raise Exception(msg)

    # Update status(es) of job(s) in database
    for job_id in encountered_jobs:
        try:
            logger.info("Marking data collection stage as complete for "
                f"job {job_id} in database.")
            job_update = {
                "id": job_id,
                "data_load_stage": COMPLETED_STATUS,
                "data_load_end_utc": stage_completed_utc
            }
            db_client.update_job(job_update)
        except Exception as e:
            msg = f"Failed to update status of job '{job_id}' in database " \
                "following completion of data collection stage."
            logger.error(msg)
            raise Exception(msg)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"An unexpected error occurred while "
            f"running data collection workflows. {e}")
        