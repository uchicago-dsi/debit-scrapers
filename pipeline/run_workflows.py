"""A script to pull web scraping tasks from a 
database and publish-subscribe ("Pub/Sub") service,
process the tasks in parallel, and then persist
the results to a database.
"""

import json
import os
import yaml
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from google.pubsub_v1.types.pubsub import ReceivedMessage
from pipeline.constants import (
    COMPLETED_STATUS,
    DEV,
    ENV,
    USER_AGENT_HEADERS_FPATH
)
from pipeline.services.web import DataRequestClient
from pipeline.services.database import DbClient
from pipeline.services.logger import LoggerFactory
from pipeline.services.pubsub import PublisherClient, SubscriberClient
from pipeline.scrapers.registry import WorkflowClassRegistry
from typing import List


#region ----------------Setup---------------------

# Create logger
logger = LoggerFactory.get("run-workflows")

# Determine current development environment
env = os.getenv(ENV, DEV)

# Load corresponding environment variables
logger.info(f"Loading environment variables for \"{env}\" environment.")
try:
    project_id = os.environ["GOOGLE_PROJECT_ID"]
    max_num_workers = int(os.environ["DATA_RETRIEVAL_MAX_NUM_WORKERS"])
    data_retrieval_topic_id = os.environ["DATA_RETRIEVAL_TOPIC_ID"]
    data_retrieval_sub_id = os.environ["DATA_RETRIEVAL_SUBSCRIPTION_ID"]
    data_cleaning_topic_id = os.environ["DATA_CLEANING_TOPIC_ID"]
    msg_batch_size = int(os.environ["PUBSUB_MESSAGE_BATCH_SIZE"])
    msg_publish_timeout = int(os.environ["PUBSUB_PUBLISH_TIMEOUT_IN_SECONDS"])
    msg_retry_deadline = int(os.environ["PUBSUB_RETRY_DEADLINE_IN_SECONDS"])
except KeyError as e:
    raise RuntimeError(f"Missing expected Pub/Sub configuration value. {e}")
except ValueError as e:
    raise RuntimeError("Unable to cast environment variable "
                       f"to correct data type. {e}") from None

# Set up database client
logger.info("Creating database client.")
db_client = DbClient(logger)

# Set up DataRequestClient to rotate HTTP headers and add random delays
logger.info("Creating data request client.")
with open(USER_AGENT_HEADERS_FPATH, "r") as stream:
    try:
        user_agent_headers = json.load(stream)
        data_request_client = DataRequestClient(user_agent_headers)
    except yaml.YAMLError as e:
        raise Exception(f"Failed to open configuration file. {e}")
        
# Set up client for publishing messages for data retrieval tasks
logger.info("Setting up client for data retrieval Pub/Sub topic.")
workflows_publisher = PublisherClient(
    project_id,
    data_retrieval_topic_id,
    logger,
    msg_publish_timeout
)

# Connect to topic subscriber
logger.info("Connecting to data retrieval topic subscriber.")
subscriber = SubscriberClient(
    project_id,
    data_retrieval_sub_id,
    msg_batch_size,
    msg_retry_deadline,
    logger
)

# Set up client for publishing messages for data cleaning tasks
logger.info("Setting up client for data cleaning Pub/Sub topic.")
data_cleaning_publisher = PublisherClient(
    project_id,
    data_cleaning_topic_id,
    logger,
    msg_publish_timeout
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

    def complete_message(msg: ReceivedMessage) -> None:
        """Local function to process and acknowledge single message.
        """
        try:
            # Retrieve message metadata
            message_id = msg.message.message_id
            num_delivery_attempts = msg.delivery_attempt

            # Decode and extract message data
            try:
                data = json.loads(msg.message.data.decode("utf"))
                task_id = data["id"]
                job_id = data["job_id"]
                source = data["source"]
                workflow_type = data["workflow_type"]
                url = data["url"]
            except KeyError as e:
                raise RuntimeError("Failed to extract data from Pub/Sub "
                                   "message. Missing expected attribute "
                                   f"\"{e}\".") from None

            # Instantiate appropriate workflow class from registry
            w = WorkflowClassRegistry.get(
                source,
                workflow_type,
                data_request_client,
                workflows_publisher,
                db_client
            )

            # Excecute workflow
            w.execute(
                message_id,
                num_delivery_attempts,
                job_id,
                task_id,
                source,
                url
            )
            
            # Add job to list of those encountered
            encountered_jobs.add(job_id)

            # Acknowledge request to avoid duplicate receipt
            subscriber.acknowledge_message(msg)

        except Exception as e:
            logger.error(f"Failed to process message: {e}")

    while True:     
        # Pull batch of messages from subscription
        logger.info("Pulling batch of new messages.")
        messages = subscriber.pull()

        # If messages exist, process using multithreading
        if messages:
            logger.info(f"Processing {len(messages)} message(s).")
            with ThreadPoolExecutor(max_num_workers) as executor:
                try:
                    executor.map(complete_message, messages)
                except Exception as e:
                    logger.error(e)
            messages_in_previous_batch = True

        # Trigger cleaning of records if no more messages are present
        elif messages_in_previous_batch:
            logger.info("No more messages encountered. "
                        "Requesting data cleaning "
                        "for staged records.")
            audit(
                list(encountered_jobs),
                db_client,
                data_cleaning_publisher
            )
            encountered_jobs = set()
            messages_in_previous_batch = False
            continue

        # Otherwise, log absence of messages to process
        else:
            logger.info("No new messages to process.")


def audit(
    encountered_jobs: List[int],
    db_client: DbClient,
    data_cleaning_pubsub_client: PublisherClient) -> None:
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
    format = "%Y_%m_%d_%H_%M_%S"
    stage_completed_utc = datetime.utcnow()
    stage_completed_utc_str = datetime.strftime(stage_completed_utc, format)

    # If no more messages are received within the timeout window,
    # publish a Pub/Sub message to trigger data cleaning
    # of staged records.
    try:
        for job_id in encountered_jobs:
            logger.info("Publishing Pub/Sub message for completion "
                f"of data collection stage for job \"{job_id}\".")
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
        logger.error("An unexpected error occurred while "
                     f"running data collection workflows. {e}")
        