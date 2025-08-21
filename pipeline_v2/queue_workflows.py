"""A Google Cloud Run app triggered by the Google
Cloud Scheduler. Kicks off the data retrieval
process for one or more sources (e.g., development
banks and government forms). Written to be idempotent.

References:
- https://cloud.google.com/scheduler/docs/creating
- https://cloud.google.com/scheduler/docs/reference/rpc/google.cloud.scheduler.v1#google.cloud.scheduler.v1.HttpTarget
"""

import os
import flask
from flask import Flask, request
from json.decoder import JSONDecodeError
from pipeline.constants import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    DEV_BANK_PROJECTS_JOB_TYPE,
    DEV,
    ENV,
    FORM_13F_ABBREVIATION,
    FORM_13F_JOB_TYPE,
    NOT_STARTED_STATUS
)
from pipeline.services.database import DbClient
from pipeline.services.logger import LoggerFactory
from pipeline.services.pubsub import PublisherClient
from pipeline.scrapers.registry import StarterWorkflowRegistry
from werkzeug.exceptions import (
    BadRequest,
    HTTPException,
    InternalServerError
)


#region ----------------Setup---------------------

# Global scope. Runs at instance cold-start
# and may be used in subsequent invocations.

# Configure logger
logger = LoggerFactory.get("queue-workflows")

# Determine current development environment
env = os.getenv(ENV, DEV)

# Load corresponding environment variables
logger.info(f"Loading environment variables for \"{env}\" environment.")
try:
    project_id = os.environ["GOOGLE_PROJECT_ID"]
    msg_topic_id = os.environ["DATA_RETRIEVAL_TOPIC_ID"]
    msg_publish_timeout = int(os.environ["PUBSUB_PUBLISH_TIMEOUT_IN_SECONDS"])
except KeyError as e:
    raise RuntimeError(f"Missing environment variable \"{e}\".") from None
except ValueError as e:
    raise RuntimeError("Unable to cast environment variable "
                       f"to correct data type. {e}") from None

# Set up Pub/Sub client
pubsub_client = PublisherClient(
    project_id,
    msg_topic_id,
    logger,
    msg_publish_timeout
)

# Set up database client
db_client = DbClient(logger)

# Set up Flask app
app = Flask(__name__)

#endregion ----------------Setup---------------------

@app.errorhandler(HTTPException)
def handle_error(e: HTTPException):
    """Error-handling process for Flask HTTP exceptions.
    """
    logger.error(e.description)
    if HTTPException is BadRequest:
        return e.description, 400
    else:
        return e.description, 500


@app.route("/", methods=["POST"])
def main() -> flask.Response:
    """Receives and processes an HTTP request from Google
    Cloud Scheduler to initiate data collection processes
    for one or more sources.

    Args:
        None

    Returns:
        (`flask.Response`)
    """
    # Log start of processing
    logger.info("Received request to queue workflow(s).")

    # Parse request headers for scheduled job name and execution time
    try:
        logger.info(request.headers)
        sch_job_name = request.headers["X-CloudScheduler-JobName"]
        sch_job_trace = request.headers["X-Cloud-Trace-Context"]
        logger.info(f"Request from job \"{sch_job_name}\" "
                    f"with trace \"{sch_job_trace}\".")
    except KeyError as e:
        raise BadRequest("Failed to queue workflows. "
            f"Missing expected HTTP request header {e}.")

    # Parse request body for list of data sources to scrape
    try:
        selected_sources = request.get_json()["sources"]
        if not isinstance(selected_sources, list):
            raise TypeError
    except (TypeError, JSONDecodeError, KeyError) as e:
        raise BadRequest("Failed to queue workflows. HTTP request "
            "body did not follow expected JSON schema "
            "{\"sources\": [\"...\"] }.")

    # Confirm at least one data source received
    if not selected_sources:
        raise BadRequest("Failed to queue workflows. One "
            "or more data sources must be specified for processing.")

    # Validate data source names
    all_sources = StarterWorkflowRegistry.list()
    for s in selected_sources:
        if not StarterWorkflowRegistry.exists(s):
            valid_sources = ", ".join(e for e in all_sources)
            raise BadRequest("Failed to queue workflows. Received "
                f"invalid source name \"{s}\" in HTTP request. Only "
                f"the following names are permitted: {valid_sources}.")

    # Ensure that there are no duplicates among the bank names
    selected_sources = list(set(selected_sources))
    logger.info(f"Processing data sources: {', '.join(selected_sources)}.")

    # Ensure that Form 13F and project requests remain separate jobs
    if FORM_13F_ABBREVIATION in selected_sources and len(selected_sources) > 1:
        raise BadRequest("Failed to queue workflows. Form 13F requests "
                         "must be queued separately from development "
                         "project requests.")

    # Determine job type
    if FORM_13F_ABBREVIATION in selected_sources:
        job_type = FORM_13F_JOB_TYPE
    else:
        job_type = DEV_BANK_PROJECTS_JOB_TYPE

    # Create new pipeline job with unique CloudScheduler invocation id
    try:
        sch_invoc_id = f"{sch_job_name}-{sch_job_trace}"
        job_id, created = db_client.create_job(sch_invoc_id, job_type)
        if not created:
            logger.info("Pipeline job with invocation id "
                        f"\"{sch_invoc_id}\" already exists in "
                        "database. Using existing job.")
    except Exception as e:
        raise InternalServerError(f"Failed to queue workflows. "
            f"Creation of pipeline job resulted in error. {e}")

    # Generate initial processing tasks for each data source
    first_tasks = []
    for source in selected_sources:
        first_tasks.append({
            "job_id": job_id,
            "status": NOT_STARTED_STATUS,
            "source": source,
            "url": "NULL",
            "workflow_type": StarterWorkflowRegistry.get(source)
        })

    # Persist data retrieval tasks to database. If the tasks already
    # exist in the database, an empty list is returned.
    try:
        inserted_tasks = db_client.bulk_create_tasks(first_tasks)
        logger.info(f"There were {len(inserted_tasks)} newly-created tasks.")
    except Exception as e:
        raise InternalServerError(f"Failed to queue workflows. {e}")

    # Publish task messages to Pub/Sub for scraper nodes to pick up
    try:
        for task in inserted_tasks:
            logger.info(f"Queueing task: \"{task}\".")
            pubsub_client.publish_message(task)
    except Exception as e:
        raise InternalServerError(f"Failed to queue workflows. Not all "
                                  f"{len(inserted_tasks)} messages to Pub/Sub "
                                  f"were successfully published. {e}")

    completion_msg = "Workflows queued successfully."
    logger.info(completion_msg)
    return completion_msg, 200



if __name__ == "__main__":
    debug = env == DEV
    host = os.environ.get("HOST", DEFAULT_HOST)
    port = int(os.environ.get("PORT", DEFAULT_PORT))
    app.run(debug=debug, host=host, port=port)   
